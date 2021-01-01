"""
Module to facilitate getting data through Interactive Brokers's API

Starting point was Brent Maranzano's GitHub repo:
see: https://interactivebrokers.github.io/tws-api/index.html

Classes
    IBClient (EClient): Creates a socket to TWS or IBGateway, and handles
        sending commands to IB through the socket.
    IBWrapper (EWrapper): Hanldes the incoming data from IB. Many of these
        methods are callbacks from the request commands.
    IBApp (IBWrapper, IBClilent): This provides the main functionality. Many
        of the methods are over-rides of the IBWrapper commands to customize
        the functionality.
"""

import time
import datetime
import pytz
import numpy as np
import pandas as pd
import ibapi
import base

from ibapi.contract import Contract

import constants

MAX_WAIT_TIME = 5   # Max wait time in seconds. Large requests are slow


class AccountApp(base.BaseApp):
    """Main program class. The TWS calls nextValidId after connection, so
    the method is over-ridden to provide an entry point into the program.

    class variables:
    saved_contracts (dict): keys are symbols, values are dictionaries of
        information to uniquely define a contract used for trading.
    """
    def __init__(self):
        super().__init__()
        
        # Initialize some variables used for storing account information
        self._account_value = None
        self._portfolio_value = None
        self._account_update_complete = False
        self.account_info_last_updated = None

    def get_positions(self):
        """Get the account positions. If the class variable, positions, exists,
        return that value, else call the EClient method reqPositions, wait for
        a short time and then return the class variable positions.

        Arguments:
        include_mv (bool): If True, then the current market value of each position
                                    will also be returned.

        Returns (dict): Dictionary of the positions information.
        """
        # Fetch the positions and contracts from the account
        _positions, contracts = self._request_positions()

        # Create a DataFrame object from the dict of positions
        #   Use the localSymbol as the index
        positions_df = pd.DataFrame.from_dict(_positions)
        if positions_df.shape[0]:
            positions_df.set_index('localSymbol', inplace=True, drop=False)
            positions_df.index.name = 'localSymbol'
            
        # Create an empty DataFrame with the correct columns if there are no positions
        if positions_df.size == 0:
            positions_df = pd.DataFrame([],
                                        index=pd.Index([], name='localSymbol'),
                                        columns=['account', 'localSymbol', 'symbol', 'secType',
                                                 'size', 'cost', 'totCost', 'multiplier'])
        return positions_df, contracts

    def get_account_details(self, group='All', tags="$LEDGER"):
        """ Get a DataFrame with the account details.
        """
        self._account_details = []
        req_id = self._get_next_req_id()
        self.reqAccountSummary(req_id, groupName=group, tags=tags)
        t0 = time.time()
        while not self._account_details and time.time() - t0 < MAX_WAIT_TIME:
            time.sleep(0.2)
        
        # Create a DataFrame from the results
        df = pd.DataFrame(self._account_details)
        #return df.set_index('tag')
        return df

    def get_total_account_value(self):
        acct_info = self.get_account_details(group='All', tags='$LEDGER').set_index('tag')
        key = 'NetLiquidationByCurrency'
        if key not in acct_info.index:
            return np.nan
        else:
            return float(acct_info.loc[key, 'value'])

    def get_position_size(self, localSymbol):
        """ Get the position size for a given local symbol.
        
            Returns 0.0 if there is no position in the symbol.
            Arguments:
                localSymbol: (str) the unique local string symbol
                    for the given instrument.
        """
        positions_df, _ = self.get_positions()
        if localSymbol in positions_df.index:
            return positions_df.loc[localSymbol, 'size']
        else:
            return 0.0        

    def get_account_info(self, acct_num=None, max_wait_time=None):
        """ Get the account information.
        
            This method returns a DataFrame with the information returned from
            the callback 'updateAccountValue'
        """
        if acct_num is None:
            acct_num = self.account_number
            
        if self._account_value is None:
            # Account updates has not yet been called, so we call it now
            self.get_account_updates(max_wait_time=max_wait_time)
                
            # Return the account info
            return self.get_account_info(acct_num=acct_num, max_wait_time=max_wait_time)
        else:
            return self._account_value.loc[acct_num].copy()

    def get_portfolio_info(self, acct_num=None, max_wait_time=None):
        """ Get the portfolio information.
        
            This method returns a DataFrame with the information returned from
            the callback 'updatePortfolio'
        """
        if acct_num is None:
            acct_num = self.account_number

        if self._portfolio_value is None:
            # Account updates has not yet been called, so we call it now
            self.get_account_updates(max_wait_time=max_wait_time)

            # Return the portfolio info
            return self.get_portfolio_info(acct_num=acct_num)
        else:
            ptf_info = self._portfolio_value.loc[acct_num].copy()
            
            # Adjust the portfolio info to reflect accurate account information
            ptf_info = self._correct_cash_balances(ptf_info)
            
            # Return the portfolio information
            return ptf_info
            
    def _correct_cash_balances(self, ptf_info, acct_num=None):
        """ Correct the cash balances to remove the virtual FX positions introduced by IB.
        """
        # Add an empty row for USD cash
        usd_row = pd.DataFrame.from_dict([dict(localSymbol='USD.USD', currency='USD',
                       position=np.nan, marketPrice=1, marketValue=np.nan,
                       averageCost=1, unrealizedPNL=0, realizedPNL=0)])
        usd_row.set_index('localSymbol', inplace=True)
        ptf_info = pd.concat([ptf_info, usd_row], axis=0)

        # Get the cash balance information
        acct_info = self.get_account_info(acct_num=acct_num)
        cash_balances = acct_info.loc['TotalCashBalance']['value'].copy()

        # Add cash balances for currencies that are in the portfolio
        ccy_symbols = [s[:3] for s in ptf_info.index.values if len(s) == 7 and s[3] == '.']
        for ccy in ccy_symbols:
            if ccy not in cash_balances:
                cash_balances[ccy] = 0.0

        # Update the position, market value and PNL info using cash balances
        for ccy in cash_balances.index:
            if ccy != 'BASE':
                symbol = f'{ccy}.USD'

                # Keep track of the original position size
                orig_pos = ptf_info.loc[symbol, 'position']

                # Update the position size and market value using the cash balances
                ptf_info.loc[symbol, 'position'] = float(cash_balances[ccy])
                ptf_info.loc[symbol, 'marketValue'] = ptf_info.loc[symbol, 'position'] * \
                                                      ptf_info.loc[symbol, 'marketPrice']

                # We don't know the exact PNL, but we approximate it by keeping an amount
                # equal to the ratio of the new : old position size
                pos_ratio = abs(ptf_info.loc[symbol, 'position'] / (orig_pos + 1e-8))
                if not np.isclose(0, ptf_info.loc[symbol, 'unrealizedPNL'], atol=1e-2):
                    ptf_info.loc[symbol, 'unrealizedPNL'] *= pos_ratio

                if not np.isclose(0, ptf_info.loc[symbol, 'realizedPNL'], atol=1e-2):
                    ptf_info.loc[symbol, 'realizedPNL'] *= pos_ratio

        # Return the portfolio information
        return ptf_info

    def get_account_updates(self, acct_num=None, max_wait_time=None):
        """ Subscribe for account and portfolio updates.
        """
        if acct_num is None:
            acct_num = self.account_number

        if max_wait_time is None:
            max_wait_time = 5
            
        # Make the request to the client
        self.reqAccountUpdates(subscribe=True, acctCode=acct_num)
        
        # Sleep until the request has been completed
        t0 = time.time()
        while not self._account_update_complete and time.time() - t0 < max_wait_time:
            time.sleep(0.05)

    def cancel_account_updates(self, acct_num=None):
        if acct_num is None:
            acct_num = self.account_number

        self.reqAccountUpdates(subscribe=False, acctCode=acct_num)

    def _request_positions(self):
        """Get contracts and a dictionary of details for all account positions.
        Call the EClient method reqPositions, wait for
        a short time and then return the class variable positions.

        Returns (tuple): (positions, contracts)
           positions (dict): contains details on the positions in the account
           contracts (list): Contract objects for each position in the account
        """
        self._positions = []
        self._position_contracts = []
        self._position_request_completed = False
        self.reqPositions()

        while not self._position_request_completed:
            time.sleep(0.05)
        return self._positions, self._position_contracts
    
    ################################################################################
    # Methods to handle callbacks from the client
    ################################################################################
    
    def position(self, account: str, _contract: ibapi.contract.Contract, 
                 position: float, avgCost: float):
        """ Callback method from EClient to obtain position info.
        """
        super().position(account, _contract, position, avgCost)
        self._positions.append({
            'account': account,
            'localSymbol': _contract.localSymbol,
            'symbol': _contract.symbol,
            'secType': _contract.secType,
            'size': position,
            'cost': avgCost,
            'totCost': avgCost * position,
            'multiplier': int(_contract.multiplier) if _contract.multiplier else 1,
        })
        # Save the contract info, withouth worrying about what exchange actually
        #   handled our order
        _contract.exchange = ''
        self._position_contracts.append(_contract)

    def positionEnd(self):
        """ Callback method from EClient indicate position query has finished.
        """
        super().positionEnd()
        self._position_request_completed = True
        self.cancelPositions()

    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                    currency: str):
        """ Callback method from EClient that returns account information.
        """
        super().accountSummary(reqId, account, tag, value, currency)
        info = dict(reqId=reqId, account=account, tag=tag, value=value,
                             currency=currency)
        self._account_details.append(info)

    def accountSummaryEnd(self, reqId: int):
        """ Callback method from EClient indicating that all account info has been returned.
        """        
        super().accountSummaryEnd(reqId)
        self.cancelAccountSummary(reqId)

    def updateAccountValue(self, key: str, value: str, currency: str, accountName: str):
        if self._account_value is None:
            new_row = dict(key=key, account_number=accountName, currency=currency, value=value)
            self._account_value = pd.DataFrame.from_dict([new_row])
            self._account_value = self._account_value.set_index(['account_number', 'key', 'currency'])
        else:
            self._account_value.loc[(accountName, key, currency)] = value

    def updatePortfolio(self, contract: Contract, position: float, marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL, realizedPNL: float, accountName: str):

        if self._portfolio_value is None:
            new_row = dict(account_number=accountName, localSymbol=contract.localSymbol, 
                           currency=contract.currency, 
                           position=position, marketPrice=marketPrice, marketValue=marketValue,
                           averageCost=averageCost, unrealizedPNL=unrealizedPNL, realizedPNL=realizedPNL)
            self._portfolio_value = pd.DataFrame.from_dict([new_row])
            self._portfolio_value = self._portfolio_value.set_index(['account_number', 'localSymbol'])
        else:
            values = pd.Series(dict(position=position, marketPrice=marketPrice, marketValue=marketValue,
                                    currency=contract.currency, averageCost=averageCost, 
                                    unrealizedPNL=unrealizedPNL, realizedPNL=realizedPNL))
            self._portfolio_value.loc[(accountName, contract.localSymbol), :] = values

    def updateAccountTime(self, timestamp: str):
        """Store the time of the last account update.
        """
        # Convert the input to a datetime
        update_time = datetime.datetime.strptime(timestamp, '%H:%M')
        
        # Get the current date in the TWS time zone.
        tws_tzinfo = pytz.timezone(constants.TIMEZONE_TWS)
        curr_datetime_tws = tws_tzinfo.localize(datetime.datetime.now())

        # Convert the date and time info. Save the last update datetime
        self.account_info_last_updated = datetime.datetime.combine(curr_datetime_tws.date(), 
                                  update_time.time(), tzinfo=tws_tzinfo)

    def accountDownloadEnd(self, account: str):
        self._account_update_complete = True
        print(f'Account download complete: {datetime.datetime.now()}')
