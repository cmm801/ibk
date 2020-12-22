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
import numpy as np
import pandas as pd
import ibapi
import base


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

    def get_account_details(self):
        """ Get a DataFrame with the account details.
        """
        self._account_details = []
        req_id = self._get_next_req_id()
        self.reqAccountSummary(req_id, "All", "$LEDGER")
        t0 = time.time()
        while not self._account_details and time.time() - t0 < MAX_WAIT_TIME:
            time.sleep(0.2)
        
        # Create a DataFrame from the results
        df = pd.DataFrame(self._account_details)
        
        # Check that there is only a single account number that is returned
        n_accounts = len(set(df['account']))
        if n_accounts > 1:
            raise ValueError(f'Only a single account was expected, but found {n_accounts}')
        else:
            return df.set_index('tag')

    def get_total_account_value(self):
        acct_info = self.get_account_details()
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
        """Cancel the position subscription after a return.
        """
        super().positionEnd()
        self._position_request_completed = True
        self.cancelPositions()

    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                    currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        info = dict(reqId=reqId, account=account, tag=tag, value=value,
                             currency=currency)
        self._account_details.append(info)

    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        self.cancelAccountSummary(reqId)

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
