"""
Module to facilitate trading through Interactive Brokers's API
see: https://interactivebrokers.github.io/tws-api/index.html

Brent Maranzano
Dec. 14, 2018

Classes
    IBClient (EClient): Creates a socket to TWS or IBGateway, and handles
        sending commands to IB through the socket.
    IBWrapper (EWrapper): Hanldes the incoming data from IB. Many of these
        methods are callbacks from the request commands.
    IBApp (IBWrapper, IBClilent): This provides the main functionality. Many
        of the methods are over-rides of the IBWrapper commands to customize
        the functionality.
"""

import os.path
import time
import logging
import threading
import json
import numpy as np
import pandas as pd
import datetime

from ibapi import wrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from ibapi.common import OrderId, ListOfContractDescription, BarData,\
        HistogramDataList, TickerId, TickAttrib
from ibapi.order import Order
from ibapi.order_state import OrderState

API_THREAD = None

# Maximum time to wait for IB to return data before giving up
MAX_WAIT_TIME = 60  # time in seconds. Large requests are slow

# Acceptable inputs for getting IB historical data
VALID_BAR_SIZES = ['1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
    '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins', '20 mins',
    '30 mins', '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
    '1 day', '1 week', '1 month' ]

def setup_logger():
    """Setup the logger.
    """
    if not os.path.exists("log"):
        os.makedirs("log")

    time.strftime("pyibapi.%Y%m%d_%H%M%S.log")

    recfmt = "(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s" \
             "%(filename)s:%(lineno)d %(message)s"

    timefmt = '%y%m%d_%H:%M:%S'

    logging.basicConfig(
        filename=time.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
        filemode="w",
        level=logging.INFO,
        format=recfmt, datefmt=timefmt
    )
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logger.addHandler(console)
    logging.debug("now is %s", datetime.datetime.now())


class IBClient(EClient):
    """Subclass EClient, which delivers message to the TWS API socket.
    """
    def __init__(self, app_wrapper):
        EClient.__init__(self, app_wrapper)


class IBWrapper(wrapper.EWrapper):
    """Subclass EWrapper, which translates messages from the TWS API socket
    to the program.
    """
    def __init__(self):
        wrapper.EWrapper.__init__(self)


class HistoricalRequestError(Exception):
    """Exceptions generated during requesting historical stock price data.
    """
    def __init__(self, message, errors):
        super().__init__(message)

        self.errors = errors
        self.message = message


class IBApp(IBWrapper, IBClient):
    """Main program class. The TWS calls nextValidId after connection, so
    the method is over-ridden to provide an entry point into the program.

    class variables:
    saved_contracts (dict): keys are symbols, values are dictionaries of
        information to uniquely define a contract used for stock trading.
        {symbol: {'contract_info_dictionary'}}
    saved_orders (dict): keys are order ids, values are Order, Contract
        {id: {order: Order, contract: Contract}}
    TODO
    positions
    """
    def __init__(self):
        IBWrapper.__init__(self)
        IBClient.__init__(self, app_wrapper=self)

        self.order_id = None
        self.req_id = None        
        self.saved_partial_contracts = dict()
        self.saved_contracts = dict()        
        self.positions = []
        self._contract_details = {}
        self._saved_orders = {}
        self._open_orders = []
        
        self._historical_data = dict()
        self._historical_data_req_end = dict()
        self._histogram = None
        
        # Keep track of request IDs and human-readable names/ids
        self._req_ids_to_string_ids = dict()
        self._string_ids_to_req_ids = dict()
        
        self._load_contracts('contract_file.json')
        
        # Subscribe to market data streaming snapshots
        self._market_data = dict()

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        """Overide EWrapper error method.
        """
        super().error(reqId, errorCode, errorString)
        print('ERROR: request {}'.format(reqId))

    def _load_contracts(self, filename):
        """Load saved contracts.
        """
        try:
            with open(filename, mode='r') as file_obj:
                # Read in all saved contract info
                contract_info = json.load(file_obj)
                
                # Loop through the top-level keys, which are instrument tickers
                for tkr, info in contract_info.items():
                    # For each ticker, create the Contract object and save it
                    self.saved_contracts[tkr] = self._get_contract_from_dict(info)
                    ct = Contract()                    
                    for key, val in info.items():
                        if key != 'conId':
                            ct.__setattr__(key, val)
                    
        except FileNotFoundError:
            pass

    def _get_contract_from_dict(self, info):
        """Create a Contract object from a dictionary of keys/values."""
        _contract = Contract()                    
        for key, val in info.items():
            _contract.__setattr__(key, val)
        return _contract
                        
    def _copy_contract(self, target_contract):
        """Create a copy of a Contract object"""
        ct_dict = target_contract.__dict__
        return self._get_contract_from_dict(ct_dict)
        
    def _save_contracts(self, file='contract_file.json', mode='w'):
        """Save contracts.
        """
        with open(file, mode=mode) as file_obj:
            contents = { k: v.__dict__ for k, v in self.saved_contracts.items()}
            json.dump(contents, file_obj)

    def nextValidId(self, orderId: int):
        """Method of EWrapper.
        Sets the order_id and req_id class variables.
        This method is called from after connection completion, so
        provides an entry point into the class.
        """
        super().nextValidId(orderId)
        self.order_id = orderId
        self.req_id = int(orderId + 1e8)
        return self

    def _get_next_order_id(self):
        """Retrieve the current class variable order_id and increment
        it by one.

        Returns (int) current order_id
        """
        #  reqIds can be used to update the order_id, if tracking is lost.
        # self.reqIds(-1)
        current_order_id = self.order_id
        self.order_id += 1
        return current_order_id
    
    def _get_next_req_id(self):
        """Retrieve the current class variable req_id and increment
        it by one.

        Returns (int) current req_id
        """
        current_req_id = self.req_id
        self.req_id += 1
        return current_req_id
    
    def _get_all_matching_contracts(self, partial_contract):
        """Find all matching contracts given a partial contract.
        Upon execution of IB backend, the EWrapper.symbolSamples is called,
        which is over-ridden to save the contracts to a class dictionary.
        This function then monitors the class dictionary until
        the contract is found and then returns the contract.

        Arguments:
            partial_contract (Contract): a Contract object with some of
                                                the fields specified

        Returns: (list) Matching contract(s).
        """
        self._contract_details = []

        # The IB server will call contractDetails upon completion.
        req_id = self._get_next_req_id()
        self.reqContractDetails(req_id, partial_contract)

        # Loop until the server has completed the request.
        t0 = time.time()
        while not self._contract_details and time.time() - t0 < MAX_WAIT_TIME:
            time.sleep(0.2)    
        return self._contract_details
    
    def get_contracts(self, listOfLocalSymbols):
        """Try to find saved contracts with the specified localSymbols.
        Arguments:
            localSymbol (list): a list of local stringsrepresenting the 
                (unique) local symbol associated with an instrument/contract. 

        Returns: (list) Matching contract(s)
        """
        contracts = []
        for symbol in listOfLocalSymbols:
            contracts.append(self.get_contract(symbol))
        return contracts
    
    def get_contract(self, localSymbol):
        """Try to find a saved contract with the specified localSymbol.
        Arguments:
            localSymbol (str): a string representing the (unique) local
                            symbol associated with an instrument/contract. 

        Returns: (Contract) Matching contract, or None if no match.
        """        
        if localSymbol in self.saved_contracts:
            return self.saved_contracts[localSymbol]
        else:
            return None
    
    def match_contract(self, partial_contract, max_wait_time=5):
        """Find the matching contract given a partial contract.
        Upon execution of IB backend, the EWrapper.symbolSamples is called,
        which is over-ridden to save the contracts to a class dictionary.
        This function then monitors the class dictionary until
        the contract is found and then returns the contract.

        Arguments:
            partial_contract (Contract): a Contract object with some of
                                                the fields specified
            max_wait_time (int): the maximum time (in seconds) to wait 
                        for a response from the IB API

        Returns: (Contract) Matching contract, or None if no match.
        """
        # If the contract has not already been saved, look it up.
        key = str(partial_contract)
        if key not in self.saved_partial_contracts:
            self._get_all_matching_contracts(partial_contract, 
                                            max_wait_time=max_wait_time)            

            # If there are multiple matches, select the desired contract
            ct = self._select_contract(partial_contract)
            if ct is None:
                s = partial_contract.symbol
                raise ValueError('Partial contract has no matches for symbol: {}'.format(s))
            else:
                # Cache the results
                self.saved_partial_contracts[key] = ct
                self.saved_contracts[ct.localSymbol] = ct
        
        # Return the cached contract
        return self.saved_partial_contracts[key]
    
    def _select_contract(self, contract):
        if 'STK' == contract.secType:
            return self._select_equity_contract(contract)
        elif 'FUT' == contract.secType:
            return self._select_futures_contract(contract)
        elif 'OPT' == contract.secType:
            return self._select_options_contract(contract)
        elif 'IND' == contract.secType:
            return self._select_index_contract(contract)
        elif 'CASH' == contract.secType:
            return self._select_forex_contract(contract)
        elif 'BOND' == contract.secType:
            return self._select_bond_contract(contract)
        elif 'CMDTY' == contract.secType:
            return self._select_commodity_contract(contract)
        elif 'FUND' == contract.secType:
            return self._select_mutual_fund_contract(contract)
        elif 'FOP' == contract.secType:
            return self._select_futures_option_contract(contract)
        else:
            raise ValueError('Invalid secType: {}'.format(contract.secType))       
    
    def _select_equity_contract(self, target_contract):
        # Select the proper contract
        for contract in self._contract_details:
            if target_contract.currency == 'USD':
                # NYSE stock
                if contract.primaryExchange in ["NYSE", 'ARCA', 'NASDAQ', 'BATS']:
                    return contract
                # Nasdaq stock
                elif contract.primaryExchange == "NASDAQ.NMS":
                    raise ValueError('This branch was created for unknown reasons on the github repo')
                    # Below is legacy code from the original github
                    #contract.primaryExchange = "ISLAND"
                    #return contract
            else:
                raise NotImplemtedError( 'Currently only supported for USD stocks.' )

    def _filter_contracts(self, contract_list, target_contract, filter_type='third_friday'):
        """Filter a list of contracts by a particular condition."""
        if 'third_friday' == filter_type:
            expiry_string = target_contract.lastTradeDateOrContractMonth
            if len(expiry_string) == 6:
                # Get the expiration year/month from the expiry string
                expiry_ym = datetime.datetime.strptime('202004', "%Y%m")
                third_friday = self.get_third_friday(expiry_ym.year, expiry_ym.month)
                expiry_date = datetime.datetime.strftime( third_friday, '%Y%m%d')
            elif len(expiry_string) == 8:
                expiry_date = expiry_string
            else:
                raise ValueError('Unsupported length of lastTradeDateOrContractMonth.')
            return [x for x in contract_list if x.lastTradeDateOrContractMonth == expiry_date]
        else:
            raise ValueError('Unsupported filter type: {}'.format(filter_type))
        
    def _select_futures_contract(self, target_contract):
        """Select the desired futures contract in case there are multiple matches."""
        matching_contracts = self._filter_contracts(self._contract_details, 
                                target_contract, filter_type='third_friday')
        if not matching_contracts:
            return None
        elif len(matching_contracts) == 1:
            return matching_contracts[0]
        else:
            raise ValueError('Multiple matching contracts - the search must be more specific.')
        
    def _select_options_contract(self, target_contract):
        """Select the desired options contract in case there are multiple matches."""
        matching_contracts = self._filter_contracts(self._contract_details, 
                                target_contract, filter_type='third_friday')
        if not matching_contracts:
            return None
        elif len(matching_contracts) == 1:
            return matching_contracts[0]
        else:
            smart_contracts = [x for x in matching_contracts 
                                       if x.exchange == 'SMART']
            if not smart_contracts:
                return None
            elif len(smart_contracts) == 1:
                return smart_contracts[0]
            else:
                raise ValueError('Multiple matching contracts - the search must be more specific.')
        
    def _select_forex_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise ValueError('Multiple matching contracts - the search must be more specific.')
        
    def _select_index_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')
        
    def _select_bond_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')
        
    def _select_commodity_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')
        
    def _select_mutual_fund_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')

    def _select_futures_option_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')

    def get_third_friday(self, year, month):
        """Returns the third friday, given a year and month"""
        dt = datetime.date(year, month, 1)
        if dt.weekday() <= 4:
            new_day = dt.day + 4 - dt.weekday() + 14
            third_friday = datetime.date(year, month, new_day)
        else:    
            new_day = dt.day + (4 - dt.weekday()) % 7 + 14
            third_friday = datetime.date(year, month, new_day)
        return third_friday    
    
    def contractDetails(self, reqId:int, contract_details):
        """Callback from reqContractDetails.
        """
        super().contractDetails(reqId, contract_details)

        # Add all contracts to the to a list that the calling function can access.
        self._contract_details.append(contract_details.contract)
        
    def symbolSamples(self, reqId: int,
                      contractDescriptions: ListOfContractDescription):
        """Callback from reqMatchingSymbols. Add contracts that are of
        secType=STK, currency=USD, and primaryExchange=(NYSE | ISLAND) to the
        class variable contract_search_results.
        """
        super().symbolSamples(reqId, contractDescriptions)

        # Add all contracts to the to a list that the calling function can
        # access.
        contracts = []
        for desc in contractDescriptions:
            contracts.append(desc.contract)
        # is complete.
        self._contract_details = contracts

    def _get_position_info(self):
        """Get contracts and a dictionary of details for all account positions. 
        Call the EClient method reqPositions, wait for
        a short time and then return the class variable positions.
        
        Returns (tuple): (positions, contracts)
           positions (dict): contains details on the positions in the account
           contracts (list): Contract objects for each position in the account
        """
        self.positions = []
        self._position_contracts = []
        self.reqPositions()
        time.sleep(1)
        
        # Save the full contract in case we need it later
        for _contract in self._position_contracts:
            if _contract.localSymbol not in self.saved_contracts:
                # Match the contract, agnostic of which exchange the position actually traded on
                _contract = self._clean_position_contracts(_contract)
                full_contract = self.match_contract(_contract)            
                self.saved_contracts[full_contract.localSymbol] = full_contract
        return self.positions, self._position_contracts
                
    def _include_mv_in_positions(self, df_pos):
        """Add market value information to a DataFrame of positions.
        
        Arguments:
            df_pos (DataFrame): contains information about the positions, including 
                                the localSymbol (IB's unique identifier)
        Returns:
            A copy of the original DataFrame, with 'price' and 'mktVal' columns included.
        """
        # Get the market data for each localSymbol in df_pos
        local_symbols = df_pos['localSymbol']
        contracts = [self.get_contract(s) for s in local_symbols]
        mkt_data = self.get_snapshot(contracts)
        
        prices = np.nan * np.ones_like(local_symbols)
        for j, symbol in enumerate(local_symbols):
            _contract = self.get_contract(symbol)
            if _contract.secType != 'CASH':
                mkt_data = self.get_snapshot([_contract])
                if 'last_price' in mkt_data:
                    prices[j] = float(mkt_data['last_price'][symbol])
                elif 'close_price' in mkt_data:
                    prices[j] = float(mkt_data['close_price'][symbol])
                  
        # Add a column with market value info at the end of the Data Frame
        pos = df_pos.copy()
        pos.insert(pos.shape[1], 'price', prices)
        pos.insert(pos.shape[1], 'mktVal', prices * pos['multiplier'] * pos['size'])
        return pos
        
    def get_positions(self, include_mv=False):
        """Get the account positions. If the class variable, positions, exists,
        return that value, else call the EClient method reqPositions, wait for
        a short time and then return the class variable positions.
        
        Arguments:
        include_mv (bool): If True, then the current market value of each position
                                    will also be returned.

        Returns (dict): Dictionary of the positions information.
        """
        # Fetch the positions and contracts from the account
        _positions, contracts = self._get_position_info()
        
        # Create a DataFrame object from the dict of positions
        #   Use the localSymbol as the index
        positions_df = pd.DataFrame.from_dict(_positions).set_index('localSymbol', 
                                                                     drop=False)
        positions_df.index.name = 'localSymbol'

        # Include market values if requested
        if include_mv:
            return self._include_mv_in_positions(positions_df)
        else:
            return positions_df

    def _clean_position_contracts(self, target_contract):
        """Make changes to contracts that are returned from get_positions in 
           order to make them findable within IB's contract universe.
           """
        if 'CASH' == target_contract.secType:
            _contract = Contract()
            _contract.symbol = target_contract.symbol
            _contract.currency = target_contract.currency
            _contract.secType = 'CASH'
        else:
            _contract = self._copy_contract(target_contract)
            _contract.exchange = ''
        return _contract
        
    def position(self, account: str, _contract: Contract, position: float,
                 avgCost: float):
        super().position(account, _contract, position, avgCost)
        self.positions.append({
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
        self.cancelPositions()
        
    def get_account_details(self):
        self._account_details = []
        req_id = self._get_next_req_id()
        self.reqAccountSummary(req_id, "All", "$LEDGER")
        t0 = time.time()
        while not self._account_details and time.time() - t0 < MAX_WAIT_TIME:
            time.sleep(0.2)
        return pd.DataFrame(self._account_details)
    
    def get_total_account_value(self):
        acct_info = self.get_account_details()
        tags = acct_info['tag']
        tot_acct_val = float(acct_info[acct_info['tag'] == 'NetLiquidationByCurrency'].value)
        return tot_acct_val

    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                    currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        info = dict(reqId=reqId, account=account, tag=tag, value=value, 
                             currency=currency)
        self._account_details.append(info) 

    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        self.cancelAccountSummary(reqId)
    
    def create_bracket_orders(self, req_orders=None):
        """Create orders, but do not place.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            localSymbol (str): local (unique) IB symbol.
            instruction (str): "BUY" | "SELL"
            price (float): Order set price.
            quantity (float): Order quantity.
            outside_rth (bool): outside regular trading hours
            tif (str): Time in force "DAY" | "GTC"
            profit_price (float): Price for profit taking
            stop_price (float): Price for stop loss
            parent_id (int): Id of parent trade.
        """
        # If only a single contract (dict) is passed convert it
        # to a list with a single item.
        if not isinstance(req_orders, list):
            req_orders = [req_orders]

        for req_order in req_orders:
            contract = self.get_contract(req_order['localSymbol'])

            # Create the parent order
            order_id = self._get_next_order_id()
            parent = Order()
            parent.orderId = order_id
            parent.action = req_order['instruction']
            parent.orderType = "LMT"
            parent.totalQuantity = req_order['quantity']
            parent.lmtPrice = req_order['price']
            parent.outsideRth = req_order['outside_rth']
            parent.tif = req_order['tif']
            parent.transmit = False
            self._saved_orders[order_id] = {
                "order": parent, "contract": contract
            }

            # Create the profit taker order
            if req_order['profit_price'] is not None:
                order_id = self._get_next_order_id()
                profit_taker = Order()
                profit_taker.orderId = order_id
                profit_taker.action = "SELL"\
                    if req_order['instruction'] == "BUY" else "BUY"
                profit_taker.orderType = "LMT"
                profit_taker.totalQuantity = req_order['quantity']
                profit_taker.lmtPrice = req_order['profit_price']
                profit_taker.parentId = parent.orderId
                profit_taker.transmit = False
                self._saved_orders[order_id] = {
                    "order": profit_taker, "contract": contract
                }

            # Create stop loss order
            if req_order['stop_price'] is not None:
                order_id = self._get_next_order_id()
                stop_loss = Order()
                stop_loss.orderId = order_id
                stop_loss.action = "SELL"\
                    if req_order['instruction'] == "BUY" else "BUY"
                stop_loss.orderType = "STP"
                stop_loss.auxPrice = req_order['stop_price']
                stop_loss.totalQuantity = req_order['quantity']
                stop_loss.parentId = parent.orderId
                stop_loss.transmit = False
                self._saved_orders[order_id] = {
                    "order": stop_loss, "contract": contract
                }

    def create_trailing_stop_orders(self, req_orders=None):
        """Create a trailing stop order.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            localSymbol (str): (unique) IB ticker symbol.
            instruction (str): "BUY" | "SELL"
            quantity (float): Order quantity.
            trail_stop_price (float): Trailing stop price
            trail_amount (float): Trailing amount in dollars.
            limit_offset (float): Offset of limit price
                for sell - limit offset is greater than trailing amount
                for buy - limit offset is less than trailing amount
            outside_rth (bool): outside regular trading hours
            tif (str): Time in force "DAY" | "GTC"
            parent_id (int): Id of parent trade.
        """
        # If only a single contract (dict) is passed convert it
        # to a list with a single item.
        if not isinstance(req_orders, list):
            req_orders = [req_orders]

        for req_order in req_orders:
            contract = self.get_contract(req_order['localSymbol'])

            # Create the order
            order_id = self._get_next_order_id()
            order = Order()
            order.orderId = order_id
            order.action = req_order['instruction']
            order.orderType = "TRAIL LIMIT"
            order.totalQuantity = req_order['quantity']
            order.trailStopPrice = req_order['trail_stop_price']
            order.auxPrice = req_order['trail_amount']
            order.lmtPriceOffset = req_order['limit_offset']
            order.outsideRth = req_order['outside_rth']
            order.tif = req_order['tif']
            order.transmit = False
            # TODO parent_id
            self._saved_orders[order_id] = {
                "order": order, "contract": contract
            }

    def create_stop_limit_orders(self, req_orders=None):
        """Create a trailing stop order.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            localSymbol (str): (unique) IB ticker symbol.
            instruction (str): "BUY" | "SELL"
            quantity (float): Order quantity.
            stop_price (float): stop price
            limit_price (float): limit price.
            outside_rth (bool): outside regular trading hours
            tif (str): Time in force "DAY" | "GTC"
            profit_price (float): Profit taking price.
        """
        # If only a single contract (dict) is passed convert it
        # to a list with a single item.
        if not isinstance(req_orders, list):
            req_orders = [req_orders]

        for req_order in req_orders:
            contract = self.get_contract(req_order['localSymbol'])

            # Create the order
            order_id = self._get_next_order_id()
            order = Order()
            order.orderId = order_id
            order.action = req_order['instruction']
            order.orderType = "STP LMT"
            order.totalQuantity = req_order['quantity']
            order.lmtPrice = req_order['limit_price']
            order.auxPrice = req_order['stop_price']
            order.outsideRth = req_order['outside_rth']
            order.tif = req_order['tif']
            order.transmit = False
            self._saved_orders[order_id] = {
                "order": order, "contract": contract
            }

            # Create the profit taker order
            if req_order['profit_price'] is not None:
                profit_taker_order_id = self._get_next_order_id()
                profit_taker = Order()
                profit_taker.orderId = profit_taker_order_id
                profit_taker.action = "SELL"\
                    if req_order['instruction'] == "BUY" else "BUY"
                profit_taker.orderType = "LMT"
                profit_taker.totalQuantity = req_order['quantity']
                profit_taker.lmtPrice = req_order['profit_price']
                profit_taker.parentId = order.orderId
                profit_taker.transmit = False
                self._saved_orders[profit_taker_order_id] = {
                    "order": profit_taker, "contract": contract
                }

    def create_pegged_orders(self, req_orders=None):
        """Create a pegged to bench mark order.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            localSymbol (str): (unique) IB ticker symbol.
            instruction (str): "BUY" | "SELL"
            quantity (float): Order quantity.
            starting_price (float): Order starting price.
            outside_rth (bool): outside regular trading hours
            tif (str): Time in force "DAY" | "GTC"
            peg_change_amount (float): Change of price for the target
            ref_change_amount (float): Change of price of the reference
            ref_contract_id (int): Contract ID of the reference
                SPY: ConID: 756733, exchange: ARCA
                QQQ: ConID: 320227571, exchange: NASDAQ
            ref_exchange (str): Exchange of the reference
            ref_price (float): Start price of the reference
            ref_lower_price (float): Lower ref price allowed
            ref_upper_price (float): Upper ref price allowed
        """
        # If only a single contract (dict) is passed convert it
        # to a list with a single item.
        if not isinstance(req_orders, list):
            req_orders = [req_orders]

        for req_order in req_orders:
            contract = self.get_contract(req_order['localSymbol'])

            # Create the parent order
            order_id = self._get_next_order_id()
            order = Order()
            order.orderId = order_id
            order.orderType = "PEG BENCH"
            order.action = req_order['instruction']
            order.totalQuantity = req_order['quantity']
            order.startingPrice = req_order['starting_price']
            order.isPeggedChangeAmountDecrease = False
            order.peggedChangeAmount = req_order['peg_change_amount']
            order.referenceChangeAmount = req_order['ref_change_amount']
            order.referenceContractId = req_order['ref_contract_id']
            order.referenceExchange = req_order['ref_exchange']
            order.stockRefPrice = req_order['ref_price']
            order.stockRangeLower = req_order['ref_lower_price']
            order.stockRangeUpper = req_order['ref_upper_price']
            order.transmit = False
            self._saved_orders[order_id] = {
                "order": order, "contract": contract
            }

    def get_saved_orders(self, symbol=None):
        """Return saved orders for symbol. If symbol is None
        return all saved orders.

        Returns (dict) {order_id: {order: order, contract: contract}}
        """
        if symbol is None:
            return self._saved_orders

        orders = dict()
        for oid, order in self._saved_orders.items():
            if order['contract'].symbol == symbol:
                orders[oid] = order
        return orders

    def place_order(self, order_id=None):
        """Place a saved order. from a previously created saved order with
        order_id.

        Arguments:
        order_id (int): The order_id of a previously created order.
        """
        if order_id in self._saved_orders:
            self.placeOrder(order_id, self._saved_orders[order_id]['contract'],
                            self._saved_orders[order_id]['order'])
        del self._saved_orders[order_id]

    def place_all_orders(self):
        """Place all the saved orders.
        """
        order_ids = list(self._saved_orders.keys())
        for order_id in order_ids:
            self.place_order(order_id=order_id)

    def get_open_orders(self):
        """Call the IBApi.EClient reqOpenOrders. Open orders are returned via
        the callback openOrder.
        """
        self.reqOpenOrders()

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,
                  orderState: OrderState):
        """Callback from reqOpenOrders(). Method is over-ridden from the
        EWrapper class.
        """
        super().openOrder(orderId, contract, order, orderState)
        self._open_orders.append({
            'order_id': orderId,
            'contract': contract,
            'order': order
        })

    def get_snapshot(self, contractList: list, fields="", max_wait_time=MAX_WAIT_TIME):
        """Get sbapshot of market data for a set of contracts.
        
        Arguments:
        contractList (list): a list of contracts for which to get market data
        fields: IB field codes that will be requested, in addition
                    to the default data fields that IB returns. By default, 
                    no additional data fields are requested.
                    
        Returns:
        DataFrame with columns as the localSymbols and rows as the data field types.
        """
        req_ids = set()
        for contract in contractList:
            self._snapshot_complete = set()
            req_id = self._get_next_req_id()
            req_ids.add(req_id)
            self._market_data[req_id] = dict(localSymbol=contract.localSymbol)
            self.reqMktData(
                            reqId=req_id, 
                            contract=contract, 
                            genericTickList=fields, 
                            snapshot=True, 
                            regulatorySnapshot=False, 
                            mktDataOptions=[]
            )

            # Wait until the snapshot request is complete for all req_ids
            t0 = time.time()
            while self._snapshot_complete != req_ids and time.time() - t0 < max_wait_time:
                time.sleep(0.1)
                
            # Format the data for output
            df = dict()
            for req_id in req_ids:
                mdata = self._market_data[req_id]
                local_symbol = mdata['localSymbol']
                df[local_symbol] = mdata
            return pd.DataFrame(df).T
            
    def tickPrice(self, tickerId: int, field: int, price: float, attribs: TickAttrib):
        field_name = TickTypeEnum.to_str(field)
        self._market_data[tickerId][field_name] = price

    def tickSize(self, tickerId: int, field: int, size: int):
        field_name = TickTypeEnum.to_str(field)
        self._market_data[tickerId][field_name] = size

    def tickString(self, tickerId: int, field: int, value: str):
        field_name = TickTypeEnum.to_str(field)        
        self._market_data[tickerId][field_name] = value

    def tickOptionComputation(self, tickerId: int, field: int, impliedVolatility: float, 
                              delta: float, optPrice: float, pvDividend: float,
                              gamma: float, vega: float, theta: float, undPrice: float):
        raise NotImplementedError('Option market data needs to be implemented.')

    def tickSnapshotEnd(self, reqId: int):
        super().tickSnapshotEnd(reqId)
        self._snapshot_complete.add(reqId)

    def get_quotes(self, contracts):
        """Get a quote for the contract. Callsback to
        Warning: This may incur fees!

        Arguments:
        contracts (Contract|list): Contract object or list of Contract objects

        Returns (Panda Series): Last trade price for the contracts.
        """
        # If only a single contract is passed convert it
        # to a list with a single item.
        if isinstance(contracts, Contract):
            contracts = [contracts]

        # Get the bar data for each Contract
        local_symbols = [x.localSymbol for x in contracts] # Use as unique IDs
        quotes = pd.Series(index=local_symbols)
        for contract in contracts:

            quote = self._req_historical_data(
                contract,
                end="",
                duration="2 D",
                bar_size="1 min",
                info="TRADES",
                rth=False
            )
            quotes[contract.localSymbol] = float(quote.iloc[-1]['close_price'])

        return quotes
        
    def get_price_history(self, contracts=None, start=None, end=None,
                          bar_size="1 day", rth=False, info="TRADES"):
        """Get the price history for contracts.

        Arguments:
        contracts (Contract|list): Contract object or list of Contract objects.
        start (datetime.datetime): First date/time for data retrieval.
        end (datetime.datetime): Last date/time for data retrieval.
        bar_size (str): Bar size (e.g. "1 min", "1 day", "1 month")
            for valid strings see:
               http://interactivebrokers.github.io/tws-api/historical_bars.html
        rth (bool): True to only return data within regular trading hours.
        info (str): Represents the type of info available from TWS. See link for details:
             http://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration

        return (pandas.DataFrame): Price history data.
        """
        if end is None:
            end = datetime.datetime.today()

        # If only a single contract is passed convert it
        # to a list with a single item.
        if isinstance(contracts, Contract):
            contracts = [contracts]

        # Estimate a duration string for the given date span.
        # TODO fix duration of seconds
        duration = end - start
        if duration.days >= 365:
            duration = "{} Y".format(int(duration.days/365))
        elif duration.days < 365 and duration.days > 1:
            if isinstance(start, datetime.datetime):
                start = start.date()
            if isinstance(end, datetime.datetime):
                end = end.date()                
            n_days = np.busday_count(start, end)
            duration = "{} D".format(n_days)
        else:
            duration = "{} S".format(duration.seconds)
        # Get the bar data for each symbol
        bars = {}
        for contract in contracts:
            try:
                # Use the 'localSymbol' as a unique label for the time series
                symbol = contract.localSymbol
                assert symbol and symbol not in bars, \
                                        'Missing (unique) local symbol.'                
                bars[symbol] = self._req_historical_data(
                    contract,
                    end=end.strftime("%Y%m%d %H:%M:%S"),
                    duration=duration,
                    bar_size=bar_size,
                    info="TRADES",
                    rth=rth
                )
            except HistoricalRequestError as err:
                print(err.message)

        # Format the bars dictionary for conversion into DataFrame
        bars = {(outerKey, innerKey): values for outerKey, innerDict
                in bars.items() for innerKey, values in innerDict.items()}
        bars = pd.DataFrame(bars)

        # Reindex the bars using real time stamps.
        if (bar_size.find("secs") != -1 or bar_size.find("min") != -1 or
            bar_size.find("hour") != -1):
            index = [datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
                     for d in bars.index]
        else:
            index = [datetime.datetime.strptime(d, "%Y-%m-%d") for d in bars.index]
        bars.index = index

        # Try to get rid of any missing data.
        bars.fillna(method="ffill", inplace=True)

        return bars

    def _req_historical_data(self, contract, end="", duration="20 D",
                             bar_size="1 day", info="TRADES", rth=False):
        """Get historical data using reqHistoricalData. Upon completion the
        server will callback historicalData, which is overridden.
        http://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration

        Arguments:
        contract (str): Contract object
        end (datetime.datetime): Last date/time requested
        duration (str): How far to go back - valid options: (S, D, W, M, Y)
        bar_size (str): Bar size (see link)
        info (str): Type of data to return (see link)
        rth (bool): Return data only in regular trading hours
        """
        # Create a unique string ID for this request
        request_string = self._get_historical_ts_request_string(contract=contract, end=end, 
                            duration=duration, bar_size=bar_size, info=info, rth=rth)
        
        # Only make API call if this request is new. Otherwise, retrieve cached data
        if self._is_new_request(request_string):
            self._historical_data[request_string] = []
            req_id = self._get_next_req_id()
            self._log_request_id(req_id, label=request_string)
            self.reqHistoricalData(req_id, 
                                   contract=contract, 
                                   endDateTime=end, 
                                   durationStr=duration, 
                                   barSizeSetting=bar_size,
                                   whatToShow=info, 
                                   useRTH=rth, 
                                   formatDate=1,  # possible values are 1 (str) or 2 (int)
                                   keepUpToDate=False,
                                   chartOptions=[])

        # Wait until the request has returned (make it blocking).
        start = datetime.datetime.now()
        while request_string not in self._historical_data_req_end:
            if (datetime.datetime.now() - start).seconds > MAX_WAIT_TIME:
                raise HistoricalRequestError(
                    "Timeout occurred while retrieving price data for {}".format(str(contract)),
                    "_req_historical_data({})".format(str(contract)))
            time.sleep(0.2)

        # Handle the case where no historical was found
        return self._clean_cached_ts_data(self._historical_data[request_string])

    def _get_historical_ts_request_string(self, contract, end="", duration="20 D",
                             bar_size="1 day", info="TRADES", rth=False):
        """Create a unique string for a historical time series data request."""        
        return '{}_{}_{}_{}_{}_{}'.format(contract, end, duration, bar_size, info, rth)
    
    def _log_request_id(self, req_id, label):
        """Keep track of request IDs and their human-readable labels."""
        self._req_ids_to_string_ids[req_id] = label
        self._string_ids_to_req_ids[label] = req_id

    def _get_req_id_from_label(self, label):
        return self._string_ids_to_req_ids[label]
    
    def _get_label_from_req_id(self, req_id):
        return self._req_ids_to_string_ids[req_id]
    
    def _is_new_request(self, label):
        """Check if a label has been logged with a request."""
        return label not in self._req_ids_to_string_ids
    
    def _clean_cached_ts_data(self, bar_data):
        """Clean cached time series data, based on a request string."""
        # Extract the time series data from an array of BarData objects
        data = pd.DataFrame([b.__dict__ for b in bar_data])
        data['date'] = [self._extract_date(d) for d in data['date']]
        data = data.set_index('date')
        return data

    def _convert_to_tws_date_format(self, d):
        """Convert a datetime object to the date format TWS requires 
           for historical data requests.
           """
        return d.strftime('%Y%m%d %H:%M %Z')
    
    def _extract_date(self, d):
        """Extract datetime information from IB's date format."""
        if len(d) == 8:
            fmt = '%Y%m%d'
        else:
            fmt = '%Y%m%d %H:%M:%S'
        return datetime.datetime.strptime(d, fmt)
    
    def historicalData(self, reqId: int, bar: BarData):
        """Overridden method from EWrapper. Checks to make sure reqId matches
        the self.historical_data[req_id] to confirm correct symbol.
        """
        label = self._get_label_from_req_id(reqId)        
        self._historical_data[label].append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """Overrides the EWrapper method.
        """
        label = self._get_label_from_req_id(reqId)
        self._historical_data_req_end[label] = datetime.datetime.now()

    def get_histogram(self, localSymbol, period="20 days"):
        """Get histograms of the local symbols (the unique IB tickers).

        Arguments:
        localSymbol (str): ticker symbol or list of ticker symbols.
        period (str): Number of days to collect data.

        Returns (?): Histograms of the symbols
        """
        # If only a single symbol is passed convert it
        # to a list with a single item.

        contract = self.get_contract(localSymbol)
        self._histogram = None
        req_id = self._get_next_req_id()        
        self.reqHistogramData(req_id, contract, False, period)
        t0 = time.time()
        while self._histogram is None and time.time() - t0 < MAX_WAIT_TIME:
            time.sleep(0.2)

        # Handle the case where no historical data is found
        if not p:
            return None
        
        histogram = pd.DataFrame(
            columns=["price", "count"],
            data=[[float(p.price), int(p.count)] for p in self._histogram]
        )

        return histogram

    def histogramData(self, reqId: int, items: HistogramDataList):
        """EWrapper method called from reqHistogramData.
        http://interactivebrokers.github.io/tws-api/histograms.html
        """
        self._histogram = items

    def keyboardInterrupt(self):
        """Stop exectution.
        """
        pass

    def quick_bracket(self, symbol=None, instruction=None, quantity=None,
                      amount=1000, limit_percent=None, profit_percent=None):
        """Calculate bracket order for symbol using a limit provided by
        limit_percent.

        Arguments
        symbol (str): Ticker symbol
        instruction (str): "BUY" | "SELL"
        quantity (int): Number of shares
        amount (float): Amount in dollars to trade
        limit_percent (float): Percent change from current quote to set limit.
        profit_percent (float): Percent change from limit price to take profit.

        Returns (dict) Parameters necessary to place a bracket order.
        """
        # Calculate a reasonable change if limit_percent is not given.
        if limit_percent is None:
            if instruction == "BUY":
                limit_percent = -0.3
            if instruction == "SELL":
                limit_percent = 0.3

        # Calculate a reasonable change if limit_percent is not given.
        if profit_percent is None:
            if instruction == "BUY":
                profit_percent = 0.3
            if instruction == "SELL":
                profit_percent = -0.3

        # Get the quote
        quote = self.get_quotes(symbol).loc[symbol]

        # Calculate the limit price from the limit_percent.
        limit_price = round(quote * (1 + limit_percent/100.), 2)
        # Calculate the profit price from the limit_price.
        profit_price = round(limit_price * (1 + profit_percent/100.), 2)

        # Calculate quantity if amount was provided.
        if quantity is None:
            quantity = int(amount / quote)

        req_order = {
            'symbol': symbol,
            'instruction': instruction,
            'quantity': quantity,
            'price': limit_price,
            'tif': "DAY",
            'outside_rth': True,
            'profit_price': profit_price,
            'stop_price': None
        }
        self.create_bracket_orders(req_orders=[req_order])

        for order_id in list(self.get_saved_orders(symbol).keys()):
            self.place_order(order_id=order_id)


def main(port=7497):
    """Entry point into the program.

    Arguments:
    port (int): Port number that IBGateway, or TWS is listening.
    """
    global API_THREAD
    print('imported ib_api')
    try:
        app = IBApp()
        app.connect("127.0.0.1", port, clientId=0)
        print("serverVersion:%s connectionTime:%s" % (app.serverVersion(),
                                                      app.twsConnectionTime()))
        API_THREAD = threading.Thread(target=app.run)
        API_THREAD.start()
        return app
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    import sys
    # port number socker server is using (paper: 7497, live: 7496)
    PORT_NUMBER = sys.argv[1]
    main(port=PORT_NUMBER)
