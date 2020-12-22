import constants
import connect

MARKETDATA_MAX_WAIT_TIME = 60


class Master(object):
    def __init__(self, port):
        super().__init__()
        self._port = port
        self._connection_info = connect.ConnectionInfo(port)

    def disconnect(self):
        """ Disconnect from IB Gateway (Reset all connections). """
        self.reset_connections()

    def reset_connections(self):
        """ Reset all connections. """
        self._connection_info.reset_connections()
        
    def __del__(self):
        """ Define the destructor - close all connections. """
        self.disconnect()
        
    ##################################################################
    # Contracts
    ##################################################################

    def get_contract(self, localSymbol: str):
        """ Get a Contract object, as specified by its 'localSymbol'.
        
            Arguments:
                localSymbol: (str) a unique symbol specifying the instrument.
        """
        return self.contracts_app.get_contract(localSymbol=localSymbol)

    def find_matching_contracts(self, max_wait_time=None, **kwargs):
        """Find a list of matching contracts given some desired attributes.

        Arguments:
            max_wait_time (int): the maximum time (in seconds) to wait
                        for a response from the IB API
            kwargs: The key/value pairs of variables that appear in the
                ibapi.contract.Contract class. The user can specify
                as many or as few of these as desired.

        Returns: (list) a list of ContractDetails objects - one for each
            possible matching contract.
        """
        return self.contracts_app.find_matching_contracts(max_wait_time=max_wait_time, **kwargs)

    def find_best_matching_contract(self, max_wait_time=None, **kwargs):
        """Find 'best' contract among possibilities matching desired attributes.

        Arguments:
            max_wait_time (int): the maximum time (in seconds) to wait
                        for a response from the IB API
            kwargs: The key/value pairs of variables that appear in the
                ibapi.contract.Contract class. The user can specify
                as many or as few of these as desired.

        Returns: (Contract) the 'best' matching Contract object.
        """
        return self.contracts_app.find_best_matching_contract(max_wait_time=max_wait_time,
                                                              **kwargs)

    def find_next_live_future(self, max_wait_time=None, min_days_until_expiry=1, **kwargs):
        """Find the next live contract for a given future.

        Arguments:
            max_wait_time (int): the maximum time (in seconds) to wait
                for a response from the IB API
            min_days_until_expiry: (int) the fewest allowable days until
                expiry that is allowed for the future's contract
            kwargs: The key/value pairs of variables that appear in the
                ibapi.contract.Contract class. The user can specify
                as many or as few of these as desired.

        Returns: (Contract) the 'best' matching Contract object.
        """
        return self.contracts_app.find_next_live_future(max_wait_time=max_wait_time,
                        min_days_until_expiry=min_days_until_expiry, **kwargs)

    ##################################################################
    # Accounts and Positions
    ##################################################################

    def get_positions(self, include_mv=False):
        positions_df, contracts = self.account_app.get_positions()
        if include_mv:
            positions_df = self._include_mv_in_positions(positions_df)
        return positions_df, contracts

    def get_account_details(self):
        return self.account_app.get_account_details()

    def get_total_account_value(self):
        return self.account_app.get_total_account_value()

    def get_position_size(self, localSymbol):
        """ Get the position size for a given local symbol.
        
            Returns 0.0 if there is no position in the symbol.
            Arguments:
                localSymbol: (str) the unique local string symbol
                    for the given instrument.
        """
        return self.account_app.get_position_size(localSymbol)

    ##################################################################
    # Market Data
    ##################################################################

    def get_market_data_snapshots(self, contractList, fields=""):
        reqObjList = self.marketdata_app.create_market_data_request(contractList,
                                                                    is_snapshot=True,
                                                                    fields=fields)
        # Place data requests for all request objects
        [reqObj.place_request() for reqObj in reqObjList]
        return reqObjList

    def get_historical_data(self, contractList, frequency, use_rth=True, data_type="TRADES",
                            start="", end="", duration="", is_snapshot=True):
        reqObjList = self.marketdata_app.create_historical_data_request(contractList,
                                                                        is_snapshot=is_snapshot,
                                                                        frequency=frequency,
                                                                        use_rth=use_rth,
                                                                        data_type=data_type,
                                                                        start=start,
                                                                        end=end,
                                                                        duration=duration)
        # Place data requests for all request objects
        [reqObj.place_request() for reqObj in reqObjList]
        return reqObjList

    def open_historical_data_streams(self, contractList, frequency, use_rth=True,
                             data_type="TRADES", start="", end="", duration=""):
        md_app = self.marketdata_app
        return md_app.open_historical_data_streams(self, contractList, frequency,
                   use_rth=True, data_type="TRADES", start="", end="", duration="")

    def close_historical_data_streams(self, contractList, frequency, use_rth=True,
                             data_type="TRADES", start="", end="", duration=""):
        md_app = self.marketdata_app
        return md_app.close_historical_data_streams(self, contractList, frequency,
                   use_rth=True, data_type="TRADES", start="", end="", duration="")

    def get_histogram(self, contract, period="20d"):
        md_app = self.marketdata_app
        return md_app.get_histogram(contract, period=period)

    ##################################################################
    # Orders
    ##################################################################

    def get_open_orders(self, max_wait_time=None):
        return self.orders_app.get_open_orders(max_wait_time=max_wait_time)

    def cancel_orders(self, order_ids):
        return self.orders_app.cancel_orders(order_ids)
        
    def cancel_all_orders(self):
        return self.orders_app.cancel_all_orders()        

    def create_order(self, contract, action, totalQuantity, orderType, **kwargs):
        """ Create a generic order. """
        return self.orders_app.create_order(contract, action=action, 
                                            totalQuantity=totalQuantity,
                                            orderType=orderType, **kwargs)
        
    def create_market_order(self, contract, action, totalQuantity, **kwargs):
        """ Create a market order.

            Arguments:
                contract (Contract): Contract object to be traded
                action (str): "BUY" | "SELL"
                totalQuantity (float): Order quantity (units of the contract).        
        """
        return self.orders_app.create_market_order(contract, action=action, 
                                                   totalQuantity=totalQuantity, **kwargs)
        
    def create_limit_order(self, contract, action, totalQuantity, lmtPrice, **kwargs):
        """ Create a limit order.

            Arguments:
                contract (Contract): Contract object to be traded
                action (str): "BUY" | "SELL"
                totalQuantity (float): Order quantity (units of the contract).
                lmtPrice (float): the limit price
        """
        return self.orders_app.create_limit_order(contract, action=action,
                                                  totalQuantity=totalQuantity, 
                                                  lmtPrice=lmtPrice, **kwargs)

    def create_bracket_order(self, req_orders=None, transmit=False):
        return self.orders_app.create_bracket_order(req_orders=req_orders, 
                                                     transmit=transmit)

    def create_trailing_stop_order(self, req_orders=None, transmit=False):
        return self.orders_app.create_trailing_stop_order(req_orders=req_orders, 
                                                           transmit=transmit)

    def create_stop_limit_order(self, req_orders=None, transmit=False):
        return self.orders_app.create_stop_limit_order(req_orders=req_orders, 
                                                        transmit=transmit)

    ##################################################################
    # Methods to retrieve specialized helper classes
    ##################################################################

    @property
    def contracts_app(self):
        return self._connection_info.get_connection(constants.CONTRACTS)

    @property
    def orders_app(self):
        return self._connection_info.get_connection(constants.ORDERS)

    @property
    def marketdata_app(self):
        return self._connection_info.get_connection(constants.MARKETDATA)

    @property
    def account_app(self):
        return self._connection_info.get_connection(constants.ACCOUNT)

    ##################################################################
    # Private functions
    ##################################################################

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
