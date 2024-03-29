import ibk.account
import ibk.constants
import ibk.connect
import ibk.contracts
import ibk.marketdata
import ibk.orders


MARKETDATA_MAX_WAIT_TIME = 60


class Master(object):
    def __init__(self, port, host=None):
        super().__init__()
        self._port = port
        self.connection_manager = ibk.connect.ConnectionManager(port=port, host=host)

    def disconnect(self):
        """ Disconnect from IB Gateway (Reset all connections). """
        self.reset_connections()

    def reset_connections(self):
        """ Reset all connections. """
        self.connection_manager.reset_connections()
        
    def __del__(self):
        """ Define the destructor - close all connections. """
        self.disconnect()
        
    ##################################################################
    # Contracts
    ##################################################################

    def get_contract_details(self, localSymbol: str):
        """ Get saved ContractDetails object, as specified by its 'localSymbol'.
        
            Arguments:
                localSymbol: (str) a unique symbol specifying the instrument.
        """
        return self.contracts_app.get_contract_details(localSymbol=localSymbol)

    def get_contract(self, localSymbol: str):
        """ Get saved Contract object, as specified by its 'localSymbol'.
        
            Arguments:
                localSymbol: (str) a unique symbol specifying the instrument.
        """
        return self.contracts_app.get_contract(localSymbol=localSymbol)

    def find_matching_contract_details(self, max_wait_time=None, **kwargs):
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
        return self.contracts_app.find_matching_contract_details(
                                    max_wait_time=max_wait_time, **kwargs)

    def find_best_matching_contract_details(self, max_wait_time=None, **kwargs):
        """Find 'best' contract among possibilities matching desired attributes.

        Arguments:
            max_wait_time (int): the maximum time (in seconds) to wait
                        for a response from the IB API
            kwargs: The key/value pairs of variables that appear in the
                ibapi.contract.Contract class. The user can specify
                as many or as few of these as desired.

        Returns: (Contract) the 'best' matching Contract object.
        """
        return self.contracts_app.find_best_matching_contract_details(
                                        max_wait_time=max_wait_time, **kwargs)

    def find_next_live_futures_contract(self, max_wait_time=None, 
                                       min_days_until_expiry=1, **kwargs):
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
        return self.contracts_app.find_next_live_futures_contract(max_wait_time=max_wait_time,
                        min_days_until_expiry=min_days_until_expiry, **kwargs)

    def get_continuous_futures_contract_details(self, symbol, **kwargs):
        """ Get the continuous futures ContractDetails for a given symbol.
        """
        return self.contracts_app.get_continuous_futures_contract_details(symbol, **kwargs)
    
    def save_contracts(self):
        """ Save the cached contract details to file.
        
            Calling this method saves the cached contract details
            to a file, so that they can be reused without querying
            the IB server. The file containing all of the 
            contract details is loaded in the __init__ method.
        """
        self.contracts_app.save_contracts()

    def get_trading_intervals(self, contract_details, liquid_hours=False):
        """ Extract the trading intervals for a ContractDetails object.

            Arguments:
                contract_details: (ContractDetails) the contract details
                    for which we want to find trading hours.
                liquid_hours: (bool) whether we want to just use the liquid
                    hours instead of all trading hours.
        """
        return self.contracts_app.get_trading_intervals(contract_details,
                                                        liquid_hours=liquid_hours)

    def is_in_trading_hours(self, contract, target=None, liquid_hours=False):
        """ Determine whether a contract is trading at a given time.

            Arguments:
                contract: (Contract/ContractDetails) the contract or 
                    contract details for which trading hours are desired.
                target: (datetime) the time at which we want to check
                    if the contract is trading. If no input is provided,
                    then the current time is used.
                liquid_hours: (bool) whether we want to just use the liquid
                    hours instead of all trading hours.
        """
        return self.contracts_app.is_in_trading_hours(contract, target=target,
                                                      liquid_hours=liquid_hours)
        
    ##################################################################
    # Accounts and Positions
    ##################################################################

    def get_positions(self, include_mv=False):
        positions_df, contracts = self.account_app.get_positions()
        if include_mv:
            positions_df = self._include_mv_in_positions(positions_df)
        return positions_df, contracts

    def get_account_summary(self, group='All', tags="$LEDGER"):
        """ Get the summary of the account. """
        return self.account_app.get_account_summary(group=group,
                                                    tags=tags)

    def get_total_account_value(self):
        return self.account_app.get_total_account_value()

    def get_portfolio_info(self, acct_num=None, max_wait_time=None):
        """ Get the portfolio information, including positions and market value.
        """
        return self.account_app.get_portfolio_info(acct_num=acct_num,
                                                   max_wait_time=max_wait_time)

    def get_account_info(self, acct_num=None, max_wait_time=None):
        """ Get the account information, including margin and cash balances.
        """
        return self.account_app.get_account_info(acct_num=acct_num,
                                                 max_wait_time=max_wait_time)

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

    def get_fundamental_data(self, contractList, report_type="ratios", options=None):
        reqObjList = self.marketdata_app.create_fundamental_data_request(contractList,
                                                                         report_type=report_type,
                                                                         options=options)

        # Place data requests for all request objects
        [reqObj.place_request() for reqObj in reqObjList]
        return reqObjList

    def get_historical_data(self, contractList, frequency, use_rth=True, data_type="TRADES",
                            start="", end="", duration="", is_snapshot=True, place=True):
        reqObjList = self.marketdata_app.create_historical_data_request(contractList,
                                                                        is_snapshot=is_snapshot,
                                                                        frequency=frequency,
                                                                        use_rth=use_rth,
                                                                        data_type=data_type,
                                                                        start=start,
                                                                        end=end,
                                                                        duration=duration)
        # Place data requests for all request objects
        if place:
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
        return self.connection_manager.get_connection(ibk.contracts.ContractsApp)

    @property
    def orders_app(self):
        return self.connection_manager.get_connection(ibk.orders.OrdersApp)

    @property
    def marketdata_app(self):
        return self.connection_manager.get_connection(ibk.marketdata.MarketDataApp)

    @property
    def account_app(self):
        return self.connection_manager.get_connection(ibk.account.AccountApp)

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
