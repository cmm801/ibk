import contracts
import orders
import marketdata
import account
import helper

class Master(object):
    def __init__(self, port):
        super().__init__()
        self._port = port
    
    ##################################################################
    # Contracts
    ##################################################################
    def get_contract(self, localSymbol: str):
        contracts_app = self._get_contracts_app()
        return contracts_app.get_contract(localSymbol=localSymbol)

    def match_contract(self, partial_contract, max_wait_time=contracts.MAX_WAIT_TIME):
        contracts_app = self._get_contracts_app()
        return contracts_app.match_contract(partial_contract=partial_contract,
                                         max_wait_time=max_wait_time)

    ##################################################################
    # Accounts and Positions
    ##################################################################
    def get_positions(self, include_mv=False):
        account_app = self._get_account_app()
        positions_df, contracts = account_app.get_positions()        
        if include_mv:
            positions_df = self._include_mv_in_positions(positions_df)            
        return positions_df, contracts
        
    def get_account_details(self):
        account_app = self._get_account_app()
        return account_app.get_account_details()
    
    def get_total_account_value(self):
        account_app = self._get_account_app()
        return account_app.get_total_account_value()

    ##################################################################
    # Market Data
    ##################################################################    

    def get_market_data_snapshots(self, contractList, fields="", max_wait_time=None):
        if max_wait_time is None:
            max_wait_time = marketdata.MAX_WAIT_TIME
        md_app = self._get_marketdata_app()
        return md_app.get_market_data_snapshots(contractList, fields=fields, \
                                                    max_wait_time=max_wait_time)
    
    def open_historical_data_streams(self, contractList, frequency, use_rth=True, 
                             data_type="TRADES", start="", end="", duration=""):
        md_app = self._get_marketdata_app()        
        return md_app.open_historical_data_streams(self, contractList, frequency, 
                   use_rth=True, data_type="TRADES", start="", end="", duration="")

    def close_historical_data_streams(self, contractList, frequency, use_rth=True, 
                             data_type="TRADES", start="", end="", duration=""):
        md_app = self._get_marketdata_app()        
        return md_app.close_historical_data_streams(self, contractList, frequency, 
                   use_rth=True, data_type="TRADES", start="", end="", duration="")    
    
    def get_historical_data(self, contractList, frequency, use_rth=True, data_type="TRADES",
                                start="", end="", duration="", max_wait_time=None):
        if max_wait_time is None:
            max_wait_time = marketdata.MAX_WAIT_TIME
        md_app = self._get_marketdata_app()
        return md_app.get_historical_data(contractList, frequency=frequency, use_rth=use_rth,
            data_type=data_type, start=start, end=end, duration=duration, max_wait_time=max_wait_time)

    def get_histogram(self, contract, period="20d"):
        md_app = self._get_marketdata_app()
        return md_app.get_histogram(contract, period=period)
    
    ##################################################################
    # Orders
    ##################################################################
    def get_saved_orders(self, localSymbol=None):
        orders_app = self._get_orders_app()
        return orders_app.get_saved_orders(localSymbol=localSymbol)
        
    def place_order(self, order_id=None):
        orders_app = self._get_orders_app()
        return orders_app.place_order(order_id=order_id)
            
    def place_all_orders(self):
        orders_app = self._get_orders_app()
        return orders_app.place_all_orders()
         
    def get_open_orders(self):
        orders_app = self._get_orders_app()
        return orders_app.get_open_orders()        
    
    def create_simple_orders(self, req_orders=None, transmit=False):
        orders_app = self._get_orders_app()        
        return self.create_simple_orders(req_orders=req_orders, transmit=transmit)
    
    def create_bracket_orders(self, req_orders=None, transmit=False):
        orders_app = self._get_orders_app()
        return orders_app.create_bracket_orders(req_orders=req_orders, transmit=transmit)
        
    def create_trailing_stop_orders(self, req_orders=None, transmit=False):
        orders_app = self._get_orders_app()
        return orders_app.create_trailing_stop_orders(req_orders=req_orders, transmit=transmit)
        
    def create_stop_limit_orders(self, req_orders=None, transmit=False):
        orders_app = self._get_orders_app()
        return orders_app.create_stop_limit_orders(req_orders=req_orders, transmit=transmit)
    
    def create_pegged_orders(self, req_orders=None, transmit=False):
        orders_app = self._get_orders_app()
        return orders_app.create_pegged_orders(self, req_orders=req_orders, transmit=transmit)
    
    def quick_bracket(self, symbol=None, action=None, quantity=None, amount=None,
                            limit_percent=None, profit_percent=None, transmit=False):
        orders_app = self._get_orders_app()
        return orders_app.quick_bracket(symbol=symbol, action=action, 
                    quantity=quantity, amount=amount, limit_percent=limit_percent, 
                    profit_percent=profit_percent, transmit=transmit)
    
    ##################################################################
    # Methods to retrieve specialized helper classes
    ##################################################################    
    def _get_contracts_app(self):
        return contracts.get_instance(port=self._port)
                
    def _get_orders_app(self):
        return orders.get_instance(port=self._port)
    
    def _get_marketdata_app(self):
        return marketdata.get_instance(port=self._port)
    
    def _get_account_app(self):
        return account.get_instance(port=self._port)        
    
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