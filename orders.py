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
import time
import threading

from ibapi.contract import Contract
from ibapi.common import OrderId
from ibapi.order import Order
from ibapi.order_state import OrderState

from base import BaseApp

__CLIENT_ID = 0       # Client id 0 is the main application
MAX_WAIT_TIME = 5     # max time to wait for TWS response (in seconds)

class OrdersApp(BaseApp):
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
        super().__init__()
        self._saved_orders = {}

    def _get_next_order_id(self):
        """Overload the Base class method to get order ID (the same as the request ID)."""
        return self._get_next_req_id()
    
    def get_saved_orders(self, localSymbol=None):
        """Return saved orders for localSymbol. If localSymbol is None
        return all saved orders.

        Returns (dict) {order_id: {order: order, contract: contract}}
        """
        if localSymbol is None:
            return self._saved_orders

        orders = dict()
        for oid, order in self._saved_orders.items():
            if order['contract'].localSymbol == localSymbol:
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

    def get_open_orders(self, max_wait_time=MAX_WAIT_TIME):
        """Call the IBApi.EClient reqOpenOrders. Open orders are returned via
        the callback openOrder.
        """
        self._open_orders = []
        self._open_order_request_complete = False
        self.reqOpenOrders()
        
        start_time = time.time()
        while not self._open_order_request_complete and time.time() - start_time < max_wait_time:
            time.sleep(0.1)

        return self._open_orders
    
    def create_simple_orders(self, req_orders=None, transmit=False):
        """Create orders, but do not place.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            contract (Contract): Contract object to be traded
            action (str): "BUY" | "SELL"
            price (float): Order set price.
            quantity (float): Order quantity.
            outside_rth (bool): outside regular trading hours
            tif (str): Time in force "DAY" | "GTC"
            profit_price (float): Price for profit taking
            stop_price (float): Price for stop loss
        """
        new_orders = dict()
        for req_order in req_orders:
            order_id = self._get_next_order_id()
            _order = Order()
            _order.orderId = order_id
            _order.action = req_order['action']
            _order.orderType = req_order['orderType']
            _order.totalQuantity = req_order['quantity']
            _order.lmtPrice = req_order['price']
            _order.outsideRth = req_order['outside_rth']
            _order.tif = req_order['tif']
            _order.transmit = transmit
            
            new_orders[order_id] = {
                "order": _order, 
                "contract": req_order['contract'],
            }

            self._saved_orders.update(new_orders)
            return new_orders
        
    def create_bracket_orders(self, req_orders=None, transmit=False):
        """Create orders, but do not place.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            contract (Contract): Contract object to be traded
            action (str): "BUY" | "SELL"
            price (float): Order set price.
            quantity (float): Order quantity.
            outside_rth (bool): outside regular trading hours
            tif (str): Time in force "DAY" | "GTC"
            profit_price (float): Price for profit taking
            stop_price (float): Price for stop loss
            parent_id (int): Id of parent trade.
        """
        new_orders = dict()
        for req_order in req_orders:
            # Create the parent order
            order_id = self._get_next_order_id()
            parent = Order()
            parent.orderId = order_id
            parent.action = req_order['action']
            parent.orderType = "LMT"
            parent.totalQuantity = req_order['quantity']
            parent.lmtPrice = req_order['price']
            parent.outsideRth = req_order['outside_rth']
            parent.tif = req_order['tif']
            parent.transmit = transmit
            
            new_orders[order_id] = {
                "order": parent, 
                "contract": req_order['contract'],
            }

            # Create the profit taker order
            if req_order['profit_price'] is not None:
                order_id = self._get_next_order_id()
                profit_taker = Order()
                profit_taker.orderId = order_id
                profit_taker.action = "SELL"\
                    if req_order['action'] == "BUY" else "BUY"
                profit_taker.orderType = "LMT"
                profit_taker.totalQuantity = req_order['quantity']
                profit_taker.lmtPrice = req_order['profit_price']
                profit_taker.parentId = parent.orderId
                profit_taker.transmit = transmit
                
                new_orders[order_id] = {
                    "order": profit_taker, 
                    "contract": req_order['contract'],
                }

            # Create stop loss order
            if req_order['stop_price'] is not None:
                order_id = self._get_next_order_id()
                stop_loss = Order()
                stop_loss.orderId = order_id
                stop_loss.action = "SELL"\
                    if req_order['action'] == "BUY" else "BUY"
                stop_loss.orderType = "STP"
                stop_loss.auxPrice = req_order['stop_price']
                stop_loss.totalQuantity = req_order['quantity']
                stop_loss.parentId = parent.orderId
                stop_loss.transmit = transmit
                
                new_orders[order_id] = {
                    "order": stop_loss, 
                    "contract": req_order['contract'],
                }
            self._saved_orders.update(new_orders)
            return new_orders

    def create_trailing_stop_orders(self, req_orders=None, transmit=False):
        """Create a trailing stop order.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            contract (Contract): Contract object to be traded
            action (str): "BUY" | "SELL"
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
        new_orders = dict()
        for req_order in req_orders:
            # Create the order
            order_id = self._get_next_order_id()
            order = Order()
            order.orderId = order_id
            order.action = req_order['action']
            order.orderType = "TRAIL LIMIT"
            order.totalQuantity = req_order['quantity']
            order.trailStopPrice = req_order['trail_stop_price']
            order.auxPrice = req_order['trail_amount']
            order.lmtPriceOffset = req_order['limit_offset']
            order.outsideRth = req_order['outside_rth']
            order.tif = req_order['tif']
            order.transmit = transmit
            # TODO parent_id
            
            new_orders[order_id] = {
                "order": order, 
                "contract": req_order['contract'],
            }
            
            self._saved_orders.update(new_orders)
            return new_orders

    def create_stop_limit_orders(self, req_orders=None, transmit=False):
        """Create a trailing stop order.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            contract (Contract): Contract object to be traded
            action (str): "BUY" | "SELL"
            quantity (float): Order quantity.
            stop_price (float): stop price
            limit_price (float): limit price.
            outside_rth (bool): outside regular trading hours
            tif (str): Time in force "DAY" | "GTC"
            profit_price (float): Profit taking price.
        """
        new_orders = dict()
        for req_order in req_orders:
            # Create the order
            order_id = self._get_next_order_id()
            order = Order()
            order.orderId = order_id
            order.action = req_order['action']
            order.orderType = "STP LMT"
            order.totalQuantity = req_order['quantity']
            order.lmtPrice = req_order['limit_price']
            order.auxPrice = req_order['stop_price']
            order.outsideRth = req_order['outside_rth']
            order.tif = req_order['tif']
            order.transmit = transmit
            
            new_orders[order_id] = {
                "order": order,
                "contract": req_order['contract'],
            }

            # Create the profit taker order
            if req_order['profit_price'] is not None:
                profit_taker_order_id = self._get_next_order_id()
                profit_taker = Order()
                profit_taker.orderId = profit_taker_order_id
                profit_taker.action = "SELL"\
                    if req_order['action'] == "BUY" else "BUY"
                profit_taker.orderType = "LMT"
                profit_taker.totalQuantity = req_order['quantity']
                profit_taker.lmtPrice = req_order['profit_price']
                profit_taker.parentId = order.orderId
                profit_taker.transmit = transmit
                
                new_orders[profit_taker_order_id] = {
                    "order": profit_taker, 
                    "contract": req_order['contract']
                }
                
            self._saved_orders.update(new_orders)
            return new_orders

    def create_pegged_orders(self, req_orders=None, transmit=False):
        """Create a pegged to bench mark order.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            contract (Contract): Contract object to be traded
            action (str): "BUY" | "SELL"
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
        new_orders = dict()
        for req_order in req_orders:
            # Create the parent order
            order_id = self._get_next_order_id()
            order = Order()
            order.orderId = order_id
            order.orderType = "PEG BENCH"
            order.action = req_order['action']
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
            order.transmit = transmit
            
            new_orders[order_id] = {
                "order": order,
                "contract": req_order['contract'],
            }
            
            self._saved_orders.update(new_orders)
            return new_orders

    def quick_bracket(self, symbol=None, action=None, quantity=None, amount=None,
                         limit_percent=None, profit_percent=None, transmit=False):
        """Calculate bracket order for symbol using a limit provided by
        limit_percent.

        Arguments
        symbol (str): Ticker symbol
        action (str): "BUY" | "SELL"
        quantity (int): Number of shares
        amount (float): Amount in dollars to trade
        limit_percent (float): Percent change from current quote to set limit.
        profit_percent (float): Percent change from limit price to take profit.

        Returns (dict) Parameters necessary to place a bracket order.
        """
        # Calculate a reasonable change if limit_percent is not given.
        if limit_percent is None:
            if action == "BUY":
                limit_percent = -0.3
            if action == "SELL":
                limit_percent = 0.3

        # Calculate a reasonable change if limit_percent is not given.
        if profit_percent is None:
            if action == "BUY":
                profit_percent = 0.3
            if action == "SELL":
                profit_percent = -0.3

        # Get the quote
        raise NotImplementedError('Need to incorporate new market data queries here.')
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
            'action': action,
            'quantity': quantity,
            'price': limit_price,
            'tif': "DAY",
            'outside_rth': True,
            'profit_price': profit_price,
            'stop_price': None
        }
        
        self.create_bracket_orders(req_orders=[req_order], transmit=transmit)
        for order_id in list(self.get_saved_orders(symbol).keys()):
            self.place_order(order_id=order_id)

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
        
    def openOrderEnd(self):
        super().openOrderEnd()
        self._open_order_request_complete = True

        
# Declare global variables used to handle the creation of a singleton class
__app = __port = __api_thread = None

def get_instance(port=7497):
    """Entry point into the program.

    Arguments:
    port (int): Port number that IBGateway, or TWS is listening.
    """
    global __app, __port, __api_thread
    if isinstance(__app, BaseApp) and __app.isConnected():
        if __port is None:
            return ValueError('Port information has been losw. Something has gone wrong.')
        elif __port != port:
            raise ValueError('Application is already open on another port.')
        else:
            # The connection is already open
            return __app
    else:
        try:
            __app = OrdersApp()
            __app.connect("127.0.0.1", port=port, clientId=__CLIENT_ID)
            print("serverVersion:%s connectionTime:%s" % (__app.serverVersion(),
                                                          __app.twsConnectionTime()))
            __api_thread = threading.Thread(target=__app.run)
            __api_thread.start()
            
            print('MarketDataApp connecting to IB...')
            while __app.req_id() is None:
                time.sleep(0.2)
            print('MarketDataApp connected.')
            
            __port = port
            return __app
        except KeyboardInterrupt:
            pass