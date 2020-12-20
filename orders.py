"""
Module to facilitate trading through Interactive Brokers's API
see: https://interactivebrokers.github.io/tws-api/index.html

Christopher Miller
Dec. 18, 2020

Classes
    OrdersApp (EClient): Inherits from BaseApp, and provides
        methods for creating and placing orders.

"""
import time
import numpy as np
import pandas as pd

import ibapi.order
import base

MAX_WAIT_TIME = 5     # max time to wait for TWS response (in seconds)

STATUS_NOT_PLACED = 'not_placed'
STATUS_PLACED = 'placed'
STATUS_FILLED = 'filled'
STATUS_CANCELLED = 'cancelled'
STATUS_INCOMPATIBLE = 'incompatible'

# Define a class to handle a single order
class SingleOrder():
    def __init__(self, contract, order, app=None):
        # Initially set 'status' flag to 'not submitted'
        self.status = STATUS_NOT_PLACED
        
        # Set the other input variables
        self.app = app
        self.contract = contract
        self.order = order

    @property
    def order_id(self):
        return self.order.orderId
    
    @property
    def single_orders(self):
        """ Auxiliary method to enable compatibility with OrderGroup. """
        return [self]

    def place(self):
        """ Place the order.
        """
        if self.status != STATUS_NOT_PLACED:
            raise ValueError(f'Cannot place an order unless its status is "not_placed".')
        else:
            self.app.placeOrder(self.order_id, self.contract, self.order)

            # Update the status after placing the order
            self.status = STATUS_PLACED
    
    def cancel(self):
        """ Cancel the order (if placed).
        """
        if self.status != STATUS_PLACED:
            raise ValueError('Cannot cancel an order unless its status is "placed".')
        else:
            self.app.placeOrder(self.order_id, self.contract, self.order)

            # Update the status after cancelling the order
            self.status = STATUS_CANCELLED

    def _is_compatible(self, other):
        """ Check if two different class objects can be combined. """
        return self.status == other.status

    def __add__(self, other):
        if not self._is_compatible(other):
            raise ValueError('The two instances are incompatible for being combined.')
        else:
            return self.to_group() + other.to_group()

    def __eq__(self, other):
        return (str(self.contract) == str(other.contract)) and (str(self.order) == str(other.order))
        
    def to_group(self):
        """ Cast SingleOrder to OrderGroup. """
        return OrderGroup.from_single_orders(self)
        

# Define a class to handle one or more orders
class OrderGroup():
    def __init__(self, contracts=None, orders=None, app=None):        
        self._single_orders = []

        if contracts is not None or orders is not None:
            if not isinstance(contracts, list):
                contracts = [contracts]

            if not isinstance(orders, list):
                orders = [orders]
                
            # Create SingleOrder objects for each order and add them
            #    to a list
            sng_ords = []
            for idx, ct in enumerate(contracts):
                sng_ord = SingleOrder(ct, orders[idx], app=app)
                sng_ords.append(sng_ord)
                
            # Set the single orders
            self.single_orders = sng_ords

    @property
    def single_orders(self):
        return self._single_orders

    @single_orders.setter
    def single_orders(self, sng_ords):
        """ Set the single_orders variable. Enforce uniqueness of order_ids.
        """
        # Check that all order IDs are unique
        oids = [so.order.orderId for so in sng_ords]
        if len(set(oids)) != len(oids):
            raise ValueError('Order Ids must all be unique.')
        else:
            self._single_orders = sng_ords
    
    @property
    def contracts(self):
        return [sng_ord.contract for sng_ord in self.single_orders]

    @property
    def orders(self):
        return [sng_ord.order for sng_ord in self.single_orders]

    @property
    def order_ids(self):
        return [sng_ord.order_id for sng_ord in self.single_orders]

    @property
    def status(self):
        order_status = list(set([sng_ord.status for sng_ord in self.single_orders]))
        if len(order_status) == 1:
            return order_status[0]
        else:
            return STATUS_INCOMPATIBLE

    def place(self):
        """ Place all orders in the group.
        """
        # Go through the orders and place them one by one
        for sng_ord in self.single_orders:
            sng_ord.place()

    def cancel(self):
        """ Cancel all orders in the group.
        """
        # Go through the orders and cancel them one by one
        for sng_ord in self.single_orders:
            sng_ord.cancel()

    def _is_compatible(self, other):
        """ Check if two different class objects can be combined. """
        return self.status == other.status

    def __add__(self, other):
        """ Combine two OrderGroup objects. """
        if not self._is_compatible(other):
            raise ValueError('The two instances are incompatible for being combined.')
        else:
            single_orders = self.single_orders + other.single_orders
            return OrderGroup.from_single_orders(single_orders)

    def __eq__(self, other):
        """ Check if two OrderGroup objects are equivalent. """
        res = len(self.single_orders) == len(other.single_orders)
        res = res and (self.__class__ == other.__class__)
        if not res:
            return False
        else:
            for idx, _ in enumerate(self.single_orders):
                if self.single_orders[idx] != other.single_orders[idx]:
                    return False
            return True
            
    def add(self, other):
        """ Add new SingleOrder or OrderGroup objects to the existing object.
        """
        # Make sure we are working with OrderGroup objects
        if isinstance(other, SingleOrder):
            other = other.to_group()
            
        if not self._is_compatible(other):
            raise ValueError('The two instances are incompatible for being combined.')
        elif set(self.order_ids) & set(other.order_ids):
            raise ValueError('Cannot add repeated order Ids to the object.')
        else:
            self.single_orders.extend(other.single_orders)
            
    @classmethod
    def from_single_orders(cls, single_orders):
        new_obj = OrderGroup()
        if isinstance(single_orders, SingleOrder):
            new_obj.single_orders = [single_orders]
        else:
            new_obj.single_orders = single_orders

        return new_obj
    
    def to_group(self):
        """ Auxiliary method to enable compatibility with SingleOrder. """        
        return self


class OrdersApp(base.BaseApp):
    """Main program class. The TWS calls nextValidId after connection, so
    the method is over-ridden to provide an entry point into the program.
    """
    def __init__(self):
        super().__init__()
        self.__open_orders = {}
    
    def _get_next_order_id(self):
        """Overload the Base class method to get order ID (the same as the request ID)."""
        return self._get_next_req_id()

    @property
    def open_orders(self):
        return self.get_open_orders()

    def get_open_orders(self, max_wait_time=None):
        """Call the IBApi.EClient reqOpenOrders. Open orders are returned via
        the callback openOrder.
        """
        if max_wait_time is None:
            max_wait_time = MAX_WAIT_TIME
        
        self.__open_orders = {}
        self._open_order_request_complete = False
        self.reqOpenOrders()

        start_time = time.time()
        while not self._open_order_request_complete and time.time() - start_time < max_wait_time:
            time.sleep(0.1)

        return self.__open_orders

    def cancel_orders(self, order_ids):
        """Cancel one or more open orders.

        Arguments:
            order_ids (list/int): The order_id(s) of previously open order(s).
        """
        if not isinstance(order_ids, list):
            order_ids = [order_ids]

        # Check that all order IDs are unique
        if len(set(order_ids)) != len(order_ids):
            raise ValueError('Order IDs must be unique.')
        
        # Go through the orders and cancel them one by one
        for order_id in order_ids:
            # Use the method inherited from IB EClient
            self.cancelOrder(order_id)

    def cancel_all_orders(self):
        """ Cancel all open orders. """
        # Use method inherited from IB EClient
        self.reqGlobalCancel()

    def create_order(self, contract, action, totalQuantity, orderType, **kwargs):
        """ Create a generic order.

            Arguments:
                contract (Contract): Contract object to be traded
                action (str): "BUY" | "SELL"
                totalQuantity (float): Order quantity (units of the contract).   
                orderType (str): Type of order - 'MKT', 'LMT', etc.
        """
        # Get the next order ID
        orderId = self._get_next_order_id()

        # Create an Order object with the minimal set of variables
        _order = ibapi.order.Order()
        _order.orderId = orderId
        _order.action = action
        _order.totalQuantity = totalQuantity
        _order.orderType = orderType

        # Set any additional specifications in the Order
        for key, val in kwargs.items():
            if not hasattr(_order, key):
                raise ValueError(f'Unsupported Order variable name was provided: {key}')
            elif val is None:                
                pass # keep the default values in this case
            else:
                _order.__setattr__(key, val)
        
        # Return the new order
        return SingleOrder(contract, _order, app=self)

    def create_market_order(self, contract, action, totalQuantity, **kwargs):
        """ Create a market order.

            Arguments:
                contract (Contract): Contract object to be traded
                action (str): "BUY" | "SELL"
                totalQuantity (float): Order quantity (units of the contract).        
        """
        if 'orderType' in kwargs:
            if kwargs['orderType'] != 'MKT':
                raise ValueError(f'Expected "orderType" to be "MKT" but instead found: {orderType}')
            else:
                del kwargs['orderType']

        # Create a basic Market order
        return self.create_order(contract, action, totalQuantity, orderType='MKT', **kwargs)

    def create_limit_order(self, contract, action, totalQuantity, lmtPrice, **kwargs):
        """ Create a limit order.

            Arguments:
                contract (Contract): Contract object to be traded
                action (str): "BUY" | "SELL"
                totalQuantity (float): Order quantity (units of the contract).
                lmtPrice (float): the limit price.
        """
        if 'orderType' in kwargs:
            if kwargs['orderType'] != 'LMT':
                raise ValueError(f'Expected "orderType" to be "LMT" but instead found: {orderType}')
            else:
                del kwargs['orderType']

        # Create a basic limit order
        return self.create_order(contract, action, totalQuantity, 
                                 orderType='LMT', lmtPrice=lmtPrice, **kwargs)

    def create_bracket_order(self, contract, action, totalQuantity, 
                             profitPrice, stopPrice, orderType='MKT',
                             lmtPrice=None, transmit=None, outsideRth=None, tif=None):
        """Create orders, but do not place.

        Arguments:
        req_orders (list): list of dictionaries - keys are:
            contract (Contract): Contract object to be traded
            action (str): "BUY" | "SELL"
            totalQuantity (float): Order quantity
            profitPrice (float): price at which to take profit
            stopPrice (float): price at which to stop loss
            lmtPrice (float): limit price for the parent leg (if this is a limit order)
            orderType (str): The type of order for the parent leg ('MKT' or 'LMT')

       NOTE: to get TWS to execute these orders properly, we must have the
            first two orders with transmit=False, and then when the last order
            goes to TWS, it uses the 'transmit' flag on this last order to properly
            handle the order group.
            
        """
        if orderType == 'LMT' and lmtPrice is None:
            raise ValueError('Must specify "lmtPrice" when orderType is "LMT".')

        # Create parent order
        parent = self.create_order(contract, action, totalQuantity,
                                   orderType=orderType, lmtPrice=lmtPrice, 
                                   transmit=False, outsideRth=outsideRth, tif=tif)

        # Create profit-taker leg
        profit_action = "SELL" if (req_order['action'] == "BUY") else "BUY"
        profit_leg = self.create_order(contract, profit_action, totalQuantity, 
                                       orderType='LMT', lmtPrice=profitPrice,
                                       transmit=False, outsideRth=outsideRth, tif=tif,
                                       parentId=parent.orderId)

        # Create stop-loss leg
        loss_action = "SELL" if (req_order['action'] == "BUY") else "BUY"
        loss_leg = self.create_order(contract, loss_action, totalQuantity,
                                     orderType='STP', auxPrice=stopPrice,
                                     transmit=transmit, outsideRth=outsideRth, tif=tif,
                                     parentId=parent.orderId)

        # Return a list with the orders combined into an OrderGroup
        return (parent + profit_leg) + loss_leg

    def create_trailing_stop_order(self, contract, action, totalQuantity, 
                                   trailStopPrice, trailAmount, lmtPriceOffset,  
                                   transmit=None, outsideRth=None, tif=None):
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
        return self.create_order(contract, action, totalQuantity,
                                 orderType="TRAIL LIMIT", lmtPriceOffset=lmtPriceOffset,
                                 trailStopPrice=trailStopPrice, auxPrice=trailAmount,
                                 transmit=False, outsideRth=outsideRth, tif=tif)

    def create_stop_limit_order(self, req_orders=None, transmit=False):
        """Create a stop limit order.

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
        if orderType == 'LMT' and lmtPrice is None:
            raise ValueError('Must specify "lmtPrice" when orderType is "LMT".')

        # Create parent order
        parent = self.create_order(contract, action, totalQuantity,
                                   orderType="STP LMT", lmtPrice=lmtPrice, auxPrice=stopPrice,
                                   transmit=False, outsideRth=outsideRth, tif=tif)

        # Create profit-taker leg
        profit_action = "SELL" if (req_order['action'] == "BUY") else "BUY"
        profit_leg = self.create_order(contract, profit_action, totalQuantity, 
                                       orderType='LMT', lmtPrice=profitPrice,
                                       transmit=False, outsideRth=outsideRth, tif=tif,
                                       parentId=parent.orderId)

        # Return a list with the orders
        return parent + profit_leg

    def _update_open_orders(self, _order):
        self.__open_orders[_order.order_id] = _order

    ########################################################################
    # Implement callback methods
    ########################################################################
    
    def openOrder(self, orderId, contract, order, orderState):
        """Callback from reqOpenOrders(). Method is over-ridden from the
        EWrapper class.
        """
        super().openOrder(orderId, contract, order, orderState)
        order_info = SingleOrder(contract, order, app=self)
        self._update_open_orders(order_info)

    def openOrderEnd(self):
        super().openOrderEnd()
        self._open_order_request_complete = True
