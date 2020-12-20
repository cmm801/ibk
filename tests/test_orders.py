import time
import unittest
import sys, os

import pandas as pd

import ibapi

import constants
import master


class OrdersTest(unittest.TestCase):
    def setUp(self):
        """ Perform any required set-up before each method call. """        
        # Make sure there are no open orders upon beginning the tests
        #self.app.cancel_all_orders()
        pass
        
    def tearDown(self):
        """ Remove anything from 'setUp' after each method call. """
        # Make sure there are no open orders upon leaving the tests
        #self.app.cancel_all_orders()
        pass

    @classmethod
    def setUpClass(cls):
        """ Perform any required set-up once, before any method is run. 
        
            This method should be used to build any classes or data structures
            that will be used by more than one of the test methods, and
            that cannot be built quickly on-the-fly.
        """
        PORT = constants.PORT_PAPER

        # After execution, TWS will prompt you to accept the connection
        # The ERROR simply confirms that there is a connection to the market data.
        cls.app = master.Master(port=PORT)
        
        # Get the next liquid ES contract
        cls.ES_contract = cls.app.find_next_live_future(min_days_until_expiry=10, symbol='ES',
                                                          exchange='GLOBEX', currency='USD')
        # Get a contract for Apple stock
        cls.AAPL_contract = cls.app.get_contract('AAPL')

    @classmethod
    def tearDownClass(cls):
        """ Perform any required tear-down once, after all methods have been run. 
            
            This method can be used to destroy any structures created in setUpClass.
        """
        # Clean up by canceling all open order
        cls.app.cancel_all_orders()

        # Disconnect the client
        cls.app.disconnect()
        
        # Delete the client
        del cls.app

    def test_get_open_orders(self):
        """ Test that we can retrieve open orders.
        """
        # Create a limit order 
        limit_order = self.app.create_limit_order(self.AAPL_contract, action='BUY',
                                                  totalQuantity=3, lmtPrice=1)
        
        # Place the limit order
        order_id = limit_order.order_id
        self.app.place_orders(order_id)

        # Get the open orders - sleep if needed until order propogates
        t0 = time.time()
        max_wait_time = 10
        open_orders = self.app.get_open_orders()
        while order_id not in open_orders and (time.time() - t0) < max_wait_time:
            time.sleep(0.5)
            open_orders = self.app.get_open_orders()

        # Check that the new order is in the open orders
        self.assertIs(limit_order, open_orders[limit_order.order_id],
                msg="The open order is not what was expected.")
        
        # Clean up by canceling the order
        self.app.cancel_orders(limit_order.order_id)

    def test_place_orders(self):
        """ Test that we can place orders.
        """
        print('\n###################################################################')
        print('Need to implement test for "place_orders" in "test_orders.py".')
        print('###################################################################')

    def test_cancel_orders(self):
        """ Test that we can cancel open orders.
        """
        print('\n###################################################################')
        print('Need to implement test for "cancel_orders" in "test_orders.py".')
        print('###################################################################')
    
    def test_cancel_all_orders(self):
        """ Test that we can cancel all open orders.
        """
        print('\n###################################################################')
        print('Need to implement test for "cancel_all_orders" in "test_orders.py".')
        print('###################################################################')

    def test_create_order_buy(self):
        """ Test that we can create a generic buy order.
        """
        # Create a limit order
        _contract = self.AAPL_contract
        action = 'BUY'
        order_type = 'MKT'
        qty = 3

        # Create the order
        order_group = self.app.create_order(_contract, action=action,
                                            totalQuantity=qty, orderType=order_type)
        
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIs(_contract, order_group.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(order_type, order_group.order.orderType, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(qty, order_group.order.totalQuantity, msg='Quantity mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(action, order_group.order.action, msg='Mismatched action.')

    def test_create_order_sell(self):
        """ Test that we can create a generic sell order.
        """
        # Create a limit order 
        _contract = self.ES_contract
        action = 'SELL'
        qty = 12
        order_type = 'LMT'

        # Create the order
        order_group = self.app.create_order(_contract, action=action,
                                            totalQuantity=qty, orderType=order_type)
        
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIs(_contract, order_group.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(order_type, order_group.order.orderType, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(qty, order_group.order.totalQuantity, msg='Quantity mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(action, order_group.order.action, msg='Mismatched action.')

    def test_create_market_order(self):
        """ Test that we can create a market order.
        """
        # Create a limit order 
        _contract = self.ES_contract
        action = 'BUY'
        qty = 5

        # Create the order
        order_group = self.app.create_market_order(_contract, action=action, totalQuantity=qty)
        
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIs(_contract, order_group.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual('MKT', order_group.order.orderType, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(qty, order_group.order.totalQuantity, msg='Quantity mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(action, order_group.order.action, msg='Mismatched action.')

    def test_create_limit_order(self):
        """ Test that we can create a limit order.
        """        
        # Create a limit order 
        _contract = self.AAPL_contract
        action = 'SELL'
        qty = 17
        limit_price = 22.0

        # Create the order
        order_group = self.app.create_limit_order(_contract, action=action, 
                                                  totalQuantity=qty, lmtPrice=limit_price)
        
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIs(_contract, order_group.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual('LMT', order_group.order.orderType, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(qty, order_group.order.totalQuantity, msg='Quantity mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(action, order_group.order.action, msg='Mismatched action.')

    def test_create_bracket_order(self):
        """ Test that we can create bracket orders.
        """
        print('\n###################################################################')
        print('Need to implement test for "create_bracket_order" in "test_orders.py".')
        print('###################################################################')
    
    def test_create_trailing_stop_order(self):
        """ Test that we can create trailing stop orders.
        """
        print('\n###################################################################')
        print('Need to implement test for "create_trailing_stop_order" in "test_orders.py".')
        print('###################################################################')


    def test_create_stop_limit_order(self):
        """ Test that we can create stop limit orders.
        """
        print('\n###################################################################')
        print('Need to implement test for "create_stop_limit_order" in "test_orders.py".')
        print('###################################################################')


if __name__ == '__main__':
    unittest.main()