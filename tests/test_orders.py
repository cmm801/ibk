import time
import unittest
import sys, os

import pandas as pd

import ibapi

import ibk.constants
import ibk.master


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
        PORT = ibk.constants.PORT_PAPER

        # After execution, TWS will prompt you to accept the connection
        # The ERROR simply confirms that there is a connection to the market data.
        cls.app = ibk.master.Master(port=PORT)
        
        # Get the next liquid ES contract
        cls.ES_contract = cls.app.find_next_live_futures_contract(min_days_until_expiry=10,
                                                symbol='ES', exchange='CME', currency='USD')
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

    def test_place_and_cancel_single_order(self):
        """ Test that we can retrieve open orders.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        # Create a limit order 
        limit_order = self.app.create_limit_order(self.AAPL_contract, action='BUY',
                                                  totalQuantity=3, lmtPrice=50)
        
        # Place the order
        limit_order.place()

        # Get the open orders
        open_orders = self.app.get_open_orders(max_wait_time=10)

        # Check that the new order is in the open orders
        with self.subTest(i=0):
            self.assertEqual(limit_order, open_orders[limit_order.order_id],
                msg="The open order is not what was expected.")
        
        # Cancel the order
        limit_order.cancel()

        # Get the open orders and wait for cancelled order to propogate
        open_orders = self._get_open_orders_wait_for_propogation([limit_order.order_id])

        # Check that the order has been cancelled
        with self.subTest(i=1):        
            self.assertNotIn(limit_order.order_id, open_orders,
                msg="The cancelled order is still contained in open orders.")

    def test_place_and_cancel_group_order(self):
        """ Test that we can retrieve open orders.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        # Create an OrderGroup object with 2 orders
        limit_order_1 = self.app.create_limit_order(self.AAPL_contract, action='BUY',
                                                  totalQuantity=3, lmtPrice=50)
        limit_order_2 = self.app.create_limit_order(self.AAPL_contract, action='SELL',
                                                  totalQuantity=5, lmtPrice=1000)
        limit_order = limit_order_1 + limit_order_2
        
        # Place the limit order
        limit_order.place()

        # Get the open orders
        open_orders = self.app.get_open_orders(max_wait_time=10)

        # Check that the new order is in the open orders
        with self.subTest(i=0):
            self.assertEqual(limit_order_1, open_orders[limit_order_1.order_id],
                msg="The first open order is not what was expected.")

        with self.subTest(i=1):            
            self.assertEqual(limit_order_2, open_orders[limit_order_2.order_id],
                msg="The second open order is not what was expected.")

        # Clean up by canceling the order
        limit_order.cancel()
        
        # Get the open orders and wait for cancelled order to propogate
        open_orders = self._get_open_orders_wait_for_propogation(limit_order.order_ids)

        # Check that the order has been cancelled
        with self.subTest(i=2):
            self.assertNotIn(limit_order_1.order_id, open_orders,
                msg="The first cancelled order is still contained in open orders.")

        with self.subTest(i=3):
            self.assertNotIn(limit_order_2.order_id, open_orders,
                msg="The second cancelled order is still contained in open orders.")

    def test_cancel_orders(self):
        """ Test that we can cancel open orders.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        # Create an OrderGroup object with 2 orders
        limit_order_1 = self.app.create_limit_order(self.AAPL_contract, action='BUY',
                                                  totalQuantity=3, lmtPrice=50)
        limit_order_2 = self.app.create_limit_order(self.AAPL_contract, action='SELL',
                                                  totalQuantity=5, lmtPrice=1000)
        limit_order = limit_order_1 + limit_order_2
        
        # Place the limit order
        limit_order.place()

        # Get the open orders and wait for cancelled order to propogate
        open_orders = self.app.get_open_orders()

        # Check that the new order is in the open orders
        with self.subTest(i=0):
            self.assertEqual(limit_order_1, open_orders[limit_order_1.order_id],
                msg="The first open order is not what was expected.")

        with self.subTest(i=1):            
            self.assertEqual(limit_order_2, open_orders[limit_order_2.order_id],
                msg="The second open order is not what was expected.")

        # Clean up by canceling the order
        self.app.cancel_orders(limit_order.order_ids)

        # Get the open orders and wait for cancelled order to propogate
        open_orders = self._get_open_orders_wait_for_propogation(limit_order.order_ids)

        # Check that the order has been cancelled
        with self.subTest(i=2):
            self.assertNotIn(limit_order_1.order_id, open_orders,
                msg="The first cancelled order is still contained in open orders.")

        with self.subTest(i=3):
            self.assertNotIn(limit_order_2.order_id, open_orders,
                msg="The second cancelled order is still contained in open orders.")

    def test_cancel_all_orders(self):
        """ Test that we can cancel all open orders.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        # Create an OrderGroup object with 2 orders
        limit_order_1 = self.app.create_limit_order(self.AAPL_contract, action='BUY',
                                                  totalQuantity=3, lmtPrice=50)
        limit_order_2 = self.app.create_limit_order(self.AAPL_contract, action='SELL',
                                                  totalQuantity=5, lmtPrice=1000)
        
        # Place the limit order
        limit_order_1.place()
        limit_order_2.place()

        # Get the open orders and wait for cancelled order to propogate
        open_orders = self.app.get_open_orders()

        # Check that the new order is in the open orders
        with self.subTest(i=0):
            self.assertEqual(limit_order_1, open_orders[limit_order_1.order_id],
                msg="The first open order is not what was expected.")

        with self.subTest(i=1):            
            self.assertEqual(limit_order_2, open_orders[limit_order_2.order_id],
                msg="The second open order is not what was expected.")

        # Clean up by canceling all open orders
        self.app.cancel_all_orders()

        # Get the open orders and wait for cancelled order to propogate
        order_ids = [limit_order_1.order_id, limit_order_2.order_id]
        open_orders = self._get_open_orders_wait_for_propogation(order_ids)

        # Check that the order has been cancelled
        with self.subTest(i=2):
            self.assertEqual(0, len(open_orders), msg='There should be no open orders.')

    def test_create_order_buy(self):
        """ Test that we can create a generic buy order.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        # Create a limit order
        _contract = self.AAPL_contract
        action = 'BUY'
        order_type = 'MKT'
        qty = 3

        # Create the order
        single_order = self.app.create_order(_contract, action=action,
                                            totalQuantity=qty, orderType=order_type)
        
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIs(_contract, single_order.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(order_type, single_order.order.orderType, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(qty, single_order.order.totalQuantity, msg='Quantity mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(action, single_order.order.action, msg='Mismatched action.')

    def test_create_order_sell(self):
        """ Test that we can create a generic sell order.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        # Create a limit order 
        _contract = self.ES_contract
        action = 'SELL'
        qty = 12
        order_type = 'LMT'

        # Create the order
        single_order = self.app.create_order(_contract, action=action,
                                            totalQuantity=qty, orderType=order_type)
        
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIs(_contract, single_order.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(order_type, single_order.order.orderType, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(qty, single_order.order.totalQuantity, msg='Quantity mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(action, single_order.order.action, msg='Mismatched action.')

    def test_create_market_order(self):
        """ Test that we can create a market order.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        # Create a limit order 
        _contract = self.ES_contract
        action = 'BUY'
        qty = 5

        # Create the order
        single_order = self.app.create_market_order(_contract, action=action, totalQuantity=qty)
        
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIs(_contract, single_order.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual('MKT', single_order.order.orderType, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(qty, single_order.order.totalQuantity, msg='Quantity mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(action, single_order.order.action, msg='Mismatched action.')

    def test_create_limit_order(self):
        """ Test that we can create a limit order.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        # Create a limit order 
        _contract = self.AAPL_contract
        action = 'SELL'
        qty = 17
        limit_price = 50.0

        # Create the order
        single_order = self.app.create_limit_order(_contract, action=action, 
                                                  totalQuantity=qty, lmtPrice=limit_price)
        
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIs(_contract, single_order.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual('LMT', single_order.order.orderType, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(qty, single_order.order.totalQuantity, msg='Quantity mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(action, single_order.order.action, msg='Mismatched action.')

    def test_create_bracket_order(self):
        """ Test that we can create bracket orders.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        print('\n###################################################################')
        print('Need to implement test for "create_bracket_order" in "test_orders.py".')
        print('###################################################################')
    
    def test_create_trailing_stop_order(self):
        """ Test that we can create trailing stop orders.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        print('\n###################################################################')
        print('Need to implement test for "create_trailing_stop_order" in "test_orders.py".')
        print('###################################################################')


    def test_create_stop_limit_order(self):
        """ Test that we can create stop limit orders.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        print('\n###################################################################')
        print('Need to implement test for "create_stop_limit_order" in "test_orders.py".')
        print('###################################################################')


    def _get_open_orders_wait_for_propogation(self, order_ids, max_wait_time=10):
        """ Get open orders, but wait as long as necessary for some orders to be cancelled.
        """
        open_orders = self.app.get_open_orders(max_wait_time=max_wait_time)
        t0 = time.time()
        while any([oid in open_orders for oid in order_ids]) \
              and time.time() - t0 < max_wait_time:
            time.sleep(1)
            open_orders = self.app.get_open_orders(max_wait_time=max_wait_time)

        # Return the open orders
        return open_orders

                                              
if __name__ == '__main__':
    unittest.main()