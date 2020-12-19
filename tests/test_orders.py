import unittest
import sys, os

import pandas as pd

import ibapi

import constants
import master


class OrdersTest(unittest.TestCase):
    def setUp(self):
        """ Perform any required set-up before each method call. """
        pass
        
    def tearDown(self):
        """ Remove anything from 'setUp' after each method call. """
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

    @classmethod
    def tearDownClass(cls):
        """ Perform any required tear-down once, after all methods have been run. 
            
            This method can be used to destroy any structures created in setUpClass.
        """
        cls.app.disconnect()
        del cls.app

    def test_get_saved_orders(self):
        """ Test that we can retrieve saved orders.
        """
        pass

    def test_place_order(self):
        """ Test that we can place saved orders.
        """
        pass
    
    def test_place_all_orders(self):
        """ Test that we can place all saved orders.
        """
        pass
    
    def test_get_open_orders(self):
        """ Test that we can retrieve open orders.
        """
        pass
    
    def test_create_market_order(self):
        # Get the next liquid ES contract
        _contract = self.app.find_next_live_future(min_days_until_expiry=10, symbol='ES',
                                                   exchange='GLOBEX', currency='USD')

        # Get the order information
        N = 1
        mkt_order = self.app.create_market_order(_contract, action='BUY', totalQuantity=N)
        
        # Find the original position size before trading
        q_0 = self.get_position_size(_contract.localSymbol)
        
        
        
    def test_create_simple_orders(self):
        """ Test that we can create simple orders.
        """
        pass
    
    def test_create_bracket_orders(self):
        """ Test that we can create bracket orders.
        """
        pass
    
    def test_create_trailing_stop_orders(self):
        """ Test that we can create trailing stop orders.
        """
        pass

    def test_create_stop_limit_orders(self):
        """ Test that we can create stop limit orders.
        """
        pass

    def test_create_pegged_orders(self):
        """ Test that we can create pegged orders.
        """
        pass

    def test_quick_bracket(self):
        """ Test that we can create quick bracket orders.
        """
        pass


if __name__ == '__main__':
    unittest.main()