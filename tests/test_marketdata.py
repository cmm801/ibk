import unittest
import sys, os

import pandas as pd

import ibapi

import constants
import master


class MarketDataTest(unittest.TestCase):
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

    def test_get_open_streams(self):
        # get_open_streams(self):
        print('\n###################################################################')
        print('Need to implement test for "get_open_streams" in "marketdata.py".')
        print('###################################################################')

    def test_create_market_data_request(self):
        # create_market_data_request(self, contractList, is_snapshot, fields=""):
        print('\n###################################################################')
        print('Need to implement test for "create_market_data_request" in "marketdata.py".')
        print('###################################################################')

    def test_create_historical_data_request(self):
        # create_historical_data_request(self, contractList, is_snapshot, frequency,
        #                               use_rth=DEFAULT_USE_RTH, data_type="TRADES",
        #                               start="", end="", duration=""):
        print('\n###################################################################')
        print('Need to implement test for "create_historical_data_request" in "marketdata.py".')
        print('###################################################################')

    def test_create_streaming_bar_data_request(self):
        # create_streaming_bar_data_request(self, contractList, frequency='5s', 
        #                                  use_rth=DEFAULT_USE_RTH, data_type="TRADES"):
        print('\n###################################################################')
        print('Need to implement test for "create_streaming_bar_data_request" in "marketdata.py".')
        print('###################################################################')

    def test_create_streaming_tick_data_request(self):
        # create_streaming_tick_data_request(self, contractList, data_type="Last",
        #                                number_of_ticks=1000, ignore_size=True):
        print('\n###################################################################')
        print('Need to implement test for "create_streaming_tick_data_request" in "marketdata.py".')
        print('###################################################################')

    def test_create_historical_tick_data_request(self):
        # create_historical_tick_data_request(self, contractList, use_rth=DEFAULT_USE_RTH, data_type="Last",
        #                               start="", end="", number_of_ticks=1000):
        print('\n###################################################################')
        print('Need to implement test for "create_historical_tick_data_request" in "marketdata.py".')
        print('###################################################################')

    def test_create_first_date_request(self):
        # create_first_date_request(self, contractList, use_rth=DEFAULT_USE_RTH, data_type='TRADES'):
        print('\n###################################################################')
        print('Need to implement test for "create_first_date_request" in "marketdata.py".')
        print('###################################################################')

    def test_create_fundamental_data_request(self):
        # create_fundamental_data_request(self, contract, report_type, options=None):
        print('\n###################################################################')
        print('Need to implement test for "create_fundamental_data_request" in "marketdata.py".')
        print('###################################################################')

    def test_create_scanner_data_request(self):
        # create_scanner_data_request(self, scanSubObj, options=None, filters=None):
        print('\n###################################################################')
        print('Need to implement test for "create_scanner_data_request" in "marketdata.py".')
        print('###################################################################')

    def test_get_scanner_parameters(self, max_wait_time=2):
        """ Test the method to 'get_scanner_parameters'. """
        # test_get_scanner_parameters(self)
        print('\n###################################################################')
        print('Need to implement test for "get_scanner_parameters" in "marketdata.py".')
        print('###################################################################')


if __name__ == '__main__':
    unittest.main()