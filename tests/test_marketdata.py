import unittest
import sys, os
import time
import datetime

import pandas as pd

import ibapi

import constants
import master
import marketdata


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
        cls.mdapp = cls.app.marketdata_app

    @classmethod
    def tearDownClass(cls):
        """ Perform any required tear-down once, after all methods have been run. 
            
            This method can be used to destroy any structures created in setUpClass.
        """
        cls.app.disconnect()
        del cls.app

    def test_get_open_streams(self):
        """ Test whether method get_open_streams works properly.
        """
        # Get the contract list
        tickers = ['IBM', 'SPY', 'JNK']
        contractList = [self.app.get_contract(tkr) for tkr in tickers]
        
        is_snapshot = False
        data_type = 'TRADES'
        frequency='1d'
        duration='10d'
        use_rth = True
        reqObjList = self.mdapp.create_historical_data_request(contractList, is_snapshot,
                                                               frequency, data_type=data_type,
                                                               duration=duration)

        # Check that there are no streams open
        ctr = 0
        with self.subTest(i=ctr):
            self.assertEqual(len(self.mdapp.get_open_streams()), 0, msg='There should be no streams open.')

        # Check the status of the request objects
        for reqObj in reqObjList:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertEqual(reqObj.status, marketdata.STATUS_REQUEST_NOT_PLACED)

        # Place requests
        [reqObj.place_request() for reqObj in reqObjList]

        # Check the status of the request objects
        for reqObj in reqObjList:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertEqual(reqObj.status, marketdata.STATUS_REQUEST_ACTIVE)

        # Wait a moment for requests to propogate
        time.sleep(1)

        # Check that streams are open now
        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(len(self.mdapp.get_open_streams()), 0, msg='There should be 3 streams open.')

        # Close all streams
        [reqObj.close_stream() for reqObj in reqObjList]

        # Check the status of the request objects
        for reqObj in reqObjList:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertEqual(reqObj.status, marketdata.STATUS_REQUEST_COMPLETE)

        # Check that all streams are closed now
        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(len(self.mdapp.get_open_streams()), 0, msg='There should be no streams open.')


    def test_create_market_data_request_snapshot(self):
        """ Test the method create_market_data_request.
        """
        # Get the contract list
        tickers = ['AAPL', 'MSFT', 'GS']
        contractList = [self.app.get_contract(tkr) for tkr in tickers]

        # Create the request objects
        is_snapshot = True  # Work with a snapshot
        reqObjList = self.mdapp.create_market_data_request(contractList, is_snapshot)

        # We expect the output to be a list of request objects
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(reqObjList, list)

        # Place requests
        [x.place_request() for x in reqObjList]
        
        # Check the details of the individual requests
        for reqObj in reqObjList:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertIsInstance(reqObj, marketdata.MarketDataRequest)

            # Wait for the request to be completed
            while not reqObj.get_data():
                time.sleep(0.1)
            
            # Check that these keys are all present
            keys = ['CLOSE', 'BID', 'ASK', 'BID_SIZE', 'ASK_SIZE']
            ctr += 1
            with self.subTest(i=ctr):
                self.assertTrue(all([x in reqObj.get_data() for x in keys]), 
                                msg='Some expected data keys are missing.')
            ctr += 1
            with self.subTest(i=ctr):
                self.assertTrue(reqObj.get_data()['CLOSE'] > 0, 
                                msg='The "CLOSE" value should always be positive.')

    def test_create_historical_data_request_snapshot(self):
        """ Test the method create_historical_data_request when is_snapshot == True.
        """        
        # Get the contract list
        tickers = ['IBM', 'SPY', 'JNK']
        contractList = [self.app.get_contract(tkr) for tkr in tickers]
        
        is_snapshot = True
        data_type = 'TRADES'
        frequency='1d'
        duration='10d'
        use_rth = True
        reqObjList = self.mdapp.create_historical_data_request(contractList, is_snapshot,
                                                               frequency, data_type=data_type,
                                                               duration=duration)
        # We expect the output to be a list of request objects
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(reqObjList, list)

        # Place requests
        [x.place_request() for x in reqObjList]
        for reqObj in reqObjList:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertIsInstance(reqObj, marketdata.HistoricalDataRequest)

            # Wait for the request to be completed
            while not len(reqObj.get_data()[0]) == 10:
                time.sleep(0.1)
            
            # Check that these keys are all present
            keys = ['date', 'open', 'high', 'low', 'close', 'barCount', 'average']
            for data_row in reqObj.get_data()[0]:
                ctr += 1
                with self.subTest(i=ctr):
                    self.assertTrue(all([k in data_row for k in keys]), 
                                    msg='Some expected data keys are missing.')

        # Check that there are no streams open
        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(len(self.mdapp.get_open_streams()), 0, msg='There should be no open streams.')

    def test_create_historical_data_request_streaming(self):
        """ Test the method create_historical_data_request when is_snapshot == False.
        """        
        # Get the contract list
        tickers = ['IBM', 'SPY', 'JNK']
        contractList = [self.app.get_contract(tkr) for tkr in tickers]
        
        is_snapshot = False
        data_type = 'TRADES'
        frequency='1d'
        duration='10d'
        use_rth = True
        reqObjList = self.mdapp.create_historical_data_request(contractList, is_snapshot,
                                                               frequency, data_type=data_type,
                                                               duration=duration)
        # We expect the output to be a list of request objects
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(reqObjList, list)

        # Place requests
        [x.place_request() for x in reqObjList]
        for reqObj in reqObjList:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertIsInstance(reqObj, marketdata.HistoricalDataRequest)

            # Wait for the request to be completed
            while not len(reqObj.get_data()[0]) == 10:
                time.sleep(0.1)
            
            # Check that these keys are all present
            keys = ['date', 'open', 'high', 'low', 'close', 'barCount', 'average']
            for data_row in reqObj.get_data()[0]:
                ctr += 1
                with self.subTest(i=ctr):
                    self.assertTrue(all([k in data_row for k in keys]), 
                                    msg='Some expected data keys are missing.')

        # Close all streams
        [reqObj.close_stream() for reqObj in reqObjList]

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
        # Get the contract list
        tickers = ['EWW', 'EWJ', 'EWP']
        contractList = [self.app.get_contract(tkr) for tkr in tickers]
        
        # Create the request object
        reqObjList = self.mdapp.create_first_date_request(contractList, data_type='TRADES')
        
        # Place the request
        [reqObj.place_request() for reqObj in reqObjList]
        
        # Sleep until the requests are complete
        for idx, reqObj in enumerate(reqObjList):
            reqObj.place_request()
            while reqObj.get_data() is None:
                time.sleep(0.2)

        # Get the first dates
        first_dates = [reqObj.get_data() for reqObj in reqObjList]
        
        # Check that the first dates are valid
        ctr = 0
        for first_date in first_dates:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertIsInstance(first_date, datetime.datetime)

            ctr += 1
            with self.subTest(i=ctr):
                self.assertGreater(first_date, datetime.datetime(1990, 12, 31))

            ctr += 1
            with self.subTest(i=ctr):
                self.assertLess(first_date, datetime.datetime(2020, 12, 31))

    def test_create_fundamental_data_request_ratios(self):
        """ Test method 'create_fundamental_data_request' for input 'ratios'.
        """
        # Create a list of contracts
        tickers = ['GS', 'TSLA', 'NVS']
        contractList = [self.app.get_contract(tkr) for tkr in tickers]
        
        # Specify the type of fundamental data to request
        report_type = 'ratios'
        reqObjList = self.mdapp.create_fundamental_data_request(contractList, report_type=report_type)

        # Place requests
        [x.place_request() for x in reqObjList]

        # Check the details of the individual requests
        ctr = 0
        for reqObj in reqObjList:
            # Check the 
            ctr += 1
            with self.subTest(i=ctr):
                self.assertIsInstance(reqObj, marketdata.MarketDataRequest)

            ctr += 1
            with self.subTest(i=ctr):
                self.assertIsInstance(reqObj.get_data(), pd.Series)

            # Wait for the request to be completed
            while not len(reqObj.get_data()):
                time.sleep(0.1)
            
            # Check that these keys are all present
            keys = ['MKTCAP', 'NPRICE', 'NHIG', 'NLOW', 'BETA']
            ctr += 1
            with self.subTest(i=ctr):
                self.assertTrue(all([x in reqObj.get_data().index for x in keys]), 
                                msg='Some expected data keys are missing.')

    def test_create_scanner_data_request(self):
        # create_scanner_data_request(self, scanSubObj, options=None, filters=None):
        print('\n###################################################################')
        print('Need to implement test for "create_scanner_data_request" in "marketdata.py".')
        print('###################################################################')

    def test_get_scanner_parameters(self, max_wait_time=2):
        """ Test the method to 'get_scanner_parameters'. """
        # get_scanner_parameters(self)
        print('\n###################################################################')
        print('Need to implement test for "get_scanner_parameters" in "marketdata.py".')
        print('###################################################################')


if __name__ == '__main__':
    unittest.main()