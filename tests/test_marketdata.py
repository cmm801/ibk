import unittest
import sys, os
import time
import datetime

import pandas as pd

import ibapi

import constants
import master
import marketdata
import requestmanager


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
        print('Running test_get_open_streams...')
        # Get the contract list
        tickers = ['SPY']
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
            if len(self.mdapp.get_open_streams()):
                print('Open streams: ', \
                  [self.mdapp.request_manager.requests[req_id] for req_id in self.mdapp.get_open_streams()])
            self.assertEqual(len(self.mdapp.get_open_streams()), 0, msg='There should be no streams open.')

        # Check the status of the request objects
        for reqObj in reqObjList:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertEqual(reqObj.status, requestmanager.STATUS_REQUEST_NOT_PLACED)

        # Place requests
        [reqObj.place_request() for reqObj in reqObjList]

        # Check the status of the request objects
        for reqObj in reqObjList:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertEqual(reqObj.status, requestmanager.STATUS_REQUEST_ACTIVE)

        # Wait a moment for requests to propogate
        time.sleep(1)

        # Check that streams are open now
        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(len(self.mdapp.get_open_streams()), 0, msg='There should be 1 stream open.')

        # Close all streams
        [reqObj.close_stream() for reqObj in reqObjList]

        # Check the status of the request objects
        for reqObj in reqObjList:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertEqual(reqObj.status, requestmanager.STATUS_REQUEST_COMPLETE)

        # Check that all streams are closed now
        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(len(self.mdapp.get_open_streams()), 0, msg='There should be no streams open.')


    def test_create_market_data_request_snapshot(self):
        """ Test the method create_market_data_request.
        """
        print('Running test_create_market_data_request_snapshot...')        
        # Get the contract list
        tickers = ['AAPL', 'MSFT']
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
            keys = set(['CLOSE', 'BID', 'ASK', 'BID_SIZE', 'ASK_SIZE'])
            ctr += 1
            with self.subTest(i=ctr):
                missing = list(keys - set(reqObj.get_data().keys()))
                self.assertEqual(0, len(missing), 
                                msg='Some expected data keys are missing: {}'.format(missing))
            ctr += 1
            with self.subTest(i=ctr):
                self.assertTrue(reqObj.get_data()['CLOSE'] > 0, 
                                msg='The "CLOSE" value should always be positive.')

    def test_create_historical_data_request_snapshot(self):
        """ Test the method create_historical_data_request when is_snapshot == True.
        """        
        print('Running test_create_historical_data_request_snapshot...')                
        # Get the contract list
        tickers = ['JNK']
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
        print('Running test_create_historical_data_request_streaming...')
        # Get the contract list
        tickers = ['IBM']
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
        """ Test that method 'create_streaming_bar_data_request' works as expected.
        """
        print('Running test_create_streaming_bar_data_request...')        
        # Create a list of contracts
        contractList = [self.app.get_contract('GS')]

        # Create the request object
        reqObjList = self.mdapp.create_streaming_bar_data_request(contractList, frequency='5s', 
                                                             use_rth=False, data_type="TRADES")
        reqObj = reqObjList[0]
        
        # Place the requests
        reqObj.place_request()

        # Check that the request is included in the open streams 
        # (we must run this test immediately after placing the request, because
        #  the callback can close the stream on its own outside of RTH)
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIn(reqObj.get_req_ids()[0], self.mdapp.get_open_streams())
        
        # Sleep until there is some data populating the request
        t0 = time.time()
        while not len(reqObjList[0].get_data()) and time.time() - t0 < 5:
            time.sleep(0.1)

        # Get the data
        ts_data = reqObj.get_data()
        
        # Check that the data is of the expected class
        ctr += 1
        with self.subTest(i=ctr):
            self.assertGreater(len(ts_data), 0, 
                                 msg='Expected some data to be returned.')

        if len(ts_data):
            # Check that the data is returned as a list of dict objects
            ctr += 1
            with self.subTest(i=ctr):
                self.assertIsInstance(len(ts_data) and ts_data[0], dict, 
                                     msg='Expected some data to be returned as a dict.')

            # Check that the data has the expected fields
            for key in ['date', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount']:
                ctr += 1
                with self.subTest(i=ctr):
                    self.assertIn(key, ts_data[0])

        # Close the stream
        reqObj.close_stream()
        
        # Check that the stream has been closed
        ctr += 1
        with self.subTest(i=ctr):
            self.assertNotIn(reqObj.get_req_ids()[0], self.mdapp.get_open_streams())

    def test_create_streaming_tick_data_request(self):
        """ Test method 'create_streaming_tick_data_request'.
        """
        print('Running test_create_streaming_tick_data_request...')                
        # Get a single contract
        contractList = [self.app.contracts_app.find_next_live_future_contract(symbol='VIX',
                                                                              exchange='CFE')]
        
        # Create the request object
        n_ticks = 50
        reqObjList = self.mdapp.create_streaming_tick_data_request(contractList, 
                    data_type="Last", number_of_ticks=n_ticks, ignore_size=True)

        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(reqObjList, list)

        reqObj = reqObjList[0]
        
        # Place the request
        reqObj.place_request()

        # Check that the request is included in the open streams 
        # (we must run this test immediately after placing the request, because
        #  the callback can close the stream on its own outside of RTH)
        ctr += 1
        with self.subTest(i=ctr):
            self.assertIn(reqObj.get_req_ids()[0], self.mdapp.get_open_streams())
        
        # Sleep until there is some data populating the request
        t0 = time.time()
        while not reqObj.get_data() and len(ts_data) < n_ticks and time.time() - t0 < 3:
            # Sleep up to a few seconds to wait for all the tick data to be returned.
            time.sleep(0.1)

        # Get the data
        ts_data = reqObj.get_data()[0]
        
        # Check that the data is of the expected class
        ctr += 1
        with self.subTest(i=ctr):
            self.assertIsInstance(ts_data[0], ibapi.common.HistoricalTickLast)

        # Check that the data is of the expected size
        ctr += 1
        with self.subTest(i=ctr):
            # The number of ticks returned must be at least as great as the @ requested            
            self.assertGreaterEqual(len(ts_data), n_ticks)

        # Close the stream
        reqObj.close_stream()

        # Check that the stream has been closed
        ctr += 1
        with self.subTest(i=ctr):
            self.assertNotIn(reqObj.get_req_ids()[0], self.mdapp.get_open_streams())

    def test_create_historical_tick_data_request(self):
        """ Test method 'create_historical_tick_data_request'.
        """
        print('Running test_create_historical_tick_data_request...')                        
        # Get a single contract
        contractList = [self.app.contracts_app.find_next_live_future_contract(symbol='ES',
                                                                              exchange='GLOBEX')]

        # Create the request object
        n_ticks = 80
        end = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d %H:%M:%S')        
        reqObjList = self.mdapp.create_historical_tick_data_request(contractList,
                    data_type="TRADES", number_of_ticks=n_ticks, end=end)

        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(reqObjList, list)

        reqObj = reqObjList[0]
        
        # Place the request
        reqObj.place_request()

        # Sleep until there is some data populating the request
        while not reqObj.get_data():
            time.sleep(0.1)

        # Get the data
        ts_data = reqObj.get_data()
        
        # Check that the data is of the expected class
        ctr += 1
        with self.subTest(i=ctr):
            self.assertIsInstance(ts_data[0], ibapi.common.HistoricalTickLast)

        # Check that the data is of the expected size
        ctr += 1
        with self.subTest(i=ctr):
            # The number of ticks returned must be at least as great as the @ requested
            self.assertGreaterEqual(len(ts_data), n_ticks)

    def test_create_first_date_request(self):
        """ Test whether create_first_date_request works.
        """
        print('Running test_create_first_date_request...')                                
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
        print('Running test_create_fundamental_data_request_ratios...')                                        
        # Create a list of contracts
        tickers = ['TSLA', 'NVS']
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
        """ Test the method for create_scanner_data_request.
        """
        print('Running test_create_scanner_data_request...')
        # Specify the number of scanner rows that we are requesting
        n_rows = 10
        
        # Create the ScannerSubscription object to specify parameters of scan
        scanSubObj = ibapi.client.ScannerSubscription()
        scanSubObj.instrument = 'STK'
        scanSubObj.locationCode = "STK.US.MAJOR"
        scanSubObj.scanCode = "TOP_PERC_GAIN"
        scanSubObj.numberOfRows = n_rows

        # Create a request object for the scanner
        reqObj = self.mdapp.create_scanner_data_request(scanSubObj)

        # Place the request
        reqObj.place_request()

        # We expect the output to be a list of dicts with contract info
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(reqObj.get_data(), list)

        ctr += 1
        with self.subTest(i=ctr):
            self.assertIsInstance(reqObj, marketdata.ScannerDataRequest)

        # Wait for the request to be completed
        while not len(reqObj.get_data()) == n_rows:
            time.sleep(0.1)

        # Check that these keys are all present
        keys = ['rank', 'contractDetails', 'distance', 'benchmark',
                'projection', 'legsStr']
        for data_row in reqObj.get_data()[0]:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertTrue(all([k in data_row for k in keys]), 
                                msg='Some expected data keys are missing.')

        # Check if the scanner is being counted as an open stream
        ctr += 1
        with self.subTest(i=ctr):
            self.assertIn(reqObj.get_req_ids()[0], self.mdapp.get_open_streams())
            
        # Close the scanner stream
        reqObj.close_stream()
                
    def test_get_scanner_parameters(self, max_wait_time=2):
        """ Test the method to 'get_scanner_parameters'.
        """
        print('Running test_get_scanner_parameters...')        
        
        # Get the scanner parameters
        scanner_params = self.mdapp.get_scanner_parameters()        

        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(scanner_params, dict)

        ctr += 1
        with self.subTest(i=ctr):
            self.assertIn('InstrumentList', scanner_params)

        ctr += 1
        with self.subTest(i=ctr):
            self.assertIn('Instrument', scanner_params['InstrumentList'])

        # Get the instrument information
        instrument_df = pd.DataFrame.from_dict(scanner_params['InstrumentList']['Instrument'])
        instrument_df.set_index('name', inplace=True)

        ctr += 1
        with self.subTest(i=ctr):
            self.assertIn('US Stocks', instrument_df.index)

        ctr += 1
        with self.subTest(i=ctr):
            self.assertIn('US Equity ETFs', instrument_df.index)


if __name__ == '__main__':
    unittest.main()