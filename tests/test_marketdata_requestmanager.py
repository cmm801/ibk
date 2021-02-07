import datetime
import ibapi
import pandas as pd
import time
import unittest
from unittest.mock import Mock

import ibk.constants
import ibk.marketdata


class MockMarketDataApp:
    """ A class that provides the API's expected of a connection to IB. 
    
        This is used to test the different classes without actually
        sending requests to IB.
    """
    _internal_counter = [0]
    requests = dict()
    
    def _get_next_req_id(self):
        self._internal_counter[0] += 1        
        return self._internal_counter[0]

    def reqScannerSubscription(self, reqId, **kwargs):
        pass

    def cancelScannerSubscription(self, reqId):
        pass

    def reqMktData(self, reqId, **kwargs):
        pass
    
    def cancelMktData(self, reqId):
        pass
    
    def reqFundamentalData(self, reqId, **kwargs):
        pass
    
    def reqHistoricalData(self, reqId, **kwargs):
        pass
    
    def cancelHistoricalData(self, reqId):
        pass
    
    def reqRealTimeBars(self, reqId, **kwargs):
        pass
    
    def cancelRealTimeBars(self, reqId):
        pass
    
    def reqTickByTickData(self, reqId, **kwargs):
        pass
    
    def cancelTickByTickData(self, reqId):
        pass
    
    def reqHistoricalTicks(self, reqId, **kwargs):
        pass
    
    def cancelTickByTickData(self, reqId):
        pass
    
    def reqHeadTimeStamp(self, reqId, **kwargs):
        pass

    def reqScannerParameters(self):
        pass


class RequestManagerTest(unittest.TestCase):
    def setUp(self):
        """ Perform any required set-up before each method call. """
        self.request_manager = ibk.marketdata.GlobalRequestManager()
        
    def tearDown(self):
        """ Remove anything from 'setUp' after each method call. 
        
            Make sure that any market data requests are closed
        """
        del self.request_manager

    @classmethod
    def setUpClass(cls):
        """ Perform any required set-up once, before any method is run. 
        
            This method should be used to build any classes or data structures
            that will be used by more than one of the test methods, and
            that cannot be built quickly on-the-fly.
        """
        # Override the RequestManager so that uses the MockMarketDataApp instead of MarketDataApp
        ibk.marketdata.request_manager._get_app = Mock(return_value=MockMarketDataApp())

    @classmethod
    def tearDownClass(cls):
        """ Perform any required tear-down once, after all methods have been run. 
            
            This method can be used to destroy any structures created in setUpClass.
        """
        pass

    def _get_contract_stock(self, symbol):
        contract = ibapi.contract.Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "SMART"
        return contract
    
    def test_create_historical_data_request(self):
        """ Test that we can create a SingleOrder object.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        start = datetime.datetime(2020, 1, 1, 0)
        end = datetime.datetime(2020, 12, 31, 0)
        frequency = '1M'
        is_snapshot = True
        contract = self._get_contract_stock('SPY')
        reqObj = ibk.marketdata.create_historical_data_request(contract, is_snapshot,
                                                               frequency=frequency,
                                                               start=start, end=end)
        #ctr = 0
        #with self.subTest(i=ctr):        
        #    self.assertEqual(cnt_1, single_order.contract, msg='Contract mismatch.')


if __name__ == '__main__':
    unittest.main()