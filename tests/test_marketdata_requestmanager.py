"""Tests for the market data GlobalRequestManager manager class.

The tests here should check the functionality of data requests without
actually sending requests to IB. This is done by mocking the critical
components of the interaction with IB, and returning simulated data.
The ability to retrieve actual market data from IB is tested instead
in the test_marketdata.py test suite.
"""

import datetime
import ibapi
import numpy as np
import pandas as pd
import time
import unittest
from unittest.mock import Mock

import ibk.constants
import ibk.marketdata
import ibk.marketdata.constants as mdconst


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
        return(False) # This test is still under contruction

        start = datetime.datetime(2022, 1, 4, 0)
        end = datetime.datetime(2022, 1, 7, 0)
        frequency = '1h'
        is_snapshot = True
        contract = self._get_contract_stock('SPY')
        reqObj = ibk.marketdata.create_historical_data_request(contract, is_snapshot,
                                                               frequency=frequency,
                                                               start=start, end=end)

        # Place the request
        reqObj.place_request()
        
        # Wait until it has been completed
        max_wait = 60
        t0 = time.time()
        while time.time() - t0 < max_wait and reqObj.is_active():
            time.sleep(0.2)

        if reqObj.is_active():
            raise ValueError('Request timed out.')

        # Collect the timestamps that each request was submitted to IB
        time_submitted = []
        req_status = reqObj.request_manager.requests
        for r in reqObj.subrequests:
            time_submitted.append(req_status[r.uniq_id].info[mdconst.STATUS_REQUEST_SENT_TO_IB])

        # Find the time between requests within the short window
        res_class = mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW
        N, T = ibk.marketdata.restrictionmanager.MAX_REQUESTS_PER_WINDOW[res_class]
        time_submitted_srt = np.array(sorted(time_submitted))
        delta = pd.Series(time_submitted_srt[1:] - time_submitted_srt[:-1])
        rolling_delta = delta.rolling(N).sum()
        print(rolling_delta.max(), T)
        
        #ctr = 0
        #with self.subTest(i=ctr):        
        #    self.assertEqual(cnt_1, single_order.contract, msg='Contract mismatch.')


if __name__ == '__main__':
    unittest.main()