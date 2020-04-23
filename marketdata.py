"""
Module to facilitate getting data through Interactive Brokers's API

Starting point was Brent Maranzano's GitHub repo:
see: https://interactivebrokers.github.io/tws-api/index.html

Classes
    IBClient (EClient): Creates a socket to TWS or IBGateway, and handles
        sending commands to IB through the socket.
    IBWrapper (EWrapper): Hanldes the incoming data from IB. Many of these
        methods are callbacks from the request commands.
    IBApp (IBWrapper, IBClilent): This provides the main functionality. Many
        of the methods are over-rides of the IBWrapper commands to customize
        the functionality.
"""
import datetime
import time
import pytz
import numpy as np
import pandas as pd
import threading
import pickle
import copy
import collections

from abc import ABC, abstractmethod

from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from ibapi.common import BarData, HistogramDataList, TickerId, TickAttrib

from base import BaseApp
import helper

__CLIENT_ID = 1              # Reserve client id 0 for the main application

# The timezone specified at login to TWS. All historical data refer to this timezone.
TWS_TIMEZONE = 'US/Eastern'

# Status flags
_STATUS_NEW = 0
_STATUS_REQUEST_PLACED = 1
_STATUS_STREAM_CLOSED = 2

# IB TWS Field codes
LAST_TIMESTAMP = 45

# Default arguments
DEFAULT_USE_RTH = False

# Activate latency monitoring for tests of streaming data
MONITOR_LATENCY = False

class DataRequestError(Exception):
    """Exceptions generated during requesting historical market data.
    """
    def __init__(self, *args,**kwargs):
        super().__init__(*args,**kwargs)

        
class AbstractDataRequest(ABC):
    def __init__(self, parent_app, contract, is_snapshot, **kwargs):
        self.parent_app = parent_app
        self.is_snapshot = is_snapshot
        self.contract = contract        
        self.reset()

    def reset(self):
        self.reset_attributes()

        # Call method to split into valid subrequests if necessary
        sub_reqs = self._split_into_valid_subrequests()
        self.set_subrequests(sub_reqs)
    
    def reset_attributes(self):
        self.__subrequests = None
        self.__req_ids = [None]
        self.__is_request_complete = None
        self.__status = _STATUS_NEW
        self.initialize_data()
        
    @abstractmethod
    def initialize_data(self):
        pass
    
    @abstractmethod
    def append_data(self, new_data):
        pass
        
    @abstractmethod        
    def _request_data(self):
        pass

    @abstractmethod        
    def get_data(self):
        pass

    def _cancelStreamingSubscription(self):
        pass
    
    def is_valid_request(self):
        is_valid, msg = True, ""
        return is_valid, msg
    
    def is_request_complete(self):
        if self.__is_request_complete is None:
            req_ids = self.get_req_ids()
            self.__is_request_complete = all([self.parent_app.is_request_complete(req_id) \
                                                  for req_id in req_ids])
        return self.__is_request_complete
        
    def to_string(self):
        return pickle.dumps(self, protocol=4)

    def get_req_ids(self):
        if all([req_id is None for req_id in self.__req_ids]):
            if len(self.get_subrequests()) > 1:
                self.__req_ids = [s.get_req_ids()[0] for s in self.get_subrequests()]
        return self.__req_ids
        
    def place_request(self):
        if self.get_status() != _STATUS_REQUEST_PLACED:
            self.set_status(_STATUS_REQUEST_PLACED)
            if len(self.get_subrequests()) > 1:
                [reqObj.place_request() for reqObj in self.get_subrequests()]
            else:
                is_valid, msg = self.is_valid_request()
                if not is_valid:
                    raise DataRequestError(msg)

                req_id = self.parent_app._get_next_req_id()
                self.__req_ids = [req_id]
                self._register_request_with_parent()
                self._request_data(req_id)
    
    def _register_request_with_parent(self):
        req_id = self.get_req_ids()[0]
        self.parent_app.register_request(self)
        if not self.is_snapshot:
            self.parent_app._register_open_stream(req_id, self)

    def close_stream(self):
        assert not self.is_snapshot, 'Cannot close a non-streaming request.'
        self.set_status(_STATUS_STREAM_CLOSED)
        if len(self.get_subrequests()) > 1:
            [reqObj._deregister_request_with_parent() for reqObj in self.get_subrequests()]
        else:
            req_id = self.get_req_ids()[0]
            self.parent_app._deregister_open_stream(req_id)
            self._cancelStreamingSubscription()
            
    def copy(self):
        return copy.copy(self)
        
    def set_subrequests(self, sub_vals):
        self.__subrequests = sub_vals
        
    def get_subrequests(self):
        return self.__subrequests    
        
    def set_status(self, status):
        self.__status = status
        
    def get_status(self):
        return self.__status    
            
    def _split_into_valid_subrequests(self):
        """Split a request that is too large to be processed by IB.
           The default version (implemented here) is to not split any requests.
           Subclasses may need to override this with more sophisticated logic.
           
           Returns:
           split_request_objects (list): a list of valid request objects, which can
                   be combined to provide the data implicit in the original object.
        """
        if self.get_subrequests() is None:
            self.set_subrequests([self])
        return self.get_subrequests()


class MarketDataRequest(AbstractDataRequest):
    def __init__(self, parent_app, contract, is_snapshot, fields=""):
        super().__init__(parent_app, contract, is_snapshot)
        self.fields = fields
    
    # abstractmethod        
    def initialize_data(self):
        self.__market_data = dict()
        
    # abstractmethod
    def append_data(self, new_data):      
        self.__market_data.update(new_data)
        
    # abstractmethod                
    def _request_data(self, req_id):
        self.parent_app.reqMktData(
                                    reqId=req_id,
                                    contract=self.contract, 
                                    genericTickList=self.fields, 
                                    snapshot=self.is_snapshot,
                                    regulatorySnapshot=False,
                                    mktDataOptions=[]
                                   )                
    # abstractmethod
    def get_data(self):
        assert len(self.get_req_ids()) == 1, 'Market Data Requests should not have to be split.'
        return self.__market_data
        
    def _cancelStreamingSubscription(self):
        for req_id in self.get_req_ids():
            self.parent_app.cancelMktData(req_id)
            

class HistoricalDataRequest(AbstractDataRequest):
    def __init__(self, parent_app, contract, is_snapshot, frequency="", 
                                 start="", end="",
                                 duration="", use_rth=DEFAULT_USE_RTH, data_type='TRADES'):
        self.start = start
        self.end = end
        self.duration = duration             # e.g., 1s, 1M (1 minute), 1d, 1h, etc.
        self.frequency = frequency           # e.g., 1s, 1M (1 minute), 1d, 1h, etc.        
        self.use_rth = use_rth               # True/False - only return regular trading hours
        self.data_type = data_type           # TRADES, ASK, BID, ASK_BID, etc.
        super().__init__(parent_app, contract, is_snapshot)
        
    # abstractmethod
    def initialize_data(self):
        self.__market_data = []

    # abstractmethod
    def append_data(self, new_data):
        self.__market_data.append(new_data)
        
    def update_data(self, new_data):
        """Only works for single request objects, and is used for handling streaming updates.
           If the new row has the same date as the previously received row, then replace it.
           Otherwise, just append the new data as normal.
       """
        if self.__market_data and new_data['date'] == self.__market_data[-1]['date']:
            self.__market_data[-1] = new_data
        else:
            self.__market_data.append(new_data)
        
    # abstractmethod
    def _request_data(self, req_id):
        self.parent_app.reqHistoricalData(
                                           req_id,
                                           contract=self.contract, 
                                           endDateTime=self.end, 
                                           durationStr=self.durationStr(), 
                                           barSizeSetting=self.barSizeSetting(),
                                           whatToShow=self.data_type, 
                                           useRTH=self.use_rth, 
                                           formatDate=1,  # 1 corresponds to string format
                                           keepUpToDate=self.keepUpToDate(),
                                           chartOptions=[]
                                        )
    # abstractmethod
    def get_data(self):
        if len(self.get_req_ids()) == 1:
            return [self.__market_data]
        else:
            return [s.get_data()[0] for s in self.get_subrequests()]
        
    def get_dataframe(self):
        df = self._get_dataframe_from_raw_data()
        
        # Remove observations outside of the range
        est_datetimes = pd.DatetimeIndex(df.date).to_pydatetime()        
        if self.get_start_tws() is not None and self.get_end_tws() is not None:
            start_time = self.get_start_tws().replace(tzinfo=None)
            end_time = self.get_end_tws().replace(tzinfo=None)
            df = df.iloc[(start_time <= est_datetimes) & (est_datetimes <= end_time)]
        
        # Add an EST timestamp index
        est_timestamps = [dt.timestamp() for dt in est_datetimes]
        df.index = pd.Index(est_timestamps, name='est_timestamp')
        return df
    
    def is_valid_request(self):
        is_valid, msg = True, ""        
        if not self.is_snapshot:
            if self.end:
                is_valid = False
                msg = 'End date cannot be specified for streaming historical data requests.'
            #elif 5 > helper.TimeHelper(self.frequency, 'frequency').total_seconds():
                #is_valid = False
                #msg = 'Bar frequency for streaming historical data requests must be >= 5 seconds.'

        return is_valid, msg
    
    def _split_into_valid_subrequests(self):
        # Find the timedelta between start and end dates
        if self.get_subrequests() is None:
            start_tws = self.get_start_tws()
            end_tws = self.get_end_tws()
            if start_tws is None:
                # If 'start' is not specified, then we just use 'duration' and 'end'
                print('WARNING: this request may be invalid. Need to add tests that duration is valid.')
                return [self]
            else:
                delta = end_tws - start_tws
                # Split the period into multiple valid periods if necessary
                valid_periods = self._split_into_valid_periods(start_tws, end_tws)
                if len(valid_periods) == 1:
                    return [self]
                else:
                    requestObjList = []
                    for period in valid_periods:
                        period_start, period_end = period
                        requestObj = self.copy()
                        requestObj.reset_attributes()
                        requestObj.set_subrequests([requestObj])
                        requestObj.start = helper.convert_datetime_to_tws_date(period_start, TWS_TIMEZONE)
                        requestObj.end = helper.convert_datetime_to_tws_date(period_end, TWS_TIMEZONE)
                        requestObj.duration = ""
                        requestObjList.append(requestObj)
                    return requestObjList
    
    def keepUpToDate(self):
        return not self.is_snapshot
    
    def durationStr(self):
        if self.start and self.duration:
            raise ValueError('Duration and start cannot both be specified.')        
        elif self.duration:
            return helper.TimeHelper(self.duration, time_type='frequency').to_tws_durationStr()
        elif self.start:
            # Get a TimeHelper object corresponding to the interval btwn start/end dates
            start_tws, end_tws = self.get_start_tws(), self.get_end_tws()
            delta = end_tws - start_tws
            return helper.TimeHelper.from_timedelta(delta).get_min_tws_duration()
        else:
            return ""

    def barSizeSetting(self):
        if self.frequency:
            return helper.TimeHelper(self.frequency, time_type='frequency').to_tws_barSizeSetting()
        else:
            return ""

    def get_start_tws(self):
        if self.start:
            return helper.convert_tws_date_to_datetime(self.start, TWS_TIMEZONE)
        else:
            return None
            
    def get_end_tws(self):
        if not self.end:
            end_utc = pytz.utc.localize(datetime.datetime.utcnow())
            tws_tzone = pytz.timezone(TWS_TIMEZONE)
            return end_utc.astimezone(tws_tzone)
        else:
            return helper.convert_tws_date_to_datetime(self.end, TWS_TIMEZONE)
        
    def _cancelStreamingSubscription(self):
        for req_id in self.get_req_ids():
            self.parent_app.cancelHistoricalData(req_id)

    def _get_period_end(self, _start, _delta):
        if _delta.total_seconds() >= (3600 * 24):
            # For daily frequency, TWS defines the days to begin and end at 18:00
            if _start.hour < 18:
                new_date = datetime.datetime.combine(_start.date(), datetime.time(18,0))
            else:
                next_date = _start.date() + _delta
                new_date = datetime.datetime.combine(next_date, datetime.time(18,0))
            # Keep the previous time zone information
            return _start.tzinfo.localize(new_date)
        else:
            return _start + _delta

    def _is_duration_daily_frequency_or_lower(self, _delta):
        th = helper.TimeHelper.from_timedelta(_delta)
        dur = helper.TimeHelper(th.get_min_tws_duration(), 'duration')
        return dur.total_seconds() / dur.n >= 24 * 3600
        
    def _split_into_valid_periods(self, start_tws, end_tws):
        bar_freq = helper.TimeHelper(self.frequency, time_type='frequency')
        delta = end_tws - start_tws

        if bar_freq.units == 'days':
            # TWS convention seems to be that days begin and end at 18:00 EST
            start_tws = datetime.datetime.combine(start_tws.date() - datetime.timedelta(days=1), datetime.time(18,0))
            end_tws = datetime.datetime.combine(end_tws.date(), datetime.time(18,0))

        assert delta.total_seconds() > 0, 'Start time must precede end time.'
        max_delta = bar_freq.get_max_tws_duration_timedelta()
        period_start = start_tws
        periods = []
        while self._get_period_end(period_start, max_delta) < end_tws:
            period_end = min(end_tws, self._get_period_end(period_start, max_delta))
            periods.append((period_start, period_end))
            period_start = period_end
        if period_start < end_tws:
            periods.append((period_start, end_tws))
        return periods
            
    def _get_dataframe_from_raw_data(self):
        """ Turn the requested data into a dataframe.
        """
        df = pd.DataFrame()
        for d in self.get_data():
            df = pd.concat([df, pd.DataFrame(d)])

        df.sort_values('date', inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.drop_duplicates(inplace=True)

        if self.data_type in ['BID', 'ASK']:
            df = df.drop(['average', 'barCount', 'volume'], axis=1)

            idx = np.zeros((df.shape[0],), dtype=bool)
            idx[0] = True
            vals = df.drop('date', axis=1).to_numpy()
            # Only keep rows where something has changed
            idx[1:] = np.any(vals[1:] != vals[:-1], axis=1)
        elif self.data_type == 'TRADES':
            idx = np.zeros((df.shape[0],), dtype=bool)
            idx[0] = True
            # Only keep rows with a non-zero volume (e.g., a trade occurred in this bar)
            idx[1:] = (df.volume.values[1:] != 0)
        else:
            raise NotImplementedError('Not implemented for data type {}'.format(self.data_type))
        return df[idx]
    

class StreamingBarRequest(AbstractDataRequest):
    def __init__(self, parent_app, contract, is_snapshot, data_type="TRADES", use_rth=DEFAULT_USE_RTH, frequency='5s'):
        super().__init__(parent_app, contract, is_snapshot)
        self.frequency = frequency        
        self.data_type = data_type
        self.use_rth = use_rth

    # abstractmethod
    def initialize_data(self):
        self.__market_data = []

    # abstractmethod
    def append_data(self, new_data):
        self.__market_data.append(new_data)
        
    # abstractmethod                
    def _request_data(self, req_id):
        self.parent_app.reqRealTimeBars(
                                           req_id,
                                           contract=self.contract,
                                           barSize=self.barSizeInSeconds(),
                                           whatToShow=self.data_type, 
                                           useRTH=self.use_rth,
                                           realTimeBarsOptions=[]
                                        )
        
    # abstractmethod
    def get_data(self):
        assert len(self.get_req_ids()) == 1, 'Streaming Tick Data Requests should not have to be split.'
        return self.__market_data

    def get_dataframe(self):
        cols = ['time', 'price', 'size']
        prices = [{c: d[c] for c in cols} for d in self.get_data()]
        df = pd.DataFrame.from_dict(prices)
        df.rename({'time': 'local_time'}, inplace=True)
        df.set_index('local_time', inplace=True)
        return df
        
    def barSizeInSeconds(self):
        if self.frequency:
            return int(helper.TimeHelper(self.frequency, time_type='frequency').total_seconds())
        else:
            return -1
        
    def _cancelStreamingSubscription(self):
        for req_id in self.get_req_ids():
            self.parent_app.cancelRealTimeBars(req_id)


class StreamingTickDataRequest(AbstractDataRequest):
    def __init__(self, parent_app, contract, is_snapshot, data_type="Last", 
                                     number_of_ticks=0, ignore_size=True):
        super().__init__(parent_app, contract, is_snapshot)
        self.tickType = data_type
        self.numberOfTicks = number_of_ticks  
        self.ignoreSize = ignore_size     # Ignore ticks with just size updates (no price chg.)

    # abstractmethod
    def initialize_data(self):
        self.__market_data = []

    # abstractmethod
    def append_data(self, new_data):
        self.__market_data.append(new_data)
        
    # abstractmethod                
    def _request_data(self, req_id):
        self.parent_app.reqTickByTickData(
                                           req_id,
                                           contract=self.contract,
                                           tickType=self.tickType,            
                                           numberOfTicks=self.numberOfTicks,
                                           ignoreSize=self.ignoreSize
                                        )
        
    # abstractmethod
    def get_data(self):
        assert len(self.get_req_ids()) == 1, 'Streaming Tick Data Requests should not have to be split.'
        return self.__market_data
        
    def get_dataframe(self):        
        cols = ['time', 'price', 'size']
        prices = [{c: d[c] for c in cols} for d in self.get_data()]
        df = pd.DataFrame.from_dict(prices)
        df.set_index('time', inplace=True)
        return df

    def _cancelStreamingSubscription(self):
        for req_id in self.get_req_ids():
            self.parent_app.cancelTickByTickData(req_id)
            
    
class HistoricalTickDataRequest(AbstractDataRequest):
    def __init__(self, parent_app, contract, is_snapshot, start="", end="", use_rth=DEFAULT_USE_RTH,
                                 data_type="Last", number_of_ticks=1000, ignore_size=True):
        super().__init__(parent_app, contract, is_snapshot)
        self.startDateTime = start
        self.endDateTime = end        
        self.whatToShow = data_type,
        self.numberOfTicks = number_of_ticks
        self.useRTH = use_rth        
        self.ignoreSize = ignore_size   # Ignore ticks with just size updates (no price chg.)

    # abstractmethod
    def initialize_data(self):
        self.__market_data = []

    # abstractmethod
    def append_data(self, new_data):
        self.__market_data.append(new_data)
        
    # abstractmethod                
    def _request_data(self, req_id):
        self.parent_app.reqHistoricalTicks(
                                           req_id,
                                           contract=self.contract,
                                           startDateTime=self.startDateTime,
                                           endDateTime=self.endDateTime,
                                           numberOfTicks=self.numberOfTicks,
                                           whatToShow=self.whatToShow, 
                                           useRTH=self.use_rth,
                                           ignoreSize=self.ignoreSize,
                                           miscOptions=[]
                                        )
        
    # abstractmethod
    def get_data(self):
        assert len(self.get_req_ids()) == 1, 'Historical Tick Data Requests should not have to be split.'
        return self.__market_data
        
    def _cancelStreamingSubscription(self):
        for req_id in self.get_req_ids():
            self.parent_app.cancelTickByTickData(req_id)

    
class HeadTimeStampDataRequest(AbstractDataRequest):
    def __init__(self, parent_app, contract, is_snapshot=True, data_type='TRADES', use_rth=DEFAULT_USE_RTH):
        self.use_rth = use_rth               # True/False - only return regular trading hours
        self.data_type = data_type           # TRADES, ASK, BID, ASK_BID, etc.
        super().__init__(parent_app, contract, is_snapshot)
        
    # abstractmethod
    def initialize_data(self):
        self.__market_data = None
        
    # abstractmethod
    def append_data(self, new_data):
        self.__market_data = helper.convert_tws_date_to_datetime(new_data)

    # abstractmethod
    def _request_data(self, req_id):
        assert self.is_snapshot, 'HeadTimeStamp is only available for non-streaming data requests.'        
        self.parent_app.reqHeadTimeStamp(
                                           req_id,
                                           contract=self.contract, 
                                           whatToShow=self.data_type, 
                                           useRTH=self.use_rth, 
                                           formatDate=1  # 1 corresponds to string format
                                        )

    # abstractmethod
    def get_data(self):
        return self.__market_data
        

class PacingViolationManager(object):
    """ Manage how many market data requests for small bars (30 seconds or less, including ticks)
            can be submitted in order to avoid pacing violations from TWS.
    """
    
    TOTAL_REQUESTS_PER_TIME_UNIT = (60, 600)   # (number of requests allowed, time unit in seconds) 
    CONTRACT_REQUESTS_PER_TIME_UNIT = (6, 2)   # (number of requests allowed, time unit in seconds)
    SMALL_BAR_CUTOFF_SIZE = 30                 # In seconds
    
    _small_bar_market_data_requests = collections.deque(maxlen=TOTAL_REQUESTS_PER_TIME_UNIT[0])
    
    def __init__(self):
        super().__init__()

    def manage_request(self, requestObj):
        """ Manage the processing of the request to avoid pacing violations.
            For small bar requests, it might be necessary to sleep before continuing.
            """
        time.sleep(0.2)  # Always sleep for 0.2 seconds between requests to avoid overloading the server
        if self._is_small_bar_data_request(requestObj):
            # Only manage the request further if it is a 'small bar' (30 seconds or less)
            self.update_queue()
            self.check_requests_on_same_contract(requestObj)
            self.ensure_total_requests_not_exceeded()
            self._small_bar_market_data_requests.appendleft((time.time(), requestObj))
            if requestObj.data_type == 'BID_ASK':
                # BID_ASK requests count 2x, so we add an extra copy of the request to the queue
                self._small_bar_market_data_requests.appendleft((time.time(), requestObj))            

    def update_queue(self):
        while self. _small_bar_market_data_requests and \
        time.time() - self._small_bar_market_data_requests[-1][0] > self.TOTAL_REQUESTS_PER_TIME_UNIT[1]:
            self._small_bar_market_data_requests.pop()

    def check_requests_on_same_contract(self, requestObj):
        # Space out requests 
        N, T = self.CONTRACT_REQUESTS_PER_TIME_UNIT
        count = 0
        if 'data_type' in requestObj.__dict__:
            for past_request in self._small_bar_market_data_requests:
                t_past, reqObj_past = past_request
                if time.time() - t_past > T:
                    break
                elif not isinstance(reqObj_past, requestObj.__class__):
                    continue
                elif requestObj.contract.__dict__ == reqObj_past.contract.__dict__ \
                        and requestObj.data_type == reqObj_past.data_type:
                    count += 1
                    t_first = t_past

        if count >= N - 1:
            dt = (time.time() - t_first)
            print('Sleeping to avoid pacing violation from requests on same contract...')
            time.sleep(T - dt + 0.1)
                
    def ensure_total_requests_not_exceeded(self):
        N, T = self.TOTAL_REQUESTS_PER_TIME_UNIT        
        if N == len(self._small_bar_market_data_requests):
            dt = (time.time() - self._small_bar_market_data_requests[-1][0])
            if dt < T:
                t_sleep = T - dt + 0.1
                print('Sleeping {} seconds to avoid pacing violation on total requests.'.format(t_sleep))
                time.sleep(t_sleep)
                
    def clear_queue(self):
        self._small_bar_market_data_requests = collections.deque(maxlen=self.TOTAL_REQUESTS_PER_TIME_UNIT[0])
        
    def _is_small_bar_data_request(self, reqObj):
        return isinstance(reqObj, HistoricalDataRequest) and \
               self.SMALL_BAR_CUTOFF_SIZE >= helper.TimeHelper(reqObj.frequency, 'frequency').total_seconds()
        
        
class MarketDataApp(BaseApp):
    """Main program class. The TWS calls nextValidId after connection, so
    the method is over-ridden to provide an entry point into the program.

    class variables:
    saved_contracts (dict): keys are symbols, values are dictionaries of
        information to uniquely define a contract used for trading.
    """
    def __init__(self):
        super().__init__()
        self._pacing_manager = PacingViolationManager()
        self.__requests = dict()
        self.__requests_complete = dict()
        self.__open_streams = dict()
        self._histogram = None
        
    def register_request(self, requestObj):
        req_id = requestObj.get_req_ids()[0]
        self._pacing_manager.manage_request(requestObj)
        self.__requests[req_id] = requestObj
        if not requestObj.is_snapshot:
            self._register_open_stream(req_id, requestObj)
                
    def register_request_complete(self, req_id):
        self.__requests_complete[req_id] = datetime.datetime.now()

    def _create_data_request(self, cls, contractList, is_snapshot, **kwargs):
        # Make sure arguments are not included in kwargs
        kwargs.pop('contractList', None)
        kwargs.pop('is_snapshot', None)
        
        # Create a request object for eqch contract
        requestObjList = []
        for contract in contractList:
            request_obj = cls(self, contract, is_snapshot, **kwargs)
            requestObjList.append(request_obj)
        return requestObjList

    def create_market_data_request(self, contractList, is_snapshot, fields=""):
        _args = [MarketDataRequest, contractList, is_snapshot]
        _kwargs = dict(fields=fields)
        return self._create_data_request(*_args, **_kwargs)
    
    def create_historical_data_request(self, contractList, is_snapshot, frequency, use_rth=DEFAULT_USE_RTH, 
                                        data_type="TRADES", start="", end="", duration=""):
        _args = [HistoricalDataRequest, contractList, is_snapshot]        
        _kwargs = dict(frequency=frequency, start=start, end=end, duration=duration,
                                                use_rth=use_rth, data_type=data_type)
        return self._create_data_request(*_args, **_kwargs)
    
    def create_streaming_bar_data_request(self, contractList, frequency='5s', use_rth=DEFAULT_USE_RTH, data_type="TRADES"):
        is_snapshot = False        
        _args = [StreamingBarRequest, contractList, is_snapshot]        
        _kwargs = dict(frequency=frequency, use_rth=use_rth, data_type=data_type)
        return self._create_data_request(*_args, **_kwargs)    
    
    def create_streaming_tick_data_request(self, contractList, data_type="Last",
                                        number_of_ticks=1000, ignore_size=True):
        is_snapshot = False
        _args = [StreamingTickDataRequest, contractList, is_snapshot]
        _kwargs = dict(data_type=data_type, number_of_ticks=number_of_ticks, ignore_size=ignore_size)
        return self._create_data_request(*_args, **_kwargs)
    
    def create_historical_tick_data_request(self, contractList, use_rth=DEFAULT_USE_RTH, data_type="Last", 
                                       start="", end="", number_of_ticks=1000):
        is_snapshot = True
        _args = [HistoricalTickDataRequest, contractList, is_snapshot]        
        _kwargs = dict(data_type=data_type, start=start, end=end, use_rth=use_rth,
                                                number_of_ticks=number_of_ticks)
        return self._create_data_request(*_args, **_kwargs)

    def create_first_date_request(self, contractList, use_rth=DEFAULT_USE_RTH, data_type='TRADES'):
        is_snapshot = True
        _args = [HeadTimeStampDataRequest, contractList, is_snapshot]        
        _kwargs = dict(use_rth=use_rth, data_type=data_type)
        return self._create_data_request(*_args, **_kwargs) 
        
    def get_histogram(self, contract, period="20d"):
        """Get histograms of the local symbols (the unique IB tickers).

        Arguments:
        contract (Contract): ibapi Contract object
        period (str): Number of days to collect data.

        Returns (?): Histograms of the symbols
        """
        self._histogram = None
        req_id = self._get_next_req_id()
        period_obj = helper.TimeHelper(period)
        tws_period_fmt = period_obj.durationStr()
        self.reqHistogramData(req_id, contract, False, tws_period_fmt)

        # Handle the case where no historical data is found
        if not p:
            return None
        
        histogram = pd.DataFrame(
            columns=["price", "count"],
            data=[[float(p.price), int(p.count)] for p in self._histogram]
        )

        return histogram
            
    def get_open_streams(self):
        return self.__open_streams
    
    def is_request_complete(self, req_id):
        return req_id in self.__requests_complete

    def tickOptionComputation(self, tickerId: int, field: int, impliedVolatility: float, 
                              delta: float, optPrice: float, pvDividend: float,
                              gamma: float, vega: float, theta: float, undPrice: float):
        raise NotImplementedError('Option market data needs to be implemented.')
        
    def tickPrice(self, tickerId: int, field: int, price: float, attribs: TickAttrib):
        self._handle_market_data_callback(tickerId, field, price, attribs)

    def tickSize(self, tickerId: int, field: int, size: int):
        self._handle_market_data_callback(tickerId, field, size)
        
    def tickString(self, tickerId: int, field: int, value: str):
        self._handle_market_data_callback(tickerId, field, value)

    def tickSnapshotEnd(self, reqId: int):
        super().tickSnapshotEnd(reqId)
        self._handle_callback_end(reqId)
        
    def historicalData(self, reqId: int, bar: BarData):
        self._handle_historical_data_callback(reqId, bar, is_update=False)

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        self._handle_historical_data_callback(reqId, bar, is_update=True)        
        
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        self._handle_callback_end(reqId)

    def realtimeBar(self, reqId, date, _open, high, low, close, volume, WAP, count):
        super().realtimeBar(reqId, date, _open, high, low, close, volume, WAP, count)
        self._handle_realtimeBar_callback(reqId, date, _open, high, low, close, volume, WAP, count)
        
    def historicalTicks(self, reqId: int, ticks, done: bool):
        self._handle_historical_tick_data_callback(reqId, ticks, done)
        
    def historicalTicksBidAsk(self, reqId: int, ticks, done: bool):
        self._handle_historical_tick_data_callback(reqId, ticks, done)
        
    def historicalTicksLast(self, reqId: int, ticks, done: bool):
        self._handle_historical_tick_data_callback(reqId, ticks, done)
        
    def tickByTickAllLast(self, reqId, tickType, _time, price, size, 
                          tickAttribLast, exchange, specialConditions):
        self._handle_tickByTickAllLast_callback(reqId, tickType, 
               _time, price, size, tickAttribLast, exchange, specialConditions)

    def tickByTickBidAsk(self, reqId, _time, bidPrice, askPrice, 
                                         bidSize, askSize, tickAttribBidAsk):
        self._handle_tickByTickBidAsk_callback(reqId, _time, bidPrice, askPrice,
                                         bidSize, askSize, tickAttribBidAsk)
        
    def tickByTickMidPoint(self, reqId, _time, midPoint):
        self._handle_tickByTickMidPoint_callback(reqId, _time, midPoint)
                
    def headTimestamp(self, reqId: int, timestamp: str):
        self._handle_headtimestamp_data_callback(reqId, timestamp)
        self._handle_callback_end(reqId)
        self.cancelHeadTimeStamp(reqId)
        
    def histogramData(self, reqId: int, items: HistogramDataList):
        """EWrapper method called from reqHistogramData.
        http://interactivebrokers.github.io/tws-api/histograms.html
        """
        self._histogram = items
    
    def _register_open_stream(self, req_id, requestObj):
        self.__open_streams[req_id] = requestObj
        
    def _deregister_open_stream(self, req_id):
        del self.__open_streams[req_id]
    
    def _get_request_object_from_id(self, req_id):
        return self.__requests[req_id]

    def _handle_callback_end(self, req_id, *args):
        self.register_request_complete(req_id)
            
    def _handle_market_data_callback(self, req_id, field, val, attribs=None):
        reqObj = self._get_request_object_from_id(req_id)
        field_name = TickTypeEnum.to_str(field)
        if field == LAST_TIMESTAMP:
            val = int(val)
        reqObj.append_data({field_name: val})
    
    def _handle_historical_data_callback(self, req_id, bar, is_update):
        reqObj = self._get_request_object_from_id(req_id)
        data = bar.__dict__
        if is_update:
            if MONITOR_LATENCY:
                data['time_received'] = datetime.datetime.now()
            reqObj.update_data(data)
        else:
            reqObj.append_data(data)
        
    def _handle_realtimeBar_callback(self, req_id, date, _open, high, low, close, volume, WAP, count):
        reqObj = self._get_request_object_from_id(req_id)        
        bar = dict(date=date, open=_open, high=high, low=low, close=close, volume=volume,
                   average=WAP, barCount=count)
        if MONITOR_LATENCY:
            bar['latency'] = datetime.datetime.now().timestamp() - date
        reqObj.append_data(bar)
        
    def _handle_historical_tick_data_callback(self, req_id, ticks, done):
        reqObj = self._get_request_object_from_id(req_id)
        reqObj.append_data(ticks.__dict__)
        if done:
            self.register_request_complete(req_id)

    def _handle_tickByTickAllLast_callback(self, req_id, tickType, _time, 
                                           price, size, tickAttribLast, exchange, specialConditions):
        reqObj = self._get_request_object_from_id(req_id)
        data = dict(time=_time, price=price, size=size, tickAttribLast=tickAttribLast, 
                    exchange=exchange, specialConditions=specialConditions)
        if MONITOR_LATENCY:
            data['latency'] = datetime.datetime.now().timestamp() - _time
        reqObj.append_data(data)
        
    def _handle_tickByTickBidAsk_callback(self, req_id, _time, bidPrice, askPrice,
                                          bidSize, askSize, tickAttribBidAsk):
        reqObj = self._get_request_object_from_id(req_id)
        data = dict(time=_time, bidPrice=biedPrice, askPrice=askPrice, bidSize=bidSize, 
                    askSize=askSize, tickAttribBidAsk=tickAttribBidAsk)
        if MONITOR_LATENCY:
            data['latency'] = datetime.datetime.now().timestamp() - _time
        reqObj.append_data(data)
        
    def _handle_tickByTickMidPoint_callback(self, req_id, _time, midPoint):
        reqObj = self._get_request_object_from_id(req_id)
        data = dict(time=_time, midPoint=midPoint)
        if MONITOR_LATENCY:
            data['latency'] = datetime.datetime.now().timestamp() - _time
        reqObj.append_data(data)
        
    def _handle_headtimestamp_data_callback(self, req_id, timestamp):
        reqObj = self._get_request_object_from_id(req_id)
        reqObj.append_data(timestamp)
        
        
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
            __app = MarketDataApp()
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