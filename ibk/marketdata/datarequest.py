from abc import ABC, abstractmethod
from collections import Iterable
import datetime
import copy
import ibapi.contract
import numpy as np
import pandas as pd
import pytz
import tempfile
import warnings
import xml.etree.ElementTree as ET

import ibk.helper
from ibk.constants import TIMEZONE_TWS
import ibk.marketdata.constants


# The number of rows that the market scanner returns by default
DEFAULT_NUMBER_OF_SCAN_ROWS = 50

# What bar size defines a 'small bar' (in seconds)
SMALL_BAR_CUTOFF_SIZE = 30

# Default maximum number of restart attempts when IB does not return any data
DEFAULT_MAX_RESTARTS = 2


class DataRequest(ABC):
    _internal_counter = [0]

    def __init__(self, request_manager, dataObj, is_snapshot, **kwargs):
        self.request_manager = request_manager
        self.is_snapshot = is_snapshot
        self.dataObj = dataObj

        # Set a unique identifier
        self.uniq_id = self._internal_counter[0]
        self._internal_counter[0] += 1

        # Set additional internal variables
        self._status = ibk.marketdata.constants.STATUS_REQUEST_NEW
        self.reset()

    def reset(self):
        """ Reset a request to its initial state.
        
            Active or Queued requests must first be cancelled before
            one can call reset(). Otherwise, an exception will be raised
            for Active/Queued requests on which reset() is called.
        """
        if self.status not in (ibk.marketdata.constants.STATUS_REQUEST_NEW,
                           ibk.marketdata.constants.STATUS_REQUEST_COMPLETE,
                           ibk.marketdata.constants.STATUS_REQUEST_CANCELLED):
            raise ValueError('Active or Queued requests must be cancelled before they can be reset.' \
                           + f'This request has status "{self.status}."')
        else:
            self._status = ibk.marketdata.constants.STATUS_REQUEST_NEW
            self.request_manager._deregister_request(self)
            self.req_id = None
            self._initialize_data()
            self.n_restarts = 0
            self.max_restarts = DEFAULT_MAX_RESTARTS

    def place_request(self, priority=0):
        """ Place a request with the RequestManager.
        
            Arguments:
                priority: (float) indicates the relative priority with which the request
                will be processed, compared to other requests in the queue. The requests
                with the lowest priority are processed first.
        """
        if self.status != ibk.marketdata.constants.STATUS_REQUEST_NEW:
            raise ValueError(f'Only new requests can be placed. This request has status "{self.status}."')
        else:
            self.request_manager.place_request(self, priority=priority)

    def is_active(self):
        """ Check whether a request is still active. """
        return self.status not in (ibk.marketdata.constants.STATUS_REQUEST_NEW,
                                   ibk.marketdata.constants.STATUS_REQUEST_COMPLETE,
                                   ibk.marketdata.constants.STATUS_REQUEST_CANCELLED)

    def cancel_request(self):
        """ Cancel a request that has been placed with IB.
        """
        if not self.is_active():
            raise ValueError(f'Only active requests can be cancelled. This request has status "{self.status}."')
        else:
            self.request_manager.cancel_request(self)

    @property
    def status(self):
        return self._status
    
    @status.setter
    def status(self, s):
        if self._status != s:
            self._status = s
            self.request_manager.update_status(self.uniq_id)

    def is_valid_request(self):
        is_valid, msg = True, ""
        return is_valid, msg

    def copy(self):
        return copy.copy(self)

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def has_data(self):
        """ Returns True/False depending on whether IB has returned some results. """
        pass

    @abstractmethod
    def restriction_class(self):
        pass

    @abstractmethod
    def _initialize_data(self):
        pass

    @abstractmethod
    def _append_data(self, new_data):
        pass

    def _place_request_with_ib(self, app):
        """ Place the request with IB. """
        self.status = ibk.marketdata.constants.STATUS_REQUEST_SENT_TO_IB
        self._place_request_with_ib_core(app)

    def _cancel_request_with_ib(self, app):
        """ Cancel the request with IB. """
        self.status = ibk.marketdata.constants.STATUS_REQUEST_CANCELLED
        self._cancel_request_with_ib_core(app)

    @abstractmethod
    def _place_request_with_ib_core(self, app):
        pass

    def _cancel_request_with_ib_core(self, app):
        pass

    @property
    def is_small_bar(self):
        if not hasattr(self, 'frequency'):
            return False
        else:
            bar_size = ibk.helper.TimeHelper(self.frequency, 'frequency').total_seconds()
            return bar_size <= SMALL_BAR_CUTOFF_SIZE

    def _get_restrictions_on_historical_requests(self):
        """ Used by some subclasses to get restrictions on high-frequency historical requests.
        """
        # Restriction on number of simultaneous historical constraints
        res = (ibk.marketdata.constants.RESTRICTION_CLASS_SIMUL_HIST,)
        
        # Constraint on maximum number of simultaneous market data lines (aka streams)
        if not self.is_snapshot:
            res = res + (ibk.marketdata.constants.RESTRICTION_CLASS_SIMUL_STREAMS,)

        # Additional constraints for high frequency data requests
        bar_size = ibk.helper.TimeHelper(self.frequency, 'frequency').total_seconds()
        if self.is_small_bar:
            res = res + (ibk.marketdata.constants.RESTRICTION_CLASS_HF_HIST_IDENTICAL,
                         ibk.marketdata.constants.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW,
                         ibk.marketdata.constants.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW,)
        return res

    def _get_restrictions_on_historical_tick_requests(self):
        """ Used by some subclasses to get restrictions on high-frequency historical requests.
        """
        # Restriction on number of simultaneous historical constraints
        res = (ibk.marketdata.constants.RESTRICTION_CLASS_SIMUL_HIST,
               ibk.marketdata.constants.RESTRICTION_CLASS_HF_HIST_IDENTICAL,
               ibk.marketdata.constants.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW,
               ibk.marketdata.constants.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW)
        
        # Constraint on maximum number of simultaneous market data lines (aka streams)
        if not self.is_snapshot:
            res = res + (ibk.marketdata.constants.RESTRICTION_CLASS_SIMUL_TICK_STREAMS,)

        return res

    def __lt__(self, other):
        return self.uniq_id < other.uniq_id

    def __le__(self, other):
        return self.uniq_id <= other.uniq_id

    def __gt__(self, other):
        return self.uniq_id > other.uniq_id

    def __ge__(self, other):
        return self.uniq_id >= other.uniq_id
    

class DataRequestForContract(DataRequest):
    """ Overload the DataRequest object to work with Contract objects.
    """
    @property
    def contract(self):
        return self.dataObj
    
    @contract.setter
    def contract(self, ct):
        if not isinstance(ct, ibapi.contract.Contract):
            raise ValueError(f'Expected a Contract object, but received a "{ct.__class__}".')
        else:
            self.dataObj = ct


class ScannerDataRequest(DataRequest):
    def __init__(self, request_manager, dataObj, is_snapshot, options=None, filters=None):
        super(ScannerDataRequest, self).__init__(request_manager, dataObj, is_snapshot)
        self.options = options
        self.filters = filters

    @property
    def scanSubObj(self):
        return self.dataObj
    
    @scanSubObj.setter
    def scanSubObj(self, scan_obj):
        self.dataObj = scan_obj

    @property
    def n_rows(self):
        if self.scanSubObj.numberOfRows == -1:
            return DEFAULT_NUMBER_OF_SCAN_ROWS
        else:
            return self.scanSubObj.numberOfRows

    # abstractmethod
    def _initialize_data(self):
        """ Create a list with one element for each ranked instrument.
        """
        self._market_data = [{} for _ in range(self.n_rows)]

    # abstractmethod
    def has_data(self):
        """ Returns True/False if IB has returned some data. """
        return any([len(x) for x in self._market_data])
    
    # abstractmethod
    def _append_data(self, new_data):
        self._market_data[new_data['rank']] = new_data

    # abstractmethod
    def _place_request_with_ib_core(self, app):
        """ Place a request to start the Scanner. """
        # Make the scanner subscription request
        app.reqScannerSubscription(reqId=self.req_id,
                                   subscription=self.scanSubObj,
                                   scannerSubscriptionOptions=self.options,
                                   scannerSubscriptionFilterOptions=self.filters)

    def _cancel_request_with_ib_core(self, app):
        """ Method to cancel the scanner subscription. """
        app.cancelScannerSubscription(self.req_id)

    # abstractmethod
    def get_data(self):
        return self._market_data

    # abstractmethod
    @property
    def restriction_class(self):
        return (ibk.marketdata.constants.RESTRICTION_CLASS_SIMUL_SCANNERS,)


class MarketDataRequest(DataRequestForContract):
    def __init__(self, request_manager, contract, is_snapshot, fields=""):
        super(MarketDataRequest, self).__init__(request_manager, contract, is_snapshot)
        self.fields = fields

    # abstractmethod
    def _initialize_data(self):
        self._market_data = dict()

    # abstractmethod
    def has_data(self):
        """ Returns True/False if IB has returned some data. """
        return len(self._market_data) > 0

    # abstractmethod
    def _append_data(self, new_data):
        # Save the data
        self._market_data.update(new_data)

    # abstractmethod
    def _place_request_with_ib_core(self, app):
        app.reqMktData(reqId=self.req_id,
                       contract=self.contract,
                       genericTickList=self.fields,
                       snapshot=self.is_snapshot,
                       regulatorySnapshot=False,
                       mktDataOptions=[])

    def _cancel_request_with_ib_core(self, app):
        app.cancelMktData(self.req_id)

    # abstractmethod
    def get_data(self):
        return self._market_data

    # abstractmethod
    @property
    def restriction_class(self):
        if not self.is_snapshot:
            return (ibk.marketdata.constants.RESTRICTION_CLASS_SIMUL_STREAMS,)
        else:
            return tuple()

    def __lt__(self, other):
        return self.uniq_id < other.uniq_id

    def __le__(self, other):
        return self.uniq_id <= other.uniq_id

    def __gt__(self, other):
        return self.uniq_id > other.uniq_id

    def __ge__(self, other):
        return self.uniq_id >= other.uniq_id


class FundamentalMarketDataRequest(MarketDataRequest):
    def __init__(self, request_manager, contract, is_snapshot):
        super(FundamentalMarketDataRequest, self).__init__(request_manager, contract, is_snapshot)
        self.fields = ibk.marketdata.constants.FUNDAMENTAL_TICK_DATA_CODE
            
    def get_data(self):
        raw_data = self._parse_fundamental_data()
        return pd.Series(raw_data, name=self.contract.localSymbol)

    def _parse_fundamental_data(self):
        """ Parse the fundamental data that is returned.
        """
        date_cols = ['LATESTADATE']
        string_cols = ['CURRENCY']

        data = dict()
        
        # Get the raw data that has been returned by IB
        raw_data = self._market_data.get('FUNDAMENTAL_RATIOS', None)
        if raw_data is not None:
            # Parse any fundamental data that has been returned
            for _item in raw_data.split(';'):
                if _item:
                    k, v = _item.split('=')
                    if k in string_cols:
                        data[k] = v
                    elif k in date_cols:
                        try:
                            data[k] = pd.Timestamp(v)
                        except:
                            data[k] = np.datetime64('NaT')
                    elif v == "-99999.99":
                        data[k] = np.nan
                    elif v == '':
                        data[k] = np.nan
                    else:
                        data[k] = float(v)

        # Return the parsed fundamental data
        return data
    
            
class FundamentalDataRequest(DataRequestForContract):
    def __init__(self, request_manager, contract, is_snapshot, report_type="", options=None):
        assert is_snapshot, 'Fundamental Data is not available as a streaming service.'
        super(FundamentalDataRequest, self).__init__(request_manager, contract, is_snapshot)
        self.report_type = report_type
        self.options = options

    # abstractmethod
    def _initialize_data(self):
        self._market_data = None

    # abstractmethod
    def has_data(self):
        """ Returns True/False if IB has returned some data. """
        return self._market_data is not None

    # abstractmethod
    def _append_data(self, new_data):
        assert self._market_data is None, 'Only expected a single update.'
        self._market_data = new_data

    # abstractmethod
    def _place_request_with_ib_core(self, app):
        app.reqFundamentalData(reqId=self.req_id,
                               contract=self.contract,
                               reportType=self.report_type,
                               fundamentalDataOptions=self.options)
    # abstractmethod
    def get_data(self):
        return self._market_data

    # abstractmethod
    @property
    def restriction_class(self):
        return tuple()


class HistoricalDataRequest(DataRequestForContract):
    def __init__(self, request_manager, contract, is_snapshot, frequency="",
                 start="", end="", duration="", use_rth=None, 
                 data_type='TRADES'):
        # Initialize some private variables
        self._start = self._end = None
        
        if use_rth is None:
            use_rth = ibk.marketdata.constants.DEFAULT_USE_RTH
        
        # Save the input variables
        self.start = start
        self.end = end
        self.duration = duration             # e.g., 1s, 1M (1 minute), 1d, 1h, etc.
        self.frequency = frequency           # e.g., 1s, 1M (1 minute), 1d, 1h, etc.
        
        # Define some variables used for requesting data from IB
        self.data_type = data_type           # TRADES, ASK, BID, ASK_BID, etc.
        self.useRTH = use_rth                # True/False - only return regular trading hours
        self.formatDate = 1                  # 1 corresponds to string format
        self.chartOptions = []               # Argument currently required but not supported by IB
        
        # Call the superclass contructor
        super(HistoricalDataRequest, self).__init__(request_manager, contract, is_snapshot)

    @property
    def start(self):
        return self._start
    
    @start.setter
    def start(self, d):
        if d is None or not d:
            self._start = ''
        else:
            dt = ibk.helper.convert_to_datetime(d, tz_name=TIMEZONE_TWS)
            self._start = dt.replace(tzinfo=None)

    @property
    def end(self):
        return self._end
    
    @end.setter
    def end(self, d):
        if d is None or not d:
            self._end = ''
        else:
            dt = ibk.helper.convert_to_datetime(d, tz_name=TIMEZONE_TWS)
            self._end = dt.replace(tzinfo=None)

    # abstractmethod
    def get_data(self):
        return self._market_data

    def is_valid_request(self):
        is_valid, msg = True, ""
        if not self.is_snapshot:
            if self.end:
                is_valid = False
                msg = 'End date cannot be specified for streaming historical data requests.'

            if 5 > ibk.helper.TimeHelper(self.frequency, 'frequency').total_seconds():
                is_valid = False
                msg = 'Bar frequency for streaming historical data requests must be >= 5 seconds.'

        return is_valid, msg

    # abstractmethod
    def _initialize_data(self):
        self._market_data = []

    # abstractmethod
    def has_data(self):
        """ Returns True/False if IB has returned some data. """
        return len(self._market_data) > 0

    # abstractmethod
    def _append_data(self, new_data):
        self._market_data.append(new_data)

    def _update_data(self, new_data):
        """Only works for single request objects, and is used for handling streaming updates.
           If the new row has the same date as the previously received row, then replace it.
           Otherwise, just append the new data as normal.
       """
        if self._market_data and new_data['date'] == self._market_data[-1]['date']:
            self._market_data[-1] = new_data
        else:
            self._market_data.append(new_data)

    # abstractmethod
    def _place_request_with_ib_core(self, app):
        app.reqHistoricalData(self.req_id,
                              contract=self.contract,
                              endDateTime=self.endDateTime,
                              durationStr=self.durationStr,
                              barSizeSetting=self.barSizeSetting,
                              whatToShow=self.whatToShow,
                              useRTH=self.useRTH,
                              formatDate=self.formatDate,
                              keepUpToDate=self.keepUpToDate,
                              chartOptions=self.chartOptions)

    def _cancel_request_with_ib_core(self, app):
        app.cancelHistoricalData(self.req_id)

    # abstractmethod
    @property
    def restriction_class(self):
        return self._get_restrictions_on_historical_requests()

    @property
    def endDateTime(self):
        if not self.end:
            return ''
        else:
            return ibk.helper.convert_datetime_to_tws_date(self.end)

    @property
    def keepUpToDate(self):
        return not self.is_snapshot

    @property
    def durationStr(self):
        if self.start and self.duration:
            raise ValueError('Duration and start cannot both be specified.')
        elif self.duration:
            return ibk.helper.TimeHelper(self.duration, time_type='frequency').to_tws_durationStr()
        elif self.start:
            # Get a TimeHelper object corresponding to the interval btwn start/end dates
            if self.end == '':
                delta = datetime.datetime.now() - self.start
            else:
                delta = self.end - self.start

            return ibk.helper.TimeHelper.from_timedelta(delta).get_min_tws_duration()
        else:
            return ""

    @property
    def barSizeSetting(self):
        if self.frequency:
            return ibk.helper.TimeHelper(self.frequency, time_type='frequency').to_tws_barSizeSetting()
        else:
            return ""
        
    @property
    def whatToShow(self):
        return self.data_type

    def get_dataframe(self, timestamp=False, drop_empty_rows=True):
        """ Get a DataFrame with the time series data returned from the request.
        
            Arguments:
                timestamp: (bool) whether to return the index in timestamp format (True)
                    or as datetime objects (False).
                drop_empty_rows: (bool) whether to drop rows that have identical values
                    to the previous row (e.g. drop rows with Volume == 0)
        """
        raw_df = pd.DataFrame.from_dict(self.get_data())
        if 0 == len(raw_df):
            return pd.DataFrame()
        else:
            return _get_dataframe(raw_df, start=self.start, end=self.end, data_type=self.data_type,
                       timestamp=timestamp, drop_empty_rows=drop_empty_rows)


class HistoricalDataMultiRequest:
    def __init__(self, request_manager, contract, is_snapshot, frequency="",
                 start="", end="", duration="", use_rth=None, data_type='TRADES'):
        # Initialize some private variables
        self._start = self._end = None
        
        if use_rth is None:
            use_rth = ibk.marketdata.constants.DEFAULT_USE_RTH
        
        # Save the input variables
        self.request_manager = request_manager
        self.contract = contract
        self.is_snapshot = is_snapshot
        self.start = start
        self.end = end
        self.frequency = frequency           # e.g., 1s, 1M (1 minute), 1d, 1h, etc.        
        self.duration = duration             # e.g., 1s, 1M (1 minute), 1d, 1h, etc.
        self.data_type = data_type           # TRADES, ASK, BID, ASK_BID, etc.
        self.useRTH = use_rth                # True/False - only return regular trading hours
        self.formatDate = 1                  # 1 corresponds to string format
        self.chartOptions = []               # Argument currently required but not supported by IB

        self.subrequests = None
        self.subrequests = self._split_into_valid_subrequests()

    @property
    def start(self):
        return self._start
    
    @start.setter
    def start(self, d):
        if d is None or not d:
            self._start = ''
        else:
            dt = ibk.helper.convert_to_datetime(d, tz_name=TIMEZONE_TWS)
            self._start = dt.replace(tzinfo=None)

    @property
    def end(self):
        return self._end
    
    @end.setter
    def end(self, d):
        if d is None or not d:
            self._end = ''
        else:
            dt = ibk.helper.convert_to_datetime(d, tz_name=TIMEZONE_TWS)
            self._end = dt.replace(tzinfo=None)

    def is_active(self):
        return any([reqObj.is_active() for reqObj in self.subrequests])

    def place_request(self, priority=0):
        """ Place a request with the RequestManager.
        
            Arguments:
                priority: (float) indicates the relative priority with which the request
                will be processed, compared to other requests in the queue. The requests
                with the lowest priority are processed first.
        """
        for reqObj in self.subrequests:
            reqObj.place_request(priority=priority)

    def cancel_request(self):
        """ Cancel a request that has been placed with IB.
        """
        for reqObj in self.subrequests:
            reqObj.cancel_request()

    def get_data(self):
        return [s.get_data() for s in self.subrequests]

    def get_dataframe(self, timestamp=False, drop_empty_rows=True):
        """ Turn the requested data into a dataframe.
        """
        # Concat all of the individual data sets
        df_list = []
        for d in self.get_data():
            if len(d):
                df_list.append(pd.DataFrame.from_dict(d))
        if 0 == len(df_list):
            return pd.DataFrame()
        else:
            raw_df = pd.concat(df_list)
            
            # Construct the combined DataFrame object
            return _get_dataframe(raw_df, start=self.start, end=self.end, data_type=self.data_type,
                           timestamp=timestamp, drop_empty_rows=drop_empty_rows)

    def _get_period_end(self, _start, _delta):
        if _delta.total_seconds() >= (3600 * 24):
            # For daily frequency, TWS defines the days to begin and end at 18:00
            if _start.hour < 18:
                new_date = datetime.datetime.combine(_start.date(), datetime.time(18,0))
            else:
                next_date = _start.date() + _delta
                new_date = datetime.datetime.combine(next_date, datetime.time(18,0))

            return new_date
        else:
            return _start + _delta

    def _is_duration_daily_frequency_or_lower(self, _delta):
        th = ibk.helper.TimeHelper.from_timedelta(_delta)
        dur = ibk.helper.TimeHelper(th.get_min_tws_duration(), 'duration')
        return dur.total_seconds() / dur.n >= 24 * 3600

    def _split_into_valid_periods(self, start_tws, end_tws):
        bar_freq = ibk.helper.TimeHelper(self.frequency, time_type='frequency')
        
        delta = end_tws - start_tws
        if bar_freq.units == 'days':
            # TWS convention seems to be that days begin and end at 18:00 EST
            start_tws = datetime.datetime.combine(start_tws.date() - datetime.timedelta(days=1),
                                                  datetime.time(18,0))
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

    def _split_into_valid_subrequests(self):
        """ Split one historical request into multiple to comply with IB window constraints."""

        if self.end == '':
            end_tws = datetime.datetime.now()
        else:
            end_tws = self.end

        if self.start == '':
            if self.duration == '':
                raise ValueError('Either "start" or "duration" must be specified.')
            else:
                th = ibk.helper.TimeHelper(self.duration, time_type='frequency')
                start_tws = end_tws - th.to_timedelta()
        else:
            start_tws = self.start
            
        # Find the timedelta between start and end dates
        delta = end_tws - start_tws

        # Split the period into multiple valid periods if necessary
        valid_periods = self._split_into_valid_periods(start_tws, end_tws)
        if len(valid_periods) == 1:
            reqObj = HistoricalDataRequest(request_manager=self.request_manager,
                                           contract=self.contract, is_snapshot=self.is_snapshot,
                                           frequency=self.frequency, duration='',
                                           start=start_tws, end=self.end,
                                           use_rth=self.useRTH, data_type=self.data_type)

            return [reqObj]
        else:
            reqObjList = []
            for period in valid_periods:
                period_start, period_end = period

                reqObj = HistoricalDataRequest(request_manager=self.request_manager,
                                               contract=self.contract, is_snapshot=self.is_snapshot,
                                               frequency=self.frequency, duration='',
                                               start=period_start, end=period_end,
                                               use_rth=self.useRTH, data_type=self.data_type)
                reqObjList.append(reqObj)
            return reqObjList


class StreamingBarRequest(DataRequestForContract):
    def __init__(self, request_manager, contract, is_snapshot, data_type="TRADES", 
                 use_rth=None, frequency='5s'):
        assert not is_snapshot, 'Streaming requests must have is_snapshot == False.'
        super(StreamingBarRequest, self).__init__(request_manager, contract, is_snapshot)
        
        self.frequency = frequency
        self.data_type = data_type
        if use_rth is None:
            self.useRTH = ibk.marketdata.constants.DEFAULT_USE_RTH
        else:
            self.useRTH = use_rth

    # abstractmethod
    def _initialize_data(self):
        self._market_data = []

    # abstractmethod
    def has_data(self):
        """ Returns True/False if IB has returned some data. """
        return len(self._market_data) > 0

    # abstractmethod
    def _append_data(self, new_data):
        self._market_data.append(new_data)

    # abstractmethod
    def _place_request_with_ib_core(self, app):
        app.reqRealTimeBars(self.req_id,
                            contract=self.contract,
                            barSize=self.barSizeInSeconds(),
                            whatToShow=self.data_type,
                            useRTH=self.useRTH,
                            realTimeBarsOptions=[])

    def _cancel_request_with_ib_core(self, app):
        app.cancelRealTimeBars(self.req_id)

    # abstractmethod
    def get_data(self):
        return self._market_data

    # implement abstractmethod
    @property
    def restriction_class(self):
        return self._get_restrictions_on_historical_requests()

    def get_dataframe(self):
        cols = ['time', 'price', 'size']
        prices = [{c: d[c] for c in cols} for d in self.get_data()]
        df = pd.DataFrame.from_dict(prices)
        df.rename({'time': 'local_time'}, inplace=True)
        df.set_index('local_time', inplace=True)
        return df

    def barSizeInSeconds(self):
        if self.frequency:
            return int(ibk.helper.TimeHelper(self.frequency, time_type='frequency').total_seconds())
        else:
            return -1


class StreamingTickDataRequest(DataRequestForContract):
    """ Create a streaming tick data request.
    
        Arguments:
            data_type: (str) allowed values are  "Last", "AllLast", "BidAsk" or "MidPoint"
    """
    def __init__(self, request_manager, contract, is_snapshot, data_type="Last",
                                     number_of_ticks=0, ignore_size=True):
        assert not is_snapshot, 'A Streaming tick request must have is_snapshot == False.'
        super(StreamingTickDataRequest, self).__init__(request_manager, contract, is_snapshot)
        self.tickType = data_type
        self.numberOfTicks = number_of_ticks
        self.ignoreSize = ignore_size     # Ignore ticks with just size updates (no price chg.)

    # abstractmethod
    def _initialize_data(self):
        self._market_data = []

    # abstractmethod
    def has_data(self):
        """ Returns True/False if IB has returned some data. """
        return len(self._market_data) > 0

    # abstractmethod
    def _append_data(self, new_data):
        self._market_data.append(new_data)

    # abstractmethod
    def _extend_data(self, new_data):
        self._market_data.extend(new_data)

    # abstractmethod
    def get_data(self):
        return self._market_data

    # abstractmethod
    def _place_request_with_ib_core(self, app):
        app.reqTickByTickData(self.req_id,
                              contract=self.contract,
                              tickType=self.tickType,
                              numberOfTicks=self.numberOfTicks,
                              ignoreSize=self.ignoreSize)

    def _cancel_request_with_ib_core(self, app):
        app.cancelTickByTickData(self.req_id)

    # abstractmethod
    @property
    def restriction_class(self):
        return (ibk.marketdata.constants.RESTRICTION_CLASS_SIMUL_TICK_STREAMS,
                ibk.marketdata.constants.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT,)

    @property
    def data_type(self):
        if self.tickType in ("Last", "AllLast"):
            return 'LAST'
        elif self.tickType == 'BidAsk':
            return 'BID_ASK'
        elif self.tickType == 'MidPoint':
            return 'MIDPOINT'
        else:
            raise ValueError(f'Unknown tick type: "{self.tickType}".')
        
    def get_dataframe(self):
        cols = ['time', 'price', 'size']
        prices = [{c: d[c] for c in cols} for d in self.get_data()]
        df = pd.DataFrame.from_dict(prices)
        df.set_index('time', inplace=True)
        return df


class HistoricalTickDataRequest(DataRequestForContract):
    """ Create a historical tick data request.
    
        Arguments:
            data_type: (str) allowed values are 'BID_ASK', 'MIDPOINT', 'TRADES'
    """
    def __init__(self, request_manager, contract, is_snapshot, start="", end="",
                 use_rth=None, data_type="TRADES", number_of_ticks=1000, ignore_size=True):
        super(HistoricalTickDataRequest, self).__init__(request_manager, contract, is_snapshot)
        
        # Initialize private variables
        self._start = self._end = None

        if use_rth is None:
            use_rth = ibk.marketdata.constants.DEFAULT_USE_RTH

        # Set additional input variables
        self.start = start
        self.end = end
        self.whatToShow = data_type
        self.numberOfTicks = number_of_ticks
        self.useRTH = use_rth
        self.ignoreSize = ignore_size   # Ignore ticks with just size updates (no price chg.)

    @property
    def start(self):
        return self._start
    
    @start.setter
    def start(self, d):
        if d is None or not d:
            self._start = ''
        else:
            dt = ibk.helper.convert_to_datetime(d)
            self._start = dt.replace(tzinfo=None)

    @property
    def end(self):
        return self._end
    
    @end.setter
    def end(self, d):
        if d is None or not d:
            self._end = ''
        else:
            dt = ibk.helper.convert_to_datetime(d)
            self._end = dt.replace(tzinfo=None)

    @property
    def startDateTime(self):
        if not self.start:
            return ''
        else:
            return ibk.helper.convert_datetime_to_tws_date(self.start)

    @property
    def endDateTime(self):
        if not self.end:
            return ''
        else:
            return ibk.helper.convert_datetime_to_tws_date(self.end)

    # abstractmethod
    def _initialize_data(self):
        self._market_data = []

    # abstractmethod
    def has_data(self):
        """ Returns True/False if IB has returned some data. """
        return len(self._market_data) > 0

    # abstractmethod
    def _append_data(self, new_data):
        self._market_data.append(new_data)

    # abstractmethod
    def _extend_data(self, new_data):
        self._market_data.extend(new_data)

    # abstractmethod
    def _place_request_with_ib_core(self, app):
        app.reqHistoricalTicks(self.req_id,
                               contract=self.contract,
                               startDateTime=self.startDateTime,
                               endDateTime=self.endDateTime,
                               numberOfTicks=self.numberOfTicks,
                               whatToShow=self.whatToShow,
                               useRth=self.useRTH,
                               ignoreSize=self.ignoreSize,
                               miscOptions=[])

    def _cancel_request_with_ib_core(self, app):
        app.cancelTickByTickData(self.req_id)

    # abstractmethod
    def get_data(self):
        return self._market_data

    @property
    def data_type(self):
        return self.whatToShow.upper()

    # abstractmethod
    @property
    def restriction_class(self):
        return self._get_restrictions_on_historical_tick_requests()

    def get_dataframe(self):
        cols = ['time', 'price', 'size']
        prices = [{c: d.__getattribute__(c) for c in cols} for d in self.get_data()]
        df = pd.DataFrame.from_dict(prices)
        df.set_index('time', inplace=True)
        return df


class HeadTimeStampDataRequest(DataRequestForContract):
    def __init__(self, request_manager, contract, is_snapshot=True,
                 data_type='TRADES', use_rth=None):
        if use_rth is None:
            self.useRTH = ibk.marketdata.constants.DEFAULT_USE_RTH
        else:
            self.useRTH = use_rth               # True/False - only return regular trading hours

        self.data_type = data_type           # TRADES, ASK, BID, ASK_BID, etc.
        super(HeadTimeStampDataRequest, self).__init__(request_manager, contract, is_snapshot)

    # abstractmethod
    def _initialize_data(self):
        self._market_data = None

    # abstractmethod
    def has_data(self):
        """ Returns True/False if IB has returned some data. """
        return self._market_data is not None

    # abstractmethod
    def _append_data(self, new_data):
        dt = ibk.helper.convert_datestr_to_datetime(new_data)
        self._market_data = dt.replace(tzinfo=None)

    # abstractmethod
    def _place_request_with_ib_core(self, app):
        assert self.is_snapshot, 'HeadTimeStamp is only available for non-streaming data requests.'
        app.reqHeadTimeStamp(self.req_id,
                             contract=self.contract,
                             whatToShow=self.data_type,
                             useRTH=self.useRTH,
                             formatDate=1)  # 1 corresponds to string format

    # abstractmethod
    def get_data(self):
        return self._market_data

    # abstractmethod
    @property
    def restriction_class(self):
        return tuple()


class ScannerParametersDataRequest(DataRequest):
    def __init__(self, request_manager, dataObj, is_snapshot=False):
        super(ScannerParametersDataRequest, self).__init__(request_manager, dataObj, is_snapshot)

    # abstractmethod
    def _initialize_data(self):
        self._xml_params = None
        self.data = None

    # abstractmethod
    def has_data(self):
        """ Returns True/False if IB has returned some data. """
        return self._xml_params is not None

    # abstractmethod
    def _append_data(self, new_data):
        pass # Nothing to append - data is saved on App

    # abstractmethod
    def _place_request_with_ib_core(self, app):
        app._xml_scanner_params_req_list.append(self)
        app.reqScannerParameters()

    # abstractmethod
    @property
    def restriction_class(self):
        return tuple()

    # abstractmethod
    def get_data(self):
        if self.data is None and self._xml_params is not None:
            # Save the XML to a text file so that ElementTree can access the data
            with tempfile.NamedTemporaryFile(mode='w', suffix='.xml') as tmp_file:
                tmp_file.writelines(self._xml_params)
                tmp_file.seek(0)

                # Use the ElementTree to read in the XML
                tree = ET.parse(tmp_file.name)

            # Parse the data into dict of dicts by going through branches
            root = tree.getroot()
            root_dict = {}
            for group in root:
                root_dict[group.tag] = {}
                for instrument in group:
                    if instrument.tag not in root_dict[group.tag]:
                        root_dict[group.tag][instrument.tag] = []

                    entry = {}
                    for child in instrument:
                        entry[child.tag] = child.text
                    root_dict[group.tag][instrument.tag].append(entry)

            # Save the information
            self.data = root_dict
            
        # Return the parsed data
        return self.data


########################################################################
# Define helper functions
########################################################################

def _restrict_to_start_end_dates(df, start, end, timestamp):
    """ Remove observations outside of the range. """
    tws_datetimes = pd.DatetimeIndex(df.index).to_pydatetime()
    if start is not None and end is not None:
        rows_to_keep = (start <= tws_datetimes) & (tws_datetimes <= end)
        df = df.iloc[rows_to_keep]
    return df

def _get_utc_timestamp_index(df):
    """ Construct a UTC timestamp index. """
    tws_tz = pytz.timezone(TIMEZONE_TWS)
    utc_tz = pytz.utc
    utc_datetimes = [tws_tz.localize(d).astimezone(utc_tz) for d in tws_datetimes]
    utc_timestamps = [d.timestamp() for d in utc_datetimes]
    return pd.Index(utc_timestamps, name='utc_timestamp')

def _drop_static_rows(df, data_type):
    """ Only keep rows where something has changed. """
    idx = np.zeros((df.shape[0],), dtype=bool)
    idx[0] = True

    if data_type in ['BID', 'ASK']:
        sub_df = df.drop(['average', 'barCount', 'volume'], axis=1)
        vals = sub_df.to_numpy()
        idx[1:] = np.any(vals[1:] != vals[:-1], axis=1)
    elif data_type == 'TRADES':
        # Only keep rows with a non-zero volume (e.g., a trade occurred in this bar)
        idx[1:] = (df.volume.values[1:] != 0)
    else:
        raise NotImplementedError('Not implemented for data type {}'.format(data_type))
    return df[idx]

def _get_dataframe(df_input, start, end, data_type, 
                   timestamp=False, drop_empty_rows=True):
    """ Turn the requested data into a dataframe.
    """    
    # Drop duplicate values and reset the index
    df = df_input.drop_duplicates()
    
    if not df.shape[0]:
        return df
    
    # Set the index
    if timestamp:
        #df.index = _get_utc_timestamp_index(df)
        raise NotImplementedError('Not implemented.')
    else:
        df.set_index('date', inplace=True)
        df.index = pd.DatetimeIndex(df.index)

    # Sort by the index
    df.sort_index(inplace=True)

    # Restrict the output data to be between the start/end dates
    df = _restrict_to_start_end_dates(df, start, end, timestamp)
    
    # Drop rows where nothing has changed (e.g. Volume = 0)
    if drop_empty_rows:
        df = _drop_static_rows(df, data_type)
    return df
    