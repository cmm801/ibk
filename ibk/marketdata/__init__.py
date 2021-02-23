""" marketdata is a package containing methods to retrieve market data from IB TWS.

    This package contains a number of class definitions which make interacting with
    the IB data requests easier. The data requests in this package also comply with
    IB restrictions on frequency and amount of requests in order to avoid throttling.
"""

import time

import ibk.marketdata.datarequest
import ibk.marketdata.restrictionmanager
from ibk.marketdata.app import MarketDataApp, MarketDataAppManager, mktdata_manager
from ibk.marketdata.requestmanager import GlobalRequestManager, request_manager
from ibk.marketdata.constants import DEFAULT_USE_RTH


def _create_data_request(cls, contract, is_snapshot, **kwargs):
    """ Private helper method that constructs data request instances. """
    # Make sure arguments are not included in kwargs
    kwargs.pop('contract', None)
    kwargs.pop('is_snapshot', None)

    # Create a request object for eqch contract
    reqObj = cls(request_manager, contract, is_snapshot, **kwargs)
    return reqObj

def create_market_data_request(contract, is_snapshot, fields=""):
    """ Create a MarketDataRequest object for getting  current market data.

        Arguments:
            contract: an ibapi Contract object
            is_snapshot: (bool) if False, then a stream will be opened with 
                IB that will continuously update the latest market price info.
                If True, then only the latest price info will be returned.
            fields: (str) additional tick data codes that are requested,
                in additional to the default data codes. A list of available
                additional tick data codes are available on the IB website.
    """
    _args = [ibk.marketdata.datarequest.MarketDataRequest, contract, is_snapshot]
    _kwargs = dict(fields=fields)
    return _create_data_request(*_args, **_kwargs)

def create_historical_data_request(contract, is_snapshot, frequency,
                                   use_rth=DEFAULT_USE_RTH, data_type="TRADES",
                                   start="", end="", duration=""):
    """ Create a HistoricalDataRequest object for getting historical data.
    """         
    _args = [ibk.marketdata.datarequest.HistoricalDataMultiRequest, contract, is_snapshot]
    _kwargs = dict(frequency=frequency, start=start, end=end, duration=duration,
                                            use_rth=use_rth, data_type=data_type)
    return _create_data_request(*_args, **_kwargs)

def create_streaming_bar_data_request(contract, frequency='5s', 
                                      use_rth=DEFAULT_USE_RTH, data_type="TRADES"):
    """ Create a data request object for getting streaming bar data. """
    is_snapshot = False
    _args = [ibk.marketdata.datarequest.StreamingBarRequest, contract, is_snapshot]
    _kwargs = dict(frequency=frequency, use_rth=use_rth, data_type=data_type)
    return _create_data_request(*_args, **_kwargs)

def create_streaming_tick_data_request(contract, data_type="Last",
                                    number_of_ticks=1000, ignore_size=True):
    """ Create a data request object for getting streaming tick data. """
    is_snapshot = False
    _args = [ibk.marketdata.datarequest.StreamingTickDataRequest, contract, is_snapshot]
    _kwargs = dict(data_type=data_type, number_of_ticks=number_of_ticks, ignore_size=ignore_size)
    return _create_data_request(*_args, **_kwargs)

def create_historical_tick_data_request(contract, use_rth=DEFAULT_USE_RTH, data_type="TRADES",
                                   start="", end="", number_of_ticks=1000):
    """ Create a data request object for getting historical tick data. """
    is_snapshot = True
    _args = [ibk.marketdata.datarequest.HistoricalTickDataRequest, contract, is_snapshot]
    _kwargs = dict(data_type=data_type, start=start, end=end, use_rth=use_rth,
                                            number_of_ticks=number_of_ticks)
    return _create_data_request(*_args, **_kwargs)

def create_fundamental_data_request(contract, report_type, options=None):
    """ Create a data request object for getting fundamental data. """
    if report_type == "ratios":
        is_snapshot = False
        _args = [ibk.marketdata.datarequest.FundamentalMarketDataRequest, contract, is_snapshot]
        _kwargs = dict()
        return _create_data_request(*_args, **_kwargs)
    else:
        return ibk.marketdata.datarequest.FundamentalDataRequest(contract,
                is_snapshot=True, report_type=report_type, options=options)

def create_scanner_data_request(scanSubObj, options=None, filters=None):
    """ Create a data request object for creating a scanner. """
    return ibk.marketdata.datarequest.ScannerDataRequest(request_manager, scanSubObj, is_snapshot=False,
                                                         options=options, filters=filters)

def create_scanner_params_request():
    """ Create a data request object for getting the parameters used in a market scanner request. """
    dataObj = None
    is_snapshot = False
    return ibk.marketdata.datarequest.ScannerParametersDataRequest(request_manager, dataObj, is_snapshot)

def create_first_date_request(contract):
    """ Create a data request object for getting the first available date of historical data. """
    is_snapshot = True
    _args = [ibk.marketdata.datarequest.HeadTimeStampDataRequest, contract, is_snapshot]
    return _create_data_request(*_args)

def get_first_date(contract, max_wait_time=10):
    """ Get the first date on which historical data is available. 
    """
    req = create_first_date_request(contract)
    req.place_request()

    t0 = time.time()
    while req.is_active() and time.time() - t0 < max_wait_time:
        time.sleep(0.1)

    if req.is_active():
        raise ValueError('No first date info available.')
    else:
        return req.get_data()

def get_scanner_params(max_wait_time=10):
    """ Get the parameters used for setting up a market scanner. 
    """
    req = create_scanner_params_request()
    req.place_request()

    t0 = time.time()
    while req.is_active() and time.time() - t0 < max_wait_time:
        time.sleep(0.1)

    if req.is_active():
        raise ValueError('Not able to retrieve scanner parameters.')
    else:
        return req.get_data()
