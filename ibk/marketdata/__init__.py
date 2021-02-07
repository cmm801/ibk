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
    is_snapshot = False
    _args = [ibk.marketdata.datarequest.StreamingBarRequest, contract, is_snapshot]
    _kwargs = dict(frequency=frequency, use_rth=use_rth, data_type=data_type)
    return _create_data_request(*_args, **_kwargs)

def create_streaming_tick_data_request(contract, data_type="Last",
                                    number_of_ticks=1000, ignore_size=True):
    is_snapshot = False
    _args = [ibk.marketdata.datarequest.StreamingTickDataRequest, contract, is_snapshot]
    _kwargs = dict(data_type=data_type, number_of_ticks=number_of_ticks, ignore_size=ignore_size)
    return _create_data_request(*_args, **_kwargs)

def create_historical_tick_data_request(contract, use_rth=DEFAULT_USE_RTH, data_type="TRADES",
                                   start="", end="", number_of_ticks=1000):
    is_snapshot = True
    _args = [ibk.marketdata.datarequest.HistoricalTickDataRequest, contract, is_snapshot]
    _kwargs = dict(data_type=data_type, start=start, end=end, use_rth=use_rth,
                                            number_of_ticks=number_of_ticks)
    return _create_data_request(*_args, **_kwargs)

def create_first_date_request(contract, use_rth=DEFAULT_USE_RTH, data_type='TRADES'):
    is_snapshot = True
    _args = [ibk.marketdata.datarequest.HeadTimeStampDataRequest, contract, is_snapshot]
    _kwargs = dict(use_rth=use_rth, data_type=data_type)
    return _create_data_request(*_args, **_kwargs)

def create_fundamental_data_request(contract, report_type, options=None):
    if report_type == "ratios":
        is_snapshot = False
        _args = [ibk.marketdata.datarequest.FundamentalMarketDataRequest, contract, is_snapshot]
        _kwargs = dict()
        return _create_data_request(*_args, **_kwargs)
    else:
        return ibk.marketdata.datarequest.FundamentalDataRequest(contract,
                is_snapshot=True, report_type=report_type, options=options)

def create_scanner_data_request(scanSubObj, options=None, filters=None):
    return ibk.marketdata.datarequest.ScannerDataRequest(request_manager, scanSubObj, is_snapshot=False,
                                                         options=options, filters=filters)

def create_scanner_params_request():
    dataObj = None
    is_snapshot = False
    return ibk.marketdata.datarequest.ScannerParametersDataRequest(request_manager, dataObj, is_snapshot)
