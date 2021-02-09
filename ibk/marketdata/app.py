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
import numpy as np

from ibapi.contract import ContractDetails
from ibapi.ticktype import TickTypeEnum
from ibapi.common import BarData, TickAttrib

import ibk.base
import ibk.connect
import ibk.marketdata.constants
import ibk.marketdata.datarequest

# Activate latency monitoring for tests of streaming data
MONITOR_LATENCY = False


class MarketDataAppManager:
    """Class for managing a pool of market data connections.
    """
    # Keep track of the App connections
    _apps = {ibk.constants.PORT_PAPER : {},
             ibk.constants.PORT_PROD : {}}

    def __init__(self, host=None):
        self.host = host

    @property
    def port(self):
        if ibk.connect.active_port is None:
            raise ValueError('"active_port" must be set in ibk.connect.')
        else:
            return ibk.connect.active_port

    @property
    def apps(self):
        return self._apps[self.port]

    def register_app(self, app):
        """ Add new app(s) to make it available for placing/cancelling requests. """
        if isinstance(app, MarketDataApp):
            self._apps[app.port][app.uniq_id] = app
        elif isinstance(app, list):
            for a in app:
                self.register_app(a)
        else:
            raise ValueError(f'Unexpected input type "{app.__class__}" for argument "app".')

    def creeate_app(self):
        """ Create a new App instance. """
        app = MarketDataApp()
        app.connect(host=self.host, port=self.port)
        return app

    def get_app(self):
        """ Select an app instance in order to communicate with IB. """
        if len(self.apps) == 0:
            app = self.creeate_app()
            self.register_app(app)
        else:
            uniq_id = np.random.choice(list(self.apps.keys()))
            app = self.apps[uniq_id]
            if not app.isConnected():
                app.reconnect()

        return app

    def _get_app_from_uniq_id(self, uniq_id):
        if uniq_id not in self.apps is None:
            raise ValueError(f"An app with uniq_id {uniq_id} was not found.")
        else:
            return self.apps[uniq_id]


class MarketDataApp(ibk.base.BaseApp):
    """Connection to IB TWS that places data requests and handles callbacks.
    """
    # Store a map from request Id to DataRequest object
    requests = dict()

    # Used to retrieve scanner parameters in callback
    _xml_scanner_params_req_list = []

    def get_active_requests(self):
        """ Return a list of requests that are still active. """
        return list([reqObj for reqObj in self.requests.values if reqObj.is_active()]) 

    ##############################################################################
    # Private methods
    ##############################################################################

    def _get_request_object_from_req_id(self, req_id):
        if req_id not in self.requests is None:
            raise ValueError(f"The request object's request Id {req_id} was not found.")
        else:
            return self.requests[req_id]

    def _handle_callback_end(self, req_id, *args):
        reqObj = self._get_request_object_from_req_id(req_id)
        reqObj.status = ibk.marketdata.constants.STATUS_REQUEST_COMPLETE

    def _handle_market_data_callback(self, req_id, field, val, attribs=None):
        reqObj = self._get_request_object_from_req_id(req_id)
        field_name = TickTypeEnum.to_str(field)
        if field == ibk.marketdata.constants.LAST_TIMESTAMP:
            val = int(val)

        # Store the value
        reqObj._append_data({field_name: val})

        # If it is a fundamental data request, we can close the stream and request
        if field == ibk.marketdata.constants.FUNDAMENTAL_TICK_DATA_CODE \
                and isinstance(reqObj, ibk.marketdata.datarequest.FundamentalMarketDataRequest):
            # Close the stream by cancelling the request
            reqObj._cancel_request_with_ib(self)

            # Register the request as 'completed' instead of 'cancelled'
            reqObj.status = ibk.marketdata.constants.STATUS_REQUEST_COMPLETE

    def _handle_historical_data_callback(self, req_id, bar, is_update):
        reqObj = self._get_request_object_from_req_id(req_id)
        data = bar.__dict__
        if is_update:
            if MONITOR_LATENCY:
                data['time_received'] = datetime.datetime.now()
            reqObj._update_data(data)
        else:
            reqObj._append_data(data)

    def _handle_realtimeBar_callback(self, req_id, date, _open, high, low, close, volume, WAP, count):
        reqObj = self._get_request_object_from_req_id(req_id)
        bar = dict(date=date, open=_open, high=high, low=low, close=close, volume=volume,
                   average=WAP, barCount=count)
        if MONITOR_LATENCY:
            bar['latency'] = datetime.datetime.now().timestamp() - date
        reqObj._append_data(bar)

    def _handle_historical_tick_data_callback(self, req_id, ticks, done):
        reqObj = self._get_request_object_from_req_id(req_id)
        if ticks:
            reqObj._extend_data(ticks)
        if done:
            reqObj.status = ibk.marketdata.constants.STATUS_REQUEST_COMPLETE

    def _handle_tickByTickAllLast_callback(self, req_id, tickType, _time,
                                           price, size, tickAttribLast, exchange, specialConditions):
        reqObj = self._get_request_object_from_req_id(req_id)
        data = dict(time=_time, price=price, size=size, tickAttribLast=tickAttribLast,
                    exchange=exchange, specialConditions=specialConditions)
        if MONITOR_LATENCY:
            data['latency'] = datetime.datetime.now().timestamp() - _time
        reqObj._append_data(data)

    def _handle_tickByTickBidAsk_callback(self, req_id, _time, bidPrice, askPrice,
                                          bidSize, askSize, tickAttribBidAsk):
        reqObj = self._get_request_object_from_req_id(req_id)
        data = dict(time=_time, bidPrice=biedPrice, askPrice=askPrice, bidSize=bidSize,
                    askSize=askSize, tickAttribBidAsk=tickAttribBidAsk)
        if MONITOR_LATENCY:
            data['latency'] = datetime.datetime.now().timestamp() - _time
        reqObj._append_data(data)

    def _handle_tickByTickMidPoint_callback(self, req_id, _time, midPoint):
        reqObj = self._get_request_object_from_req_id(req_id)
        data = dict(time=_time, midPoint=midPoint)
        if MONITOR_LATENCY:
            data['latency'] = datetime.datetime.now().timestamp() - _time
        reqObj._append_data(data)

    def _handle_headtimestamp_data_callback(self, req_id, timestamp):
        reqObj = self._get_request_object_from_req_id(req_id)
        reqObj._append_data(timestamp)

    def _handle_scanner_subscription_data_callback(self, req_id, rank, 
                   contractDetails, distance, benchmark, projection, legsStr):
        reqObj = self._get_request_object_from_req_id(req_id)
        data = dict(rank=rank, contractDetails=contractDetails, distance=distance,
                    benchmark=benchmark, projection=projection, legsStr=legsStr)
        reqObj._append_data(data)

    def _handle_fundamental_data_callback(self, req_id, data):
        reqObj = self._get_request_object_from_req_id(req_id)
        reqObj._append_data(data)
        
    ##############################################################################
    # Methods for handling response from the client
    ##############################################################################

    def fundamentalData(self, reqId: int, data : str):
        self._handle_fundamental_data_callback(reqId, data)

    def scannerParameters(self, xmlParams):
        while len(self._xml_scanner_params_req_list):
            reqObj = self._xml_scanner_params_req_list.pop()
            reqObj._xml_params = xmlParams
            reqObj.status = ibk.marketdata.constants.STATUS_REQUEST_COMPLETE

    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails,
                    distance: str, benchmark: str, projection: str, legsStr: str):
        self._handle_scanner_subscription_data_callback(reqId, rank, contractDetails,
                    distance, benchmark, projection, legsStr)

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

        

# Define a global version of the market data manager
mktdata_manager = MarketDataAppManager()
        