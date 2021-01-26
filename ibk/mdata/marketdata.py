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

from ibapi.contract import ContractDetails
from ibapi.ticktype import TickTypeEnum
from ibapi.common import BarData, TickAttrib

import ibk.base
import ibk.requestmanager


# IB TWS Field codes
LAST_TIMESTAMP = 45

# Activate latency monitoring for tests of streaming data
MONITOR_LATENCY = False


class MarketDataApp(ibk.base.BaseApp):
    """Main program class. The TWS calls nextValidId after connection, so
    the method is over-ridden to provide an entry point into the program.

    class variables:
    saved_contracts (dict): keys are symbols, values are dictionaries of
        information to uniquely define a contract used for trading.
    """
    def __init__(self):
        super(MarketDataApp, self).__init__()
        self.request_manager = ibk.requestmanager.RequestManager()

    def get_open_streams(self):
        return self.request_manager.open_streams

    ##############################################################################
    # Private methods
    ##############################################################################

    def _get_request_object_from_id(self, req_id):
        return self.request_manager.requests[req_id]

    def _handle_callback_end(self, req_id, *args):
        reqObj = self._get_request_object_from_id(req_id)
        reqObj.status = ibk.requestmanager.STATUS_REQUEST_COMPLETE
        self.request_manager.register_request_complete(req_id)

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
        if ticks:
            reqObj.append_data(ticks)
        if done:
            reqObj.status = ibk.requestmanager.STATUS_REQUEST_COMPLETE
            self.request_manager.register_request_complete(req_id)

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

    def _handle_scanner_subscription_data_callback(self, req_id, rank, 
                   contractDetails, distance, benchmark, projection, legsStr):
        reqObj = self._get_request_object_from_id(req_id)
        data = dict(rank=rank, contractDetails=contractDetails, distance=distance,
                    benchmark=benchmark, projection=projection, legsStr=legsStr)
        reqObj.append_data(data)

    def _handle_fundamental_data_callback(self, req_id, data):
        reqObj = self._get_request_object_from_id(req_id)
        reqObj.append_data(data)
        
    ##############################################################################
    # Methods for handling response from the client
    ##############################################################################

    def fundamentalData(self, reqId: int, data : str):
        self._handle_fundamental_data_callback(reqId, data)
    
    def scannerParameters(self, xmlParams):
        self._xml_params = xmlParams

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
