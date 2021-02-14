import datetime
import time
import queue
import threading
import collections
from abc import ABC, abstractmethod

import ibk.connect
import ibk.errors

import ibk.marketdata.constants as mdconst
import ibk.marketdata.datarequest
from ibk.marketdata.restrictionmanager import RestrictionManager
from ibk.marketdata.app import mktdata_manager

# Define names of Queues used for managing data requests
QUEUE_MONITORING = 'monitoring'
QUEUE_STREAM = 'stream'
QUEUE_GENERIC = 'generic'
QUEUE_SCANNER = 'scanner'
QUEUE_TICK_STREAM = 'tick_Stream'
QUEUE_HIST_SMALL_BAR = 'hist_small_bar'
QUEUE_HIST_LARGE_BAR = 'hist_large_bar'

QUEUE_TYPES = [QUEUE_MONITORING, QUEUE_STREAM, QUEUE_GENERIC, QUEUE_SCANNER,
               QUEUE_TICK_STREAM, QUEUE_HIST_SMALL_BAR, QUEUE_HIST_LARGE_BAR]

# Default timeout (in seconds) if IB does not provide a response to a request
DEFAULT_TIMEOUT = 30


# Create a class that will contain timestamps for any status changes
class RequestStatus:
    def __init__(self, obj):
        if not obj.status == mdconst.STATUS_REQUEST_NEW:
            raise ValueError('Expected new registered request to have status "new".')
        else:
            self.object = obj
            self.info = {k : None for k in mdconst.STATUS_REQUEST_OPTIONS}
            self.update()

    @property
    def status(self):
        return self.object.status

    def is_updated(self):
        return self.info[self.status] is not None

    def update(self):
        if self.is_updated():
            raise ValueError(f'Status {self.status} has already been set.')
        else:
            self.info[self.status] = time.time()


class GlobalRequestManager:
    # A map from unique Id to the DataRequest object
    requests = dict()
    
    # Requests being tracked to ensure restrictions are enforced
    restriction_manager = RestrictionManager()

    # Define request queues
    queues = {q : None for q in QUEUE_TYPES}

    def place_request(self, reqObj, priority=0):
        """ Place a request with IB. """
        self._register_new_request(reqObj)
        
        # Route the request to one of the request queues
        assigned_queue = self.get_assigned_queue(reqObj)
        assigned_queue.enqueue_request(reqObj, priority=priority)
        
    @property
    def monitoring_queue(self):
        return self.get_queue(QUEUE_MONITORING)
            
    def get_queue(self, tag):
        """ Get a specific DataRequestQueue. """
        if tag not in self.queues:
            raise ValueError(f'Unsupported queue name: "{tag}".')
        elif tag == QUEUE_MONITORING:
            if self.queues[tag] is None:
                self.queues[tag] = MonitoringQueue(self, timeout=DEFAULT_TIMEOUT)
            return self.queues[tag]            
        else:
            if self.queues[tag] is None:
                self.queues[tag] = DataRequestQueue(self, name=tag)
            return self.queues[tag]

    def cancel_request(self, reqObj):
        """ Cancel a request with IB. """
        app = self._get_app()
        reqObj._cancel_request_with_ib(app)

    def update_status(self, uniq_id):
        """ Record the time of any change in status.
        """
        reqObj = self.requests[uniq_id].object
        if not self.requests[uniq_id].is_updated():
            self.requests[uniq_id].update()
        else:
            raise ValueError('Unexpected attempt to record a status change that has already occured.')

        self.restriction_manager.update_status(reqObj)

    def get_active_requests(self):
        """ Return a list of requests that are still active. """
        return list([reqStatus.object for reqStatus in self.requests.values() if reqStatus.object.is_active()]) 
        
    def _register_new_request(self, reqObj):
        """ Save the details of a new request.
        """
        # Create a request ID for this request
        app = self._get_app()        
        reqObj.req_id = app._get_next_req_id()

        # Check that we are not re-registering an old request with the App (this should never happen)
        if reqObj.req_id in app.requests:
            raise ValueError(f'The request req_id {reqObj.req_id} has already been registered.')
        else:
            app.requests[reqObj.req_id] = reqObj

        # Check that we are not re-registering an old request with the Request Manager (this should never happen)
        if reqObj.uniq_id in self.requests:
            raise ValueError(f'The request uniq_id {reqObj.uniq_id} has already been registered.')
        else:
            self.requests[reqObj.uniq_id] = RequestStatus(reqObj)
        
        # Update the request status
        reqObj.status = mdconst.STATUS_REQUEST_QUEUED

    def _deregister_request(self, reqObj):
        if reqObj.uniq_id in self.requests:
            del self.requests[reqObj.uniq_id]

    def _get_app(self):
        """ Get an App instance from the MarketDataAppManager. """
        return mktdata_manager.get_app()

    def get_assigned_queue(self, reqObj):
        """ Find (or assign) DataRequestQueue to manage a particular request. 
        
            This assignment will be based on the type of DataRequest being made.
        """
        if isinstance(reqObj, ibk.marketdata.datarequest.HistoricalDataRequest):
            if not reqObj.is_snapshot:
                tag = QUEUE_STREAM
            elif reqObj.is_small_bar:
                tag = QUEUE_HIST_SMALL_BAR
            else:
                tag = QUEUE_HIST_LARGE_BAR
        elif isinstance(reqObj, ibk.marketdata.datarequest.HistoricalTickDataRequest):
            tag = QUEUE_HIST_SMALL_BAR
        elif isinstance(reqObj, ibk.marketdata.datarequest.StreamingBarRequest):
            tag = QUEUE_STREAM
        elif isinstance(reqObj, (ibk.marketdata.datarequest.FundamentalDataRequest,
                                 ibk.marketdata.datarequest.HeadTimeStampDataRequest,
                                 ibk.marketdata.datarequest.ScannerParametersDataRequest,)):
            tag = QUEUE_GENERIC
        elif isinstance(reqObj, ibk.marketdata.datarequest.ScannerDataRequest):
            tag = QUEUE_SCANNER
        elif isinstance(reqObj, ibk.marketdata.datarequest.MarketDataRequest):
            if not reqObj.is_snapshot:
                tag = QUEUE_STREAM
            else:
                tag = QUEUE_GENERIC
        elif isinstance(reqObj, ibk.marketdata.datarequest.StreamingTickDataRequest):
            tag = QUEUE_TICK_STREAM
        else:
            raise ValueError(f'Unsupported data request class: {reqObj.__class__}')

        return self.get_queue(tag)


class DataRequestQueue:
    def __init__(self, request_manager, name=''):
        self.request_manager = request_manager
        self.name = name

        self.queue = queue.PriorityQueue()
        self.thread = None

        self.counter = 0
        self.n_timeouts = 0
        self.max_timeouts = 3

    def _get_app(self):
        return self.request_manager._get_app()

    @property
    def restriction_manager(self):
        return self.request_manager.restriction_manager

    @property
    def thread(self):
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(name=f'DataRequestQueue-{self.name}',
                                            target=self._process_requests)
            self._thread.start()

        return self._thread

    @thread.setter
    def thread(self, t):
        self._thread = t
    
    def enqueue_request(self, reqObj, priority=0):
        """ Put a request in a queue to be processed. 
        
            Arguments:
                reqObj: the request object to be processed.
                priority: (float) the requests with the lowest priority
                    will be processed first.
        """
        self.queue.put((priority, reqObj))
        reqObj.status = mdconst.STATUS_REQUEST_QUEUED

        # Make sure there is a live version of the thread
        _ = self.thread

    def qsize(self):
        return self.queue.qsize()

    def _process_requests(self):
        """ The target function run by the thread to process requests in the queue.
        
            This should be defined by the subclass in order to process requests.
        """
        finished_status = [mdconst.STATUS_REQUEST_SENT_TO_IB,
                           mdconst.STATUS_REQUEST_COMPLETE,
                           mdconst.STATUS_REQUEST_CANCELLED]
        while self.queue.qsize():
            priority, reqObj = self.queue.get(timeout=0.001)
            is_valid, msg = reqObj.is_valid_request()
            if not is_valid:
                # Check that this is a valid request
                raise ibk.errors.DataRequestError(msg)
            elif reqObj.status != mdconst.STATUS_REQUEST_QUEUED:
                raise ValueError('Unexpected status: this request is no longer queued.')
            else:
                reqObj.status = mdconst.STATUS_REQUEST_PROCESSING

            # Sleep until ready to process this request.
            self._wait_until_ready(reqObj)

            # Place the request
            app = self._get_app()
            reqObj._place_request_with_ib(app)

            # Put the request onto the monitoring queue to make sure it gets fulfilled
            self.request_manager.monitoring_queue.enqueue_request(reqObj, priority=priority)

            # Wait for request to propogate
            wait_time = 1.0
            t0 = time.time()
            if time.time() - t0 < wait_time and reqObj.status not in finished_status:
                time.sleep(0.05)

            # Re-queue the request if it timed out
            if reqObj.status not in finished_status:
                # Handle the case where the request timed out
                # Reset the request instance to its original settings, and add it back to the queue
                reqObj.cancel_request()
                reqObj.reset()
                print(f'Requeueing request {reqObj.uniq_id}...')
                self.enqueue_request(reqObj, priority=priority)
                self.n_timeouts += 1
            else:
                self.counter += 1
                self.n_timeouts = 0

            # If we have timed out too many consecutive times, try disconnecting and reconnecting 
            if self.n_timeouts > self.max_timeouts:
                print('Reconnecting App...')
                app.disconnect()
                self.n_timeouts = 0

        # Get rid of the finished thread
        self.thread = None

    def _wait_until_ready(self, reqObj):
        """ Function that holds the request (sleeps) until ready to send it to IB.
        """
        is_satisfied = self.restriction_manager.check_is_satisfied(reqObj)
        while not all(is_satisfied.values()):
            time.sleep(0.1)
            is_satisfied = self.restriction_manager.check_is_satisfied(reqObj)


class MonitoringQueue:
    def __init__(self, request_manager, timeout=None):
        self.request_manager = request_manager
        
        if timeout is None:
            self.timeout = DEFAULT_TIMEOUT
        else:
            self.timeout = timeout

        self.name = QUEUE_MONITORING
        self.queue = queue.Queue()
        self.thread = None
        self.counter = 0

    def _get_app(self):
        return self.request_manager._get_app()

    @property
    def restriction_manager(self):
        return self.request_manager.restriction_manager

    @property
    def thread(self):
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(name=f'DataRequestQueue-{self.name}',
                                            target=self._process_requests)
            self._thread.start()

        return self._thread

    @thread.setter
    def thread(self, t):
        self._thread = t

    def enqueue_request(self, reqObj, priority=0):
        """ Put a request in a queue to be processed. 
        
            Arguments:
                reqObj: the request object to be processed.
                priority: (float) the requests with the lowest priority
                    will be processed first.
        """
        if reqObj.n_restarts > reqObj.max_restarts:
            raise ValueError(f'Maximum restarts exceeded for request {reqObj.uniq_id}.')
        
        self.queue.put((priority, reqObj))

        # Make sure there is a live version of the thread
        _ = self.thread

    def qsize(self):
        return self.queue.qsize()

    def _process_requests(self):
        """ The target function run by the thread to process requests in the queue.
        
            This should be defined by the subclass in order to process requests.
        """
        while self.queue.qsize():
            priority, reqObj = self.queue.get(timeout=0.001)

            # Check if the request was completed/cancelled or has returned any data
            if reqObj.status not in (mdconst.STATUS_REQUEST_COMPLETE, mdconst.STATUS_REQUEST_CANCELLED,) \
                    and not reqObj.has_data():

                # Get the time that the request was placed
                t_0 = self.request_manager.requests[reqObj.uniq_id].info[mdconst.STATUS_REQUEST_SENT_TO_IB]

                # Check if the max wait time has been exceedeed
                if time.time() - t_0 > self.timeout:
                    # Cancel the request, as it has timed out
                    reqObj.cancel_request()
                    if reqObj.n_restarts == reqObj.max_restarts:
                        # If we have already exceeded our allowed restarts, then cancel the request
                        reqObj.status = mdconst.STATUS_REQUEST_TIMED_OUT
                    else:
                        # ...otherwise try to place the request once again
                        N = reqObj.n_restarts
                        reqObj.reset()
                        reqObj.n_restarts = N + 1
                        self.request_manager.place_request(reqObj, priority)
                else:
                    # We haven't timed out yet, so put the request back on the queue and wait longer
                    self.queue.put((priority, reqObj))
            
            # Sleep after checking each request so we don't use too much CPU rechecking requests
            time.sleep(1)

        # Get rid of the finished thread
        self.thread = None


# Define a global version of the request manager
request_manager = GlobalRequestManager()        