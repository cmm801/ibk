import datetime
import time
import queue
import threading
import collections
from abc import ABC, abstractmethod

import ibk.connect
import ibk.errors

import ibk.marketdata.constants as mdconst
import ibk.marketdata.datarequest as datareq
from ibk.marketdata.app import mktdata_manager

# Define names of Queues used for managing data requests
QUEUE_STREAM = 'stream'
QUEUE_GENERIC = 'generic'
QUEUE_SCANNER = 'scanner'
QUEUE_TICK_STREAM = 'tick_Stream'
QUEUE_HIST_SMALL_BAR = 'hist_small_bar'
QUEUE_HIST_LARGE_BAR = 'hist_large_bar'

QUEUE_TYPES = [QUEUE_STREAM, QUEUE_GENERIC, QUEUE_SCANNER,
               QUEUE_TICK_STREAM, QUEUE_HIST_SMALL_BAR, QUEUE_HIST_LARGE_BAR]

# Maximum number of allowed simultaneous requests
MAX_SIMUL_REQUESTS = {
    mdconst.RESTRICTION_CLASS_SIMUL_HIST : 2, # 50        # Max # of simultaneous historical data requests
    mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS : 10,        # Max # of simultaneous market scanners
    mdconst.RESTRICTION_CLASS_SIMUL_MKT_DATA_LINES : 100, # Max # of simultaneous market data streams (aka lines)
    mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS : 5,     # Max # of simultaneous tick data streams
}

# Limits to number of requests in window - (# requests per window, seconds per window)
MAX_REQUESTS_PER_WINDOW = {
    mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL : (1, 15),          # no. allowed identical requests
    mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW : (6, 2),        # no. requests allowed on 1 contract
    mdconst.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW : (60, 600),      # total # of requests allowed
    mdconst.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT : (1, 15),  # no. tick stream requests allowed on 1 contract
}

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
            self.info[self.status] = datetime.datetime.now()


class GlobalRequestManager(ABC):
    requests = dict()                           # A map from unique Id to the DataRequest object
    restriction_manager = RestrictionManager()  # Requests being tracked to ensure restrictions are enforced

    # Define request queues
    queues = {q : None for q in QUEUE_TYPES}

    def place_request(self, reqObj, priority=0):
        """ Place a request with IB. """
        self._register_new_request(reqObj)
        
        # Route the request to one of the request queues
        assigned_queue = self.get_assigned_queue(reqObj)
        assigned_queue.enqueue_request(reqObj, priority=priority)
        
    def get_queue(self, tag):
        """ Get a specific DataRequestQueue. """
        if tag not in self.queues:
            raise ValueError(f'Unsupported queue name: "{tag}".')
        else:
            if self.queues[tag] is None:
                self.queues = DataRequestQueue(self, timeout=DEFAULT_TIMEOUT)
            return self.queues[tag]

    def cancel_request(self, reqObj):
        """ Cancel a request with IB. """
        self._cancel_request_core(reqObj)

    def update_status(self, uniq_id):
        """ Record the time of any change in status.
        """
        reqObj = self.requests[uniq_id].object
        if not self.requests[uniq_id].is_updated():
            self.requests[uniq_id].update()
        else:
            raise ValueError('Unexpected attempt to record a status change that has already occured.')

        self.restriction_manager.update_status(reqObj)

    def _register_new_request(self, reqObj):
        """ Save the details of a new request.
        """
        # Create a request ID for this request
        app = self._get_app()        
        reqObj.req_id = app._get_next_req_id()

        # Check that we are not re-registering an old request with the Request Manager (this should never happen)
        if reqObj.uniq_id in self.requests:
            raise ValueError(f'The request uniq_id {reqObj.uniq_id} has already been registered.')
        else:
            self.requests[reqObj.uniq_id] = RequestStatus(reqObj)
        
        # Check that we are not re-registering an old request with the App (this should never happen)
        app = self._get_app()
        if reqObj.req_id in app.requests:
            raise ValueError(f'The request req_id {reqObj.req_id} has already been registered.')
        else:
            app.requests[reqObj.req_id] = reqObj

        # Update the request status
        reqObj.status = mdconst.STATUS_REQUEST_QUEUED
        
    def _get_app(self):
        """ Get an App instance from the MarketDataAppManager. """
        return mktdata_manager.get_app()

    def get_assigned_queue(self, reqObj):
        """ Find (or assign) DataRequestQueue to manage a particular request. 
        
            This assignment will be based on the type of DataRequest being made.
        """
        if isinstance(reqObj, datareq.HistoricalDataRequest):
            if not reqObj.is_snapshot:
                tag = QUEUE_STREAM
            elif reqObj.is_small_bar:
                tag = QUEUE_HIST_SMALL_BAR
            else:
                tag = QUEUE_HIST_LARGE_BAR
        if isinstance(reqObj, datareq.HistoricalTickDataRequest):
            tag = QUEUE_HIST_SMALL_BAR
        elif isinstance(reqObj, datareq.StreamingBarRequest):
            tag = QUEUE_STREAM
        elif isinstance(reqObj, (datareq.FundamentalDataRequest,
                                 datareq.HeadTimeStampDataRequest,)):
            tag = QUEUE_GENERIC
        elif isinstance(reqObj, datareq.ScannerDataRequest):
            tag = QUEUE_SCANNER
        elif isinstance(reqObj, datareq.MarketDataRequest):
            if not reqObj.is_snapshot:
                tag = QUEUE_STREAM
            else:
                tag = QUEUE_GENERIC
        elif isinstance(reqObj, datareq.StreamingTickDataRequest):
            tag = QUEUE_TICK_STREAM
        else:
            raise ValueError(f'Unsupported data request class: {reqObj.__class__}')

        return self.get_queue(tag)


class DataRequestQueue:
    def __init__(self, request_manager, timeout=None):
        self.request_manager = request_manager
        if timeout is None:
            self.timeout = 1e6  # Don't time out if no timeout is specified

        self.queue = queue.PriorityQueue()
        self.thread = None

        self.counter = 0
        self.n_timeouts = 0
        self.max_timeouts = 3

        self.restriction_class_handler = {
            mdconst.RESTRICTION_CLASS_SIMUL_HIST : 
                self._check_simultaneous_historical_requests,
            mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS : 
                self._check_simultaneous_scanners,
            mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS : 
                self._check_simultaneous_tick_streams,
            mdconst.RESTRICTION_CLASS_SIMUL_MKT_DATA_LINES : 
                self._check_simultaneous_streams,
            mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL : 
                self._check_identical_historical_requests,
            mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW : 
                self._check_historical_requests_on_same_contract,
            mdconst.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW : 
                self._check_too_many_historical_requests,
            mdconst.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT : 
                self._check_tick_streams_on_same_contract,
        }

    def _get_app(self):
        return self.request_manager._get_app()

    @property
    def restriction_manager(self):
        return self.request_manager.restriction_manager

    @property
    def thread(self):
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(name=self.name, target=self._process_requests)
            self._thread.start()

        return self._thread

    @thread.setter
    def thread(self, t):
        self._thread = t
    
    @property
    def name(self):
        """ This is the name that is used for the thread that processes the queue. """
        return self.__class__.__name__

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

    def _process_requests(self):
        """ The target function run by the thread to process requests in the queue.
        
            This should be defined by the subclass in order to process requests.
        """
        finished_status = [mdconst.STATUS_REQUEST_COMPLETE,
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
            self._check_ready(reqObj)

            # Place the request
            app = self._get_app()
            reqObj._place_request_with_ib(app)

            # Re-queue the request if it timed out
            if reqObj.status not in finished_status:
                # Handle the case where the request timed out
                # Reset the request instance to its original settings, and add it back to the queue
                reqObj.cancel_request()
                reqObj.reset()
                self.enqueue_request(reqObj, priority=priority)
                self.n_timeouts += 1
            else:
                self.counter += 1
                self.n_timeouts = 0

            # If we have timed out too many consecutive times, try disconnecting and reconnecting 
            if self.n_timeouts > self.max_timeouts:
                app.disconnect()
                self.n_timeouts = 0

        # Get rid of the finished thread
        self.thread = None

    def _wait_until_ready(self, reqObj):
        """ Function that holds the request (sleeps) until ready to send it to IB.
        """
        is_satisfied = self.restriction_manager.check_if_satisfied(reqObj)
        while not all(is_satisfied.values()):
            time.sleep(0.1)


class RestrictionManager:
    # Define a set of containers to store the current set of historical / open requests
    restrictions = {
        mdconst.RESTRICTION_CLASS_SIMUL_HIST :
            set(),
        mdconst.RESTRICTION_CLASS_SIMUL_STREAMS :
            set(),
        mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS :
            set(),
        mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS :
            set(),
        mdconst.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW :
            queue.PriorityQueue(maxsize=MAX_REQUESTS_PER_WINDOW[mdconst.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW][0]),
        mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW :
            collections.defaultdict(lambda :
                queue.PriorityQueue(maxsize=MAX_REQUESTS_PER_WINDOW[mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW][0])),
        mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL :
            collections.defaultdict(lambda :
                queue.PriorityQueue(maxsize=MAX_REQUESTS_PER_WINDOW[mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL][0])),
        mdconst.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT :
            collections.defaultdict(lambda :
                queue.PriorityQueue(maxsize=MAX_REQUESTS_PER_WINDOW[mdconst.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT][0])),
    }
    
    # Define a set of threading locks to prevent race conditions when accessing shared resources
    locks = {res_class : threading.Lock() for res_class in mdconst.RESTRICTION_CLASSES}

    def get_container(self, reqObj, res_class):
        """ Get the container pertaining to a particular restriction class. 
         
            Make sure the container is up-to-date by removing closed/cancelled requests.
        """
        container = self.restrictions.get(res_class, None)
        if res_class in (mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW,
                         mdconst.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT):
            # There are separate containers for each contract
            container = container[reqObj.contract.localSymbol]
        elif res_class == mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL:
            key = '{}_{}_{}_{}'.format(reqObj.contract.localSymbol, reqObj.start,
                                    reqObj.end, reqObj.frequency)
            # Each unique request lives in its own container
            container = container[reqObj.contract.localSymbol]

        if container is None:
            raise ValueError(f'Unknown restriction class: "{res_class}".')        
        elif res_class in (mdconst.RESTRICTION_CLASS_SIMUL_HIST,
                           mdconst.RESTRICTION_CLASS_SIMUL_STREAMS,
                           mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS,
                           mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS):
            # Remove requests that are no longer open
            for uniq_id in list(container):
                reqObj = self.requests[uniq_id]
                if reqObj.status != mdconst.STATUS_REQUEST_SENT_TO_IB:
                    container.remove(uniq_id)
        elif res_class in (mdconst.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW,
                           mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW,
                           mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL,
                           mdconst.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT):
            # Get the number of seconds in a window
            T_max = MAX_REQUESTS_PER_WINDOW[res_class][1]
            
            # Remove old requests that no longer have an effect on throttling
            up_to_date = False
            while container.qsize() and not up_to_date:
                T, uniq_id = container.get()
                if time.time() - T >= T_max:
                    up_to_date = True
                    container.put((T, uniq_id))

        return container
    
    def update_status(self, reqObj):
        if reqObj.status == mdconst.STATUS_REQUEST_SENT_TO_IB:
            self._register(reqObj)
        elif reqObj.status in [mdconst.STATUS_REQUEST_COMPLETE,
                               mdconst.STATUS_REQUEST_CANCELLED],
            self._deregister(reqObj)

    def check_is_satisfied(self, reqObj):
        for res_class in reqObj.restriction_class:
            lock = self.locks[res_class]
            with lock:
                self._register_single(reqObj, res_class)

    def _register(self, reqObj):
        for res_class in reqObj.restriction_class:
            lock = self.locks[res_class]
            with lock:            
                self._register_single(reqObj, res_class)

    def _deregister(self, regObj):
        for res_class in reqObj.restriction_class:
            lock = self.locks[res_class]
            with lock:            
                self._deregister_single(reqObj, res_class)

    def _register_single(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)
        
        if res_class in (mdconst.RESTRICTION_CLASS_SIMUL_HIST,
                         mdconst.RESTRICTION_CLASS_SIMUL_STREAMS,
                         mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS,
                         mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS):
            # Using a 'set' object
            if reqObj.uniq_id in container:
                raise ValueError(f'Did not expect to find uniq_id already registered: {uniq_id}.')
            else:
                container.add(reqObj.uniq_id)
        elif res_class in (mdconst.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW,
                           mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW,
                           mdconst.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT):
            # Using a 'PriorityQueue' object
            entry = (time.time(), reqObj.uniq_id)
            container.put(entry)
            if reqObj.data_type == 'BID_ASK':
                container.put(entry)  # Getting 'BID_ASK' counts as 2 requests for IB
        elif res_class in (mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL):
            entry = (time.time(), reqObj.uniq_id)
            container.put(entry)
            
    def _deregister_single(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)
        if res_class in (mdconst.RESTRICTION_CLASS_SIMUL_HIST,
                         mdconst.RESTRICTION_CLASS_SIMUL_STREAMS,
                         mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS,
                         mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS):

            if reqObj.uniq_id not in container:
                raise ValueError(f'Expected to find uniq_id already registered: {uniq_id}.')
            else:
                container.remove(reqObj.uniq_id)
        elif res_class in (mdconst.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW,
                           mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW,
                           mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL,
                           mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS):
            pass # Nothing to deregister for historical request

    def _check_single_restriction(self, reqObj, res_class):
        """ Function that checks if a single restriction is resolved.
        """
        res_fun_handle = self.restriction_class_handler.get(res_class, None)
        if res_class is None:
            raise ValueError(f'Unknown restriction class: "{res_class}".')
        else:
            res_fun_handle = self.restriction_class_handler[res_class]
            return res_fun_handle(reqObj, res_class)

    def _check_simultaneous_historical_requests(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_SIMUL_REQUESTS[req_class]

    def _check_simultaneous_streams(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_SIMUL_REQUESTS[req_class]

    def _check_simultaneous_scanners(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_SIMUL_REQUESTS[req_class]

    def _check_simultaneous_tick_streams(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_SIMUL_REQUESTS[req_class]

    def _check_too_many_historical_requests(self, reqObj, res_class):
        """  Check if rate of historical high freq. requests is too high. """
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_REQUESTS_PER_WINDOW[res_class][1]

    def _check_identical_historical_requests(self, reqObj, res_class):
        """ Check identical historical small bar requests. """
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_REQUESTS_PER_WINDOW[res_class][1]

    def _check_historical_requests_on_same_contract(self, reqObj, res_class):
        """ Check if historical requests on same contract are too frequent. """
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_REQUESTS_PER_WINDOW[res_class][1]

    def _check_tick_streams_on_same_contract(self, reqObj, res_class):
        """ Only 1 streaming tick data request per contract is allowed every 15 seconds. """
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_REQUESTS_PER_WINDOW[res_class][1]


# Define a global version of the request manager
request_manager = GlobalRequestManager()        