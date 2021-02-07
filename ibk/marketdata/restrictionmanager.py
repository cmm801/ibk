import ibk.marketdata.constants as mdconst
import collections
import time
import queue
import threading

import ibk.marketdata.constants as mdconst


# Maximum number of allowed simultaneous requests
MAX_SIMUL_REQUESTS = {
    mdconst.RESTRICTION_CLASS_SIMUL_HIST : 2, # 50      # Max # of simultaneous historical data requests
    mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS : 10,      # Max # of simultaneous market scanners
    mdconst.RESTRICTION_CLASS_SIMUL_STREAMS : 100,      # Max # of simultaneous market data streams (aka lines)
    mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS : 5,   # Max # of simultaneous tick data streams
}

# Limits to number of requests in window - (# requests per window, seconds per window)
MAX_REQUESTS_PER_WINDOW = {
    mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL : (1, 15),          # no. allowed identical requests
    mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW : (6, 2),        # no. requests allowed on 1 contract
    mdconst.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW : (60, 600),      # total # of requests allowed
    mdconst.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT : (1, 15),  # no. tick stream requests allowed on 1 contract
}


class RestrictionManager:
    requests = dict()
    
    # Define a set of containers to store the current set of historical / open requests
    restrictions = {
        mdconst.RESTRICTION_CLASS_SIMUL_HIST :
            dict(),
        mdconst.RESTRICTION_CLASS_SIMUL_STREAMS :
            dict(),
        mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS :
            dict(),
        mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS :
            dict(),
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

    def __init__(self):
        self.restriction_class_handler = {
            mdconst.RESTRICTION_CLASS_SIMUL_HIST :
                self._check_simultaneous_historical_requests,
            mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS :
                self._check_simultaneous_scanners,
            mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS :
                self._check_simultaneous_tick_streams,
            mdconst.RESTRICTION_CLASS_SIMUL_STREAMS :
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
            # Get a unique key to represent distinct data requests
            key = '{}_{}'.format(reqObj.__class__.__name__, reqObj.contract.localSymbol)
            for attr in ['start', 'end', 'frequency']:
                if hasattr(reqObj, attr):
                    key = key + '_' + str(reqObj.__getattribute__(attr))

            # Each unique request lives in its own container
            container = container[key]

        if container is None:
            raise ValueError(f'Unknown restriction class: "{res_class}".')        
        elif res_class in (mdconst.RESTRICTION_CLASS_SIMUL_HIST,
                           mdconst.RESTRICTION_CLASS_SIMUL_STREAMS,
                           mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS,
                           mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS):

            # Remove requests that are no longer open
            uniq_ids = list(container.keys())
            for uniq_id in uniq_ids:
                reqObj = container[uniq_id]
                if reqObj.status != mdconst.STATUS_REQUEST_SENT_TO_IB:
                    del container[uniq_id]
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
                               mdconst.STATUS_REQUEST_CANCELLED]:
            self._deregister(reqObj)
        else:
            pass  # Nothing to do here

    def check_is_satisfied(self, reqObj):
        result = dict()
        for res_class in reqObj.restriction_class:
            lock = self.locks[res_class]
            with lock:
                result[res_class] = self._check_single_restriction(reqObj, res_class)                
        return result

    def _register(self, reqObj):
        for res_class in reqObj.restriction_class:
            lock = self.locks[res_class]
            with lock:            
                self._register_single(reqObj, res_class)

    def _deregister(self, reqObj):
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
                container[reqObj.uniq_id] = reqObj
        elif res_class in (mdconst.RESTRICTION_CLASS_HF_HIST_LONG_WINDOW,
                           mdconst.RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW,
                           mdconst.RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT,):
            # Using a 'PriorityQueue' object
            entry = (time.time(), reqObj.uniq_id)
            container.put(entry)
            if reqObj.data_type == 'BID_ASK':
                container.put(entry)  # Getting 'BID_ASK' counts as 2 requests for IB
        elif res_class == mdconst.RESTRICTION_CLASS_HF_HIST_IDENTICAL:
            entry = (time.time(), reqObj.uniq_id)
            container.put(entry)

    def _deregister_single(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)

        if res_class in (mdconst.RESTRICTION_CLASS_SIMUL_HIST,
                         mdconst.RESTRICTION_CLASS_SIMUL_STREAMS,
                         mdconst.RESTRICTION_CLASS_SIMUL_SCANNERS,
                         mdconst.RESTRICTION_CLASS_SIMUL_TICK_STREAMS):
            # If the request has not already been removed from the container, then delete it
            if reqObj.uniq_id in container:
                del container[reqObj.uniq_id]
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
        return len(container) < MAX_SIMUL_REQUESTS[res_class]

    def _check_simultaneous_streams(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_SIMUL_REQUESTS[res_class]

    def _check_simultaneous_scanners(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_SIMUL_REQUESTS[res_class]

    def _check_simultaneous_tick_streams(self, reqObj, res_class):
        container = self.get_container(reqObj, res_class)
        return len(container) < MAX_SIMUL_REQUESTS[res_class]

    def _check_too_many_historical_requests(self, reqObj, res_class):
        """  Check if rate of historical high freq. requests is too high. """
        container = self.get_container(reqObj, res_class)
        return container.qsize() < MAX_REQUESTS_PER_WINDOW[res_class][1]

    def _check_identical_historical_requests(self, reqObj, res_class):
        """ Check identical historical small bar requests. """
        container = self.get_container(reqObj, res_class)
        return container.qsize() < MAX_REQUESTS_PER_WINDOW[res_class][1]

    def _check_historical_requests_on_same_contract(self, reqObj, res_class):
        """ Check if historical requests on same contract are too frequent. """
        container = self.get_container(reqObj, res_class)
        return container.qsize() < MAX_REQUESTS_PER_WINDOW[res_class][1]

    def _check_tick_streams_on_same_contract(self, reqObj, res_class):
        """ Only 1 streaming tick data request per contract is allowed every 15 seconds. """
        container = self.get_container(reqObj, res_class)
        return container.qsize() < MAX_REQUESTS_PER_WINDOW[res_class][1]

