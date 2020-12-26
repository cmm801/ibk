import collections
import datetime
import time


HIST_HF_TOT_REQ_PER_TIME_UNIT = (60, 600)    # total # of requests allowed per unit time (sec)
HIST_HF_CONTRACT_REQ_PER_TIME_UNIT = (6, 2)  # no. requests allowed on 1 contract per unit time
HIST_HF_SPACING_FOR_IDENTICAL_REQ = 15       # time required between identical high-freq requests

MAX_SIMUL_HISTORICAL_REQUESTS = 50   # Max # of simultaneous historical data requests
MAX_SIMUL_SCANNERS = 10              # Max # of simultaneous market scanners
MAX_SIMUL_MKT_DATA_LINES = 100       # Max # of simultaneous market data streams (aka lines)

MAX_SIMUL_TICK_DATA_REQUESTS = 3                # Max # of simultaneous streaming tick data requests
MIN_TIME_BTWN_TICK_REQ_ON_SAME_INSTRUMENT = 15  # Min. time to wait between tick requests on same contract

ERROR_EXCEED_MAX_SIMUL_HIST_REQUESTS = -1       # Error code if max # of simul. hist. requests exceeded
ERROR_EXCEED_MAX_SIMUL_SCANNERS = -2            # Error code if max # of simul. scanners exceeded
ERROR_EXCEED_MAX_SIMUL_MKT_DATA_LINES = -3      # Error code if max # of simul. market data lines exceeded
ERROR_EXCEED_MAX_SIMUL_TICK_DATA_REQUESTS = -4  # Error code if max # of simul. tick requests exceeded

# Default time to sleep between historical data requests (in seconds)
DEFAULT_SLEEP_TIME_FOR_HISTORICAL_REQUEST = 0.2

# Data Request types
RESTRICTION_CLASS_MKT_DATA_LINES = 'market_data_lines'
RESTRICTION_CLASS_HISTORICAL_HF = 'historical_high_freq'
RESTRICTION_CLASS_HISTORICAL_LF = 'historical_low_freq'
RESTRICTION_CLASS_FUNDAMENTAL = 'fundamental'
RESTRICTION_CLASS_TICK_DATA = 'tick_data'
RESTRICTION_CLASS_SCANNER = 'scanner'
RESTRICTION_CLASS_NONE = 'none'


class RequestManager():
    """ Class for managing the # of requests to avoid violating IB restrictions.

        Different types of IB requests are subjected to different types of 
        restrictions. This class looks at individual requests, and provides
        information to the request about the validity of its request.
        
        IB subjects some requests to pacing requirements.
        IB defines small bar requests as having a bars of 30 seconds or smaller.
        The requirement is that requests which are defined as 'small bar' can
        be made only at a certain rate. If the rate of requests exceed the allowed 
        rate, then IB would reject the request. 
        
        This RequestManager class slows down the requests to avoid violations.
    """    
    _small_bar_hist_data_requests = collections.deque(maxlen=HIST_HF_TOT_REQ_PER_TIME_UNIT[0])

    def __init__(self):
        """ Class initializer. """
        self.requests = dict()
        self.requests_complete = dict()
        
        self.open_streams = set()
        self.open_tick_streams = set()
        self.open_hist_reqs = set()
        self.open_lines = set()
        self.open_scanners = set()
        
        self._tick_requests = dict()

    def register_request(self, requestObj):
        """ Save the details of a new request.
        """
        print('Request made...')        
        req_id = requestObj.get_req_ids()[0]
        self.requests[req_id] = requestObj

        if not requestObj.is_snapshot:
            self.open_streams.add(req_id)

        if RESTRICTION_CLASS_HISTORICAL_HF == requestObj.restriction_class:
            self.open_hist_reqs.add(req_id)
            self._small_bar_hist_data_requests.appendleft((time.time(), requestObj))
            if requestObj.data_type == 'BID_ASK':
                # BID_ASK requests count 2x, so we add an extra copy of the request to the queue
                self._small_bar_hist_data_requests.appendleft((time.time(), requestObj))
        elif RESTRICTION_CLASS_HISTORICAL_LF == requestObj.restriction_class:
            self.open_hist_reqs.add(req_id)
        elif RESTRICTION_CLASS_FUNDAMENTAL == requestObj.restriction_class:
            pass
        elif RESTRICTION_CLASS_SCANNER == requestObj.restriction_class:
            self.open_scanners.add(req_id)
        elif RESTRICTION_CLASS_MKT_DATA_LINES == requestObj.restriction_class:
            self.open_lines.add(req_id)
        elif RESTRICTION_CLASS_TICK_DATA == requestObj.restriction_class:
            self.open_tick_streams.add(req_id)
            self._tick_requests[requestObj.contract.localSymbol] = time.time()
        elif RESTRICTION_CLASS_NONE == requestObj.restriction_class:
            pass
        else:
            raise ValueError(f'Unknown restriction class: {requestObj.restriction_class}')

    def register_request_complete(self, req_id):
        """ Change the register information to indicate a request is closed.
        """
        self.requests_complete[req_id] = datetime.datetime.now()

        if req_id in self.open_streams:
            self.open_streams.remove(req_id)

        if req_id in self.open_tick_streams:
            self.open_tick_streams.remove(req_id)

        if req_id in self.open_hist_reqs:
            self.open_hist_reqs.remove(req_id)

        if req_id in self.open_lines:
            self.open_lines.remove(req_id)

        if req_id in self.open_scanners:
            self.open_scanners.remove(req_id)

    def is_request_complete(self, req_id):
        return req_id in self.requests_complete

    def check_if_ready(self, requestObj):
        """ Provides guidance on whether a request can be placed.
        
            Tells the caller whether the request can be placed
            immediately, whether it cannot be placed at all, or
            whether it must sleep for some time before going ahead.
        """
        if RESTRICTION_CLASS_HISTORICAL_HF == requestObj.restriction_class:
            return self._check_if_ready_historical_high_freq(requestObj)
        elif RESTRICTION_CLASS_HISTORICAL_LF == requestObj.restriction_class:
            return self._check_if_ready_historical_low_freq(requestObj)
        elif RESTRICTION_CLASS_FUNDAMENTAL == requestObj.restriction_class:
            return self._check_if_ready_fundamental(requestObj)
        elif RESTRICTION_CLASS_SCANNER == requestObj.restriction_class:
            return self._check_if_ready_scanner(requestObj)
        elif RESTRICTION_CLASS_MKT_DATA_LINES == requestObj.restriction_class:
            return self._check_if_ready_lines(requestObj)
        elif RESTRICTION_CLASS_TICK_DATA == requestObj.restriction_class:
            return self._check_if_ready_tick_data(requestObj)        
        elif RESTRICTION_CLASS_NONE == requestObj.restriction_class:
            return self._check_if_ready_none(requestObj)
        else:
            raise ValueError(f'Unknown restriction class: {requestObj.restriction_class}')

    def _check_if_ready_historical_high_freq(self, requestObj):
        """ Check simultaneous high frequency data requests.
        """        
        if len(self.open_hist_reqs) + 1 > MAX_SIMUL_HISTORICAL_REQUESTS:
            # Notify the caller that the max. historical requests have been reached
            return ERROR_EXCEED_MAX_SIMUL_HIST_REQUESTS
        else:
            # Always sleep between hist. requests to avoid overloading the server
            sleep_default = DEFAULT_SLEEP_TIME_FOR_HISTORICAL_REQUEST

            # Sleep more if historical requests on same contract are too frequent
            sleep_req_on_same_ct = self._check_hist_hf_requests_on_same_contract(requestObj)

            # Sleep more if rate of historical high freq. requests is too high
            sleep_tot_hist_req = self._ensure_total_hist_hf_requests_not_exceeded()

            # Sleep more if making identical requests too frequently
            sleep_identical = self._check_hist_hf_identical_requests(requestObj)

            # Sleep the maximum amount
            return max(sleep_default, sleep_req_on_same_ct, sleep_tot_hist_req)

    def _check_if_ready_tick_data(self, requestObj):
        """ Check simultaneous tick data requests.
        
            Only 1 streaming tick data request per contract is allowed every 15 seconds.
            Only 3 streaming tick data requests are allowed to be open at any time.
        """        
        if len(self.open_tick_streams) + 1 > MAX_SIMUL_TICK_DATA_REQUESTS:
            # Notify the caller that the max. historical requests have been reached
            return ERROR_EXCEED_MAX_SIMUL_TICK_DATA_REQUESTS
        else:
            # Get the time of the last tick request on this contract
            t_last = self._tick_requests.get(requestObj.contract.localSymbol, 0.0)

            # Don't make a repeat request on the same contract too quickly
            if time.time() - t_last < MIN_TIME_BTWN_TICK_REQ_ON_SAME_INSTRUMENT:
                return MIN_TIME_BTWN_TICK_REQ_ON_SAME_INSTRUMENT - (time.time() - t_last) + 0.1
            else:
                return 0.0

    def _check_if_ready_historical_low_freq(self, requestObj):
        """ Check simultaneous low frequency data requests.
        """
        if len(self.open_hist_reqs) + 1 > MAX_SIMUL_HISTORICAL_REQUESTS:
            # Notify the caller that the max. historical requests have been reached
            return ERROR_EXCEED_MAX_SIMUL_HIST_REQUESTS
        else:        
            # Always sleep between hist. requests to avoid overloading the server
            return DEFAULT_SLEEP_TIME_FOR_HISTORICAL_REQUEST

    def _check_if_ready_fundamental(self, requestObj):
        return 0.0

    def _check_if_ready_scanner(self, requestObj):
        if len(self.open_scanners) + 1 > MAX_SIMUL_SCANNERS:
            return ERROR_EXCEED_MAX_SIMUL_SCANNERS
        else:
            return 0.0

    def _check_if_ready_lines(self, requestObj):
        if len(self.open_scanners) + 1 > MAX_SIMUL_MKT_DATA_LINES:
            return ERROR_EXCEED_MAX_SIMUL_MKT_DATA_LINES
        else:
            return 0.0

    def _check_if_ready_none(self, requestObj):
        return 0.0

    def _check_hist_hf_requests_on_same_contract(self, requestObj):
        """ Check the rate of historical small bar requests on the same contract.
        
            IB does not allow more than 6 small bar requests on the same contract
            with the same Tick Type (e.g. 'TRADES', 'BID') within 15 seconds.

            This method returns the amount of time needed to sleep before 
            performing the specified request in order to avoid pacing violations.
        """
        # Get the information about the spacing of historical requests
        N, T = HIST_HF_CONTRACT_REQ_PER_TIME_UNIT

        # Get the number of new requests (BID/ASK data counts as 2 requests)
        if requestObj.data_type == 'BID_ASK':
            n_new_requests = 2
        else:
            n_new_requests = 1
            
        # Count the number of historical requests on the same contract
        count = 0
        for past_request in self._small_bar_hist_data_requests:
            t_past, reqObj_past = past_request
            if time.time() - t_past > T:
                # All requests after this are too old to matter
                break
            elif count == N - n_new_requests:
                # We already know we will violate the contraint
                break
            elif str(requestObj.contract) == str(reqObj_past.contract) \
                    and requestObj.data_type == reqObj_past.data_type:
                count += 1
                t_first = t_past

        # Determine how much time we need to sleep to avoid a pacing violation
        if count == N - n_new_requests:
            dt = time.time() - t_first  # The time passed since the first request
            return T - dt + 0.1         # The amount of time needed to sleep
        else:
            return 0.0

    def _ensure_total_hist_hf_requests_not_exceeded(self):
        """ Check if the rate of high freq. hist. requests is too high.
        """
        N, T = HIST_HF_TOT_REQ_PER_TIME_UNIT
        if N == len(self._small_bar_hist_data_requests):
            time_of_nth_request = self._small_bar_hist_data_requests[-1][0]
            dt = time.time() - time_of_nth_request
            if dt < T:
                return T - dt + 0.1
            else:
                return 0.0
        else:
            return 0.0

    def _check_hist_hf_identical_requests(self, requestObj):
        """ Check identical historical small bar requests.
        
            Return the number of seconds needed to sleep to avoid
            too frequent identical requests.
        """
        for past_request in self._small_bar_hist_data_requests:
            t_past, reqObj_past = past_request
            if time.time() - t_past > HIST_HF_SPACING_FOR_IDENTICAL_REQ:
                return 0.0  # All requests after this are too old to matter
            elif str(requestObj.contract) == str(reqObj_past.contract) \
                    and requestObj.data_type == reqObj_past.data_type:
                return HIST_HF_SPACING_FOR_IDENTICAL_REQ - t_past + 0.1

        # We have gone through all requests with no matching requests found
        return 0.0
