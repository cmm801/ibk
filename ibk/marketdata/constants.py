
# Default arguments
DEFAULT_USE_RTH = False

# IB TWS Field codes
LAST_TIMESTAMP = 45

# The tick data code used to obtain fundamental data in a MarketDataRequest
FUNDAMENTAL_TICK_DATA_CODE = 47

# Status flags
STATUS_REQUEST_TIMED_OUT = -2                 # request has timed out
STATUS_REQUEST_CANCELLED = -1                 # request has been cancelled
STATUS_REQUEST_NEW = 0                        # new request
STATUS_REQUEST_COMPLETE = 1                   # request is complete
STATUS_REQUEST_QUEUED = 2                     # is queued in request manager
STATUS_REQUEST_PROCESSING = 3                 # has been removed from queue and being processed
STATUS_REQUEST_SENT_TO_IB = 4                 # has been sent to IB


# List of all supported request statuses
STATUS_REQUEST_OPTIONS = (STATUS_REQUEST_TIMED_OUT,
                          STATUS_REQUEST_CANCELLED,
                          STATUS_REQUEST_NEW,
                          STATUS_REQUEST_COMPLETE,
                          STATUS_REQUEST_QUEUED,
                          STATUS_REQUEST_PROCESSING,
                          STATUS_REQUEST_SENT_TO_IB,
                         )

# Types of restrictions on data requests
RESTRICTION_CLASS_SIMUL_HIST = 0
RESTRICTION_CLASS_SIMUL_STREAMS = 1
RESTRICTION_CLASS_SIMUL_SCANNERS = 2
RESTRICTION_CLASS_SIMUL_TICK_STREAMS = 3
RESTRICTION_CLASS_HF_HIST_IDENTICAL = 4
RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW = 5
RESTRICTION_CLASS_HF_HIST_LONG_WINDOW = 6
RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT = 7

RESTRICTION_CLASSES = (
    RESTRICTION_CLASS_SIMUL_HIST,
    RESTRICTION_CLASS_SIMUL_STREAMS,
    RESTRICTION_CLASS_SIMUL_SCANNERS,
    RESTRICTION_CLASS_SIMUL_TICK_STREAMS,
    RESTRICTION_CLASS_HF_HIST_IDENTICAL,
    RESTRICTION_CLASS_HF_HIST_SHORT_WINDOW,
    RESTRICTION_CLASS_HF_HIST_LONG_WINDOW,
    RESTRICTION_CLASS_TICK_STREAM_SAME_CONTRACT,
)
