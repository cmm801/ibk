""" A module containing the errors used for IB TWS alerting.
"""


class ServerValidationError(Exception):
    """ Exception for handling case when the server raises an error while validating the request.
    """
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(ServerValidationError, self).__init__(message)


class AmbiguousContractError(Exception):
    """ Exception for handling ambiguously defined contract requests.
    """
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(AmbiguousContractError, self).__init__(message)


class ConnectionNotEstablishedError(Exception):
    """ Exception for handling case when connection could not be established to IB server."""
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(ConnectionNotEstablishedError, self).__init__(message)


class DataRequestError(Exception):
    """Exceptions generated during requesting historical market data.
    """
    def __init__(self, *args,**kwargs):
        super(DataRequestError, self).__init__(*args,**kwargs)


class AttemptingToReuseClientIdError(Exception):
    """ Exception for case when in use client ID is used to establish a new connection."""
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(AttemptingToReuseClientIdError, self).__init__(message)
