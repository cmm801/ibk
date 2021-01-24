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

