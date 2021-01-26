"""
Module to facilitate trading through Interactive Brokers's API
see: https://interactivebrokers.github.io/tws-api/index.html

Brent Maranzano
Dec. 14, 2018

Classes
    IBClient (EClient): Creates a socket to TWS or IBGateway, and handles
        sending commands to IB through the socket.
    IBWrapper (EWrapper): Hanldes the incoming data from IB. Many of these
        methods are callbacks from the request commands.
    BaseApp (IBWrapper, IBClilent): This provides the main functionality. Many
        of the methods are over-rides of the IBWrapper commands to customize
        the functionality.
"""

import os.path
import time
import logging
import datetime
import collections

from ibapi import wrapper
from ibapi.client import EClient
from ibapi.common import TickerId

import ibk.constants
import ibk.connect
import ibk.errors


def setup_logger():
    """Setup the logger.
    """
    if not os.path.exists("log"):
        os.makedirs("log")

    #time.strftime("pyibapi.%Y%m%d_%H%M%S.log")
    filename = os.path.join(ibk.constants.DIRECTORY_LOGS, 'ibk.log')
    recfmt = "(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s " \
             "%(filename)s:%(lineno)d %(message)s"
    datefmt = '%y%m%d_%H:%M:%S'

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    handler = logging.FileHandler(filename, 'a', 'utf-8')
    handler.setFormatter(logging.Formatter(recfmt, datefmt=datefmt)) # or whatever
    logger.addHandler(handler)    

    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logger.addHandler(console)
    
    logging.debug("now is %s", datetime.datetime.now())
    return logger


class IBClient(EClient):
    """Subclass EClient, which delivers message to the TWS API socket.
    """
    def __init__(self, app_wrapper):
        EClient.__init__(self, app_wrapper)


class IBWrapper(wrapper.EWrapper):
    """Subclass EWrapper, which translates messages from the TWS API socket
    to the program.
    """
    def __init__(self):
        wrapper.EWrapper.__init__(self)


class BaseApp(IBWrapper, IBClient):
    """Main program class. The TWS calls nextValidId after connection, so
    the method is over-ridden to provide an entry point into the program.
    """
    logger = setup_logger()

    def __init__(self):
        IBWrapper.__init__(self)
        IBClient.__init__(self, app_wrapper=self)

        self.__req_id = None
        self.conn_info = None

    @property
    def connection_manager(self):
        """ Return an instance of the connection manager, once a connection has been established.
        """
        if self.conn_info is not None:
            return ibk.connect.ConnectionManager(port=self.conn_info.port, 
                                                 host=self.conn_info.host)
        else:
            return None

    def connect(self, host=None, port=None, clientId=None):
        """ Establish a connection with the client and register the connection. """
        if port is None:
            raise ValueError('Port must be specified to establish a connection.')
            
        if host is None:
            host = ibk.constants.HOST_IP

        connection_mgr = ibk.connect.ConnectionManager(port=port, host=host)
        if clientId is not None:
            if clientId in connection_mgr.registered_clientIds:
                msg = 'Client ID {clientId} is already registered with another connection.'
                raise ibk.errors.AttemptingToReuseClientIdError(msg)
            else:
                super().connect(host=host, port=port, clientId=clientId)
        else:
            connection_mgr.connect_with_unknown_clientId(self)

        # Register the connection and save the connection information
        self.conn_info = connection_mgr.register_connection(self)

    def reconnect(self):
        """ Reestablish a connection if it has been broken. """
        if not self.isConnected():
            # We first must deregister the connection - otherwise it \
            #     will raise an exception for reusing a registered clientId
            self.connection_manager.deregister_connection(self)
            
            # Reestablish the connection using the info from the previous connection
            self.connect(host=self.conn_info.host, port=self.conn_info.port, 
                         clientId=self.conn_info.clientId)
        
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        """Overide EWrapper error method.
        """
        if errorCode == 502:
            msg = ''.join(['A connection could not be established. ',
                           'Check that the correct port has been specified and ',
                           'that the client Id is not already in use.\n',
                           errorString])
            raise ibk.errors.ConnectionNotEstablishedError(msg)
        elif errorCode == 200:
            # This error means that the contract request was ambiguous
            super().error(reqId, errorCode, errorString)            
            raise ibk.errors.AmbiguousContractError('Ambiguous contract definition.')
        elif errorCode == 321:
            super().error(reqId, errorCode, errorString)
            raise ibk.errors.ServerValidationError('Validation error returned by server.')
        else:
            ignorable_error_codes = [2104,  # Market data farm connection is OK 
                                     2106,  # A historical data farm is connected.
                                     2158,  # Sec-def data farm connection is OK
                                    ]
            
            if errorCode not in ignorable_error_codes:
                super().error(reqId, errorCode, errorString)

    def nextValidId(self, reqId: int):
        """Method of EWrapper.
        Sets the request id req_id class variable.
        This method is called from after connection completion, so
        provides an entry point into the class.
        """
        super().nextValidId(reqId)
        self.__req_id = reqId
        return self

    @property
    def req_id(self):
        """Retrieve the current request id."""
        return self.__req_id

    @property
    def account_number(self):
        """ Get the account number based on the port we used for the connection.
        """
        if self.port == ibk.constants.PORT_PAPER:
            return ibk.constants.TWS_PAPER_ACCT_NUM
        elif self.port == ibk.constants.PORT_PROD:
            return ibk.constants.TWS_PROD_ACCT_NUM
        else:
            raise ValueError(f'Unsupported port: {self.port}')

    def keyboardInterrupt(self):
        """Stop execution.
        """
        logging.error('Keyboard interrupt.')
        raise KeyboardInterrupt

    def _get_next_req_id(self):
        """Retrieve the current class variable req_id and increment
        it by one.

        Returns (int) current req_id
        """
        current_req_id = self.__req_id
        self.__req_id += 1
        return current_req_id
