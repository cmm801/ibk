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
import threading

from ibapi import wrapper
from ibapi.client import EClient
from ibapi.common import TickerId

def setup_logger():
    """Setup the logger.
    """
    if not os.path.exists("log"):
        os.makedirs("log")

    time.strftime("pyibapi.%Y%m%d_%H%M%S.log")

    recfmt = "(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s" \
             "%(filename)s:%(lineno)d %(message)s"

    timefmt = '%y%m%d_%H:%M:%S'

    logging.basicConfig(
        filename=time.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
        filemode="w",
        level=logging.INFO,
        format=recfmt, datefmt=timefmt
    )
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logger.addHandler(console)
    logging.debug("now is %s", datetime.datetime.now())


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


class ConnectionNotEstablishedError(Exception):
    """ Exception for handling case when connection could not be established to IB server."""
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(ConnectionNotEstablishedError, self).__init__(message)


class BaseApp(IBWrapper, IBClient):
    """Main program class. The TWS calls nextValidId after connection, so
    the method is over-ridden to provide an entry point into the program.
    """
    def __init__(self):
        IBWrapper.__init__(self)
        IBClient.__init__(self, app_wrapper=self)
        self.__req_id = None

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        """Overide EWrapper error method.
        """
        super().error(reqId, errorCode, errorString)
        if errorCode == 502:
            raise ConnectionNotEstablishedError(''.join(['A connection could not be established. ',
                                                         'Check that the correct port has been specified and ',
                                                         'that the client Id is not already in use.\n',
                                                         errorString]))


    def nextValidId(self, reqId: int):
        """Method of EWrapper.
        Sets the request id req_id class variable.
        This method is called from after connection completion, so
        provides an entry point into the class.
        """
        super().nextValidId(reqId)
        self.__req_id = reqId
        return self

    def keyboardInterrupt(self):
        """Stop exectution.
        """
        pass

    def req_id(self):
        """Retrieve the current request id."""
        return self.__req_id

    def _get_next_req_id(self):
        """Retrieve the current class variable req_id and increment
        it by one.

        Returns (int) current req_id
        """
        current_req_id = self.__req_id
        self.__req_id += 1
        return current_req_id

def _find_existing_app_instance(class_handle, port, clientId, global_apps, global_ports):
    """ Find an application that has already been created.
        If a clientId is not provided, then find an application of the same type.
    """
    if not global_apps:
        return None
    elif clientId is not None:
        # Return the application with the requested client Id
        if clientId in global_apps and global_ports[clientId] == port:
            return global_apps[clientId]
        else:
            return None
    else:
        # If no client Id is specified, find the first application of the same type with the same port
        clientIds = [cid for cid, app in global_apps.items()]
        while clientIds:
            cid = clientIds.pop(0)
            if global_ports[cid] == port and global_apps[cid].isConnected():
                return global_apps[cid]
        # If we get here, then no application was found with matching type and port
        return None

def _connect_and_check(class_handle, port, clientId):
    """Attempt to connect an application. Return None if no connection is established."""
    app = class_handle()
    app.connect("127.0.0.1", port=port, clientId=clientId)
    _thread = threading.Thread(target=app.run)
    _thread.start()
    t = 0
    while app.req_id() is None and t < 10:
        time.sleep(0.2)
        t += 1
    if app.req_id() is not None:
        return app, _thread
    else:
        return None, None

def _get_instance(class_handle, port, clientId=None, global_apps=None, 
                  global_ports=None, global_threads=None):
    """Entry point into the program.

    Arguments:
    port (int): Port number that IBGateway, or TWS is listening.
    """
    # Retrieve application if one already exists with these specs
    app = _find_existing_app_instance(class_handle, port=port, clientId=clientId, \
                                     global_apps=global_apps, global_ports=global_ports)
    if app is not None:
        return app
    else:
        # ...otherwise open a new connection
        if clientId is not None:
            # A specific client Id has been requested
            app, _thread = _connect_and_check(class_handle, port, clientId)
        else:
            # No specific client Id has been requested, so we try
            #     different client Ids until we find one that works
            cid = j = 1
            n_iters = 10
            print('Attempting to connect with unused clientId...'.format(cid))
            while (app is None or not app.isConnected()) and j <= n_iters:
                while cid in global_apps.keys():
                    cid += 1

                try:
                    app, _thread = _connect_and_check(class_handle, port, clientId=cid)
                except:
                    cid += 1
                    j += 1
                else:
                    cid += 1
                    j += 1

        if app is None or not app.isConnected():
            # If still not connecting, try more time to raise an exception
            msg = ''.join(['Connection could not be established. ',
                           'Check that the port is correct, and that the correct port has been specified.'])
            raise ConnectionNotEstablishedError(msg)
        else:
            global_apps[app.clientId] = app
            global_threads[app.clientId] = _thread
            global_ports[app.clientId] = port
            print("serverVersion:{} connectionTime:{} clientId:{}".format(\
                        app.serverVersion(), app.twsConnectionTime(), app.clientId))
            print('MarketDataApp connecting to IB...')
            return app

