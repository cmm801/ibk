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
        print('ERROR: request {}'.format(reqId))

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
    
