import time
import threading

import ibk.constants
import ibk.orders
import ibk.contracts            
import ibk.account            
import ibk.marketdata
import ibk.errors


class ConnectionInfo():
    # Declare global variables used to handle the creation of connections
    _global_apps = dict()
    _global_ports = dict()
    _global_threads = dict()

    def __init__(self, port=None):
        self._port = port

    @property
    def port(self):
        return self._port
    
    @port.setter
    def port(self, p):
        if self._port != p:
            self.reset_connections()
            self._port = p
            
    def reset_connections(self):
        """ Reset all connections. """
        for _app in self._global_apps.values():
            _app.disconnect()
            
        self._global_apps = dict()
        self._global_ports = dict()
        self._global_threads = dict()        

    def get_connection(self, client_name, clientId=None):
        """ Entry point into the IB API

            Arguments:
                client_name: the name of the client that we are trying to connect.
                    Available options are 'marketdata', 'account', 'orders', 'contracts'
                clientId: (int) Optional - specifiy which client Id to
                    use for creating a new client or retrieving an open client
        """
        if client_name == ibk.constants.MARKETDATA:
            class_handle = ibk.marketdata.MarketDataApp
        elif client_name == ibk.constants.ACCOUNT:
            class_handle = ibk.account.AccountApp
        elif client_name == ibk.constants.ORDERS:
            class_handle = ibk.orders.OrdersApp
        elif client_name == ibk.constants.CONTRACTS:
            class_handle = ibk.contracts.ContractsApp
        else:
            raise ValueError(f'Unsupported client: {client_name}')

        return self._get_connection_from_class(class_handle, clientId=clientId)
            
    def _find_existing_app_instance(self, class_handle, clientId):
        """ Find an application that has already been created.
            If a clientId is not provided, then find an application of the same type.
        """
        if clientId is not None:
            # Return the application with the requested client Id if it has the same class handle
            if clientId in self._global_apps and self._global_ports[clientId] == self.port:
                if isinstance(self._global_apps[clientId], class_handle):
                    return self._global_apps[clientId]
                else:
                    return None
            else:
                return None
        else:
            # If no client Id is specified, find the first application of the same type with the same port
            clientIds = [cid for cid, app in self._global_apps.items()]
            while clientIds:
                cid = clientIds.pop(0)
                if self._global_ports[cid] == self.port \
                        and self._global_apps[cid].isConnected() \
                        and isinstance(self._global_apps[cid], class_handle):                    
                    return self._global_apps[cid]

            # If we reach this line, then no application was found with matching type and port
            return None

    def _connect_and_check(self, class_handle, clientId):
        """Attempt to connect an application. Return None if no connection is established."""
        app = class_handle()
        app.connect(ibk.constants.HOST_IP, port=self.port, clientId=clientId)
        _thread = threading.Thread(target=app.run, name=class_handle.__name__)
        _thread.start()
        
        # Wait to see if the connection can be established
        t = 0
        while app.req_id() is None and t < 10:
            time.sleep(0.2)
            t += 1

        # Return the instance if a connection is established
        if app.req_id() is not None:
            return app, _thread
        else:
            return None, None

    def _get_connection_from_class(self, class_handle, clientId=None):
        """Entry point into the program.

        Arguments:
            class_handle: The handle for the that inherits from IB EClient/EWrapper
            clientId: (int) the ID of the client (optional)
        """
        # Retrieve application if one already exists with these specs
        app = self._find_existing_app_instance(class_handle, clientId=clientId)
        if app is not None:
            return app
        else:
            # ...otherwise open a new connection
            if clientId is not None:
                # A specific client Id has been requested
                app, _thread = self._connect_and_check(class_handle, clientId)
            else:
                # No specific client Id has been requested, so we try
                #     different client Ids until we find one that works
                cid = n_attempts = 1
                max_attempts = 30
                print('Attempting to connect with unused clientId...'.format(cid))
                while (app is None or not app.isConnected()) and n_attempts <= max_attempts:
                    while cid in self._global_apps.keys():
                        cid += 1
                        
                    app, _thread = self._connect_and_check(class_handle, clientId=cid)
                    cid += 1
                    n_attempts += 1

            if app is None or not app.isConnected():
                # If still not connecting, try more time to raise an exception
                msg = ''.join(['Connection could not be established. ',
                               'Check that the correct port has been specified.'])
                raise ibk.errors.ConnectionNotEstablishedError(msg)
            else:
                self._global_apps[app.clientId] = app
                self._global_threads[app.clientId] = _thread
                self._global_ports[app.clientId] = self.port
                print("serverVersion:{} connectionTime:{} clientId:{}".format(\
                            app.serverVersion(), app.twsConnectionTime(), app.clientId))
                print('MarketDataApp connecting to IB...')
                return app
