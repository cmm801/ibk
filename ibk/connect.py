import time
from collections import namedtuple
import threading
import ibk.constants
import ibk.errors

# Maximum time to wait while trying to verify if a connection has been established
MAX_WAIT_TIME = 10

# Maximum number of clientIds to attempt for establishing a connection
MAX_CONNECTION_ATTEMPTS = 30

# Create a namedtuple to store information about different connections
ConnectionInfo = namedtuple('ConnectionInfo',
                            field_names=['name', 'host', 'port', 'clientId'],
                            defaults=['', ibk.constants.HOST_IP, None, None])

class ConnectionManager():
    # Declare global variables used to handle the creation of connections
    _registered_connections = {ibk.constants.PORT_PROD : {},
                               ibk.constants.PORT_PAPER : {}}

    def __init__(self, port=None, host=None):
        self.port = port

        if host is None:
            self.host = ibk.constants.HOST_IP
        else:
            self.host = host

    @property
    def registered_connections(self):
        return self._registered_connections[self.port]

    @property
    def registered_clientIds(self):
        return [x.clientId for x in self.registered_connections.keys()]

    def reset_connections(self):
        """ Reset all connections. """
        keys = list(self.registered_connections.keys())
        for k in keys:
            app = self.registered_connections.pop(k)
            if app is not None:
                app.disconnect()

    def get_connection(self, class_handle, clientId=None):
        """ Find an existing API connection with a given class handle, or else
            create a new API connection.

            Arguments:
                class_handle: A handle to a subclass of EClient/EWrapper 
                    that can connect to IB TWS.
                clientId: (int) the ID of the client (optional)
        """
        # Retrieve application if one already exists with these specs
        app = self.find_existing_connection(class_handle, clientId=clientId)
        
        if app is None:
            # Establish a new connection if there is no existing connection
            app = class_handle()
            self.establish_new_connection(app, clientId)

        return app

    def establish_new_connection(self, app, clientId=None):
        """ Extablish a new connection for a given class instance. """
        if clientId is not None:
            # A specific client Id has been requested
            self.connect_with_clientId(app, clientId)
        else:
            self.connect_with_unknown_clientId(app)
    
    def connect_with_clientId(self, app, clientId):
        """Attempt to connect an application. Return None if no connection is established."""
        app.connect(host=self.host, port=self.port, clientId=clientId)

        # Wait to see if the connection can be established
        t = 0
        while app.req_id is None and t < MAX_WAIT_TIME:
            time.sleep(0.2)
            t += 1

        # Register the new connection
        self.register_connection(app)

    def connect_with_unknown_clientId(self, app, clientId=None):
        """ Establish a new connection given an instance of an API connection.
        """
        # No specific client Id has been requested, so we try
        #     different client Ids until we find one that works
        if len(self.registered_clientIds):
            # Start with the next available client ID
            initial_cid = 1 + max(self.registered_clientIds)
        else:
            initial_cid = 1

        n_attempts = 1
        cid = initial_cid
        while (app is None or not app.isConnected()) and n_attempts <= MAX_CONNECTION_ATTEMPTS:
            while cid in self.registered_clientIds:
                cid += 1

            try:
                self.connect_with_clientId(app, clientId=cid)
            except ibk.errors.ConnectionNotEstablishedError:
                cid += 1
                n_attempts += 1

    def register_connection(self, app):
        if app is not None and app.isConnected():
            # Save the connection information
            info = ConnectionInfo(name=app.__class__.__name__, host=app.host, 
                                  port=app.port, clientId=app.clientId)
            self.registered_connections[info] = app
            return info
        else:
            # If still not connecting, try more time to raise an exception
            msg = ''.join(['Connection could not be established. ',
                           'Check that the correct port has been specified ',
                           'or that the clientId is not already in use.'])
            raise ibk.errors.ConnectionNotEstablishedError(msg)

    def deregister_connection(self, app):
        """ Deregister a connection from the manager. """
        if app is not None and app.conn_info is not None:
            if app.conn_info in self.registered_connections:
                del self.registered_connections[app.conn_info]
        
    def find_existing_connection(self, class_handle, clientId):
        """ Find an application that has already been created.
            If a clientId is not provided, then find an application of the same type.
        """
        if clientId is not None:
            # Return the instance if it is registered
            target_key = ConnectionInfo(name=class_handle.__name__, host=self.host, 
                                  port=self.port, clientId=clientId)
            app = self.registered_connections.get(target_key, None)
            if not app.isConnected():
                app.reconnect()
        else:
            # If no client ID is provided, try to find a match
            candidates = []            
            target_key = ConnectionInfo(name=class_handle.__name__, 
                                        host=self.host, port=self.port)
            for k, v in self.registered_connections.items():
                if target_key == ConnectionInfo(name=k.name, host=k.host, port=k.port):
                    candidates.append(v)

            # Get a list of any candidates that are already connected
            connected_candidates = [x for x in candidates if x.isConnected()]
            if len(connected_candidates):
                # If there are any candidates that are already connected, take the first
                app = connected_candidates[0]
            elif len(candidates) > 0:
                # If there are no connected candidates, see if we can connect with one of them
                app = candidates[0]
                app.reconnect()                
            else:
                app = None

        return app
