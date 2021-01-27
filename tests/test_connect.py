import unittest
import sys, os

import pandas as pd

import ibapi

import ibk.account
import ibk.connect
import ibk.constants
import ibk.contracts
import ibk.master
import ibk.orders


class ConnectTest(unittest.TestCase):
    def setUp(self):
        """ Perform any required set-up before each method call. """
        self.connection_manager.reset_connections()

    def tearDown(self):
        """ Remove anything from 'setUp' after each method call. """
        self.connection_manager.reset_connections()

    @classmethod
    def setUpClass(cls):
        """ Perform any required set-up once, before any method is run. 
        
            This method should be used to build any classes or data structures
            that will be used by more than one of the test methods, and
            that cannot be built quickly on-the-fly.
        """
        cls.port = ibk.constants.PORT_PAPER

        # After execution, TWS will prompt you to accept the connection
        # The ERROR simply confirms that there is a connection to the market data.
        cls.connection_manager = ibk.connect.ConnectionManager(port=cls.port)

    @classmethod
    def tearDownClass(cls):
        """ Perform any required tear-down once, after all methods have been run. 
            
            This method can be used to destroy any structures created in setUpClass.
        """
        cls.connection_manager.reset_connections()
        del cls.connection_manager

    def test_establish_new_connection(self):
        """ Test the method 'establish_new_connection'. """
        print(f"\nRunning test method {self._testMethodName}\n")

        for clientId in [None, 21223241]:
            with self.subTest(clientId=None):
                app = ibk.account.AccountApp()
                self.connection_manager.establish_new_connection(app, clientId=clientId)
                self.assertTrue(app.isConnected())

    def test_connect_with_clientId(self):
        """ Test the method 'connect_with_clientId'. """
        print(f"\nRunning test method {self._testMethodName}\n")

        clientId = 112414215        
        with self.subTest('first_connection'):
            app = ibk.orders.OrdersApp()
            self.connection_manager.establish_new_connection(app, clientId=clientId)
            self.assertTrue(app.isConnected())

        with self.subTest('second_connection_same_class'):
            app = ibk.orders.OrdersApp()
            with self.assertRaises(ibk.errors.AttemptingToReuseClientIdError):
                self.connection_manager.establish_new_connection(app, clientId=clientId)

        with self.subTest('second_connection_different_class'):
            app = ibk.contracts.ContractsApp()
            with self.assertRaises(ibk.errors.AttemptingToReuseClientIdError):
                self.connection_manager.establish_new_connection(app, clientId=clientId)
        
    def test_connect_with_unknown_clientId(self):
        """ Test the method 'connect_with_unknown_clientId'. """
        print(f"\nRunning test method {self._testMethodName}\n")

        clientId = None
        app = ibk.contracts.ContractsApp()
        self.connection_manager.establish_new_connection(app, clientId=clientId)
        self.assertTrue(app.isConnected())

    def test_reset_connections(self):
        """ Test the method 'reset_connections'. """
        print(f"\nRunning test method {self._testMethodName}\n")
        app = ibk.marketdata.MarketDataApp()
        #app.connect(port=self.port, clientId=12)
        self.connection_manager.establish_new_connection(app, clientId=12)

        with self.subTest('connect'):
            self.assertTrue(app.isConnected())

        with self.subTest('thread_is_present'):
            self.assertTrue(app.thread is not None)

        with self.subTest('thread_is_alive'):
            self.assertTrue(app.thread is not None and app.thread.is_alive())

        app.disconnect()
        with self.subTest('disconnect'):
            self.assertFalse(app.isConnected())

        with self.subTest('disconnected_thread_is_none'):
            self.assertTrue(app.thread is None)

        app.reconnect()
        with self.subTest('resconnect'):
            self.assertTrue(app.isConnected())

        with self.subTest('reconnected_thread_is_present'):
            self.assertTrue(app.thread is not None)

        with self.subTest('reconnected_thread_is_alive'):
            self.assertTrue(app.thread is not None and app.thread.is_alive())

        with self.subTest('still_registered'):
            self.assertIn(app.conn_info, self.connection_manager.registered_connections)

    def test_registered_connections(self):
        """ Test the property 'registered_connections'. """
        print(f"\nRunning test method {self._testMethodName}\n")

        with self.subTest('registered_connections_is_empty'):
            self.assertEqual(0, len(self.connection_manager.registered_connections))
            
        ord_app = ibk.orders.OrdersApp()
        self.connection_manager.establish_new_connection(ord_app, clientId=234235223)

        with self.subTest('registered_connections_has_len_1'):
            self.assertEqual(1, len(self.connection_manager.registered_connections))

        ct_app = ibk.contracts.ContractsApp()
        self.connection_manager.establish_new_connection(ct_app, clientId=99232352)
            
        with self.subTest('registered_connections_has_len_2'):
            self.assertEqual(2, len(self.connection_manager.registered_connections))

        with self.subTest('OrdersApp is registered'):
            self.assertIn(ord_app.conn_info, self.connection_manager.registered_connections)

        for app in [ord_app, ct_app]:
            app_name = app.__class__.__name__
            with self.subTest(f'{app_name} is registered'):
                self.assertIn(app.conn_info, self.connection_manager.registered_connections)

            with self.subTest(f'{app_name} is found'):
                self.assertIs(app, self.connection_manager.registered_connections[app.conn_info])

    def test_deregister_connection(self):
        """ Test the method 'deregister_connection'. """
        print(f"\nRunning test method {self._testMethodName}\n")

        with self.subTest('no_connections'):
            self.assertEqual(0, len(self.connection_manager.registered_connections))

        app = ibk.marketdata.MarketDataApp()
        self.connection_manager.establish_new_connection(app, clientId=234276953)

        with self.subTest('registered'):
            self.assertEqual(1, len(self.connection_manager.registered_connections))

        self.connection_manager.deregister_connection(app)
        with self.subTest('deregistered'):
            self.assertEqual(0, len(self.connection_manager.registered_connections))
            
        # Cleanup - we must manually disconnect the application now that it is deregistered.
        app.disconnect()
        with self.subTest('disconnected'):
            self.assertFalse(app.isConnected())

    def test_find_existing_connection(self):
        """ Test the method 'find_existing_connection'. """
        print(f"\nRunning test method {self._testMethodName}\n")

        clientId = 1234
        class_handle = ibk.marketdata.MarketDataApp
        app = class_handle()
        self.connection_manager.establish_new_connection(app, clientId=clientId)

        with self.subTest('find_with_known_clientId'):
            found = self.connection_manager.find_existing_connection(class_handle, clientId=clientId)
            self.assertIs(app, found)
 
        with self.subTest('find_with_unknown_clientId'):
            found = self.connection_manager.find_existing_connection(class_handle, clientId=None)
            self.assertIs(app, found)

    def test_base_connect(self):
        """ Test the method 'connect' in base.py. """
        print(f"\nRunning test method {self._testMethodName}\n")

        app = ibk.orders.OrdersApp()
        app.connect(port=self.port, clientId=234276953)

        with self.subTest('connected'):
            self.assertTrue(app.isConnected())

        with self.subTest('thread_is_present'):
            self.assertTrue(app.thread is not None)
            
        with self.subTest('thread_is_alive'):
            self.assertTrue(app.thread is not None and app.thread.is_alive())
            
        with self.subTest('conn_info_is_ConnectionInfo_instance'):
            self.assertIsInstance(app.conn_info, ibk.connect.ConnectionInfo)

        with self.subTest('registered'):
            self.assertIn(app.conn_info, self.connection_manager.registered_connections)

    def test_base_reconnect(self):
        """ Test the method 'reconnect' in base.py. """
        print(f"\nRunning test method {self._testMethodName}\n")

        app = ibk.account.AccountApp()
        app.connect(port=self.port, clientId=124125)
        with self.subTest('connected'):
            self.assertTrue(app.isConnected())
            
        with self.subTest('connected_thread_is_alive'):
            self.assertTrue(app.thread is not None and app.thread.is_alive())

        app.disconnect()
        with self.subTest('disconnected'):
            self.assertFalse(app.isConnected())            

        with self.subTest('disconnected_thread_is_none'):
            self.assertTrue(app.thread is None)

        app.reconnect()
        with self.subTest('reconnected'):
            self.assertTrue(app.isConnected())            
            
        with self.subTest('reconnected_thread_is_alive'):
            self.assertTrue(app.thread is not None and app.thread.is_alive())

        with self.subTest('still_registered'):
            self.assertIn(app.conn_info, self.connection_manager.registered_connections)

    def test_base_disconnect(self):
        """ Test the method 'disconnect' in base.py. """
        print(f"\nRunning test method {self._testMethodName}\n")

        app = ibk.contracts.ContractsApp()
        
        app.connect(port=self.port, clientId=124125)
        with self.subTest('connected'):
            self.assertTrue(app.isConnected())
            
        with self.subTest('connected_thread_is_alive'):
            self.assertTrue(app.thread is not None and app.thread.is_alive())

        app.disconnect()
        with self.subTest('disconnected'):
            self.assertFalse(app.isConnected())            

        with self.subTest('disconnected_thread_is_none'):
            self.assertTrue(app.thread is None)

        with self.subTest('still_registered'):
            self.assertIn(app.conn_info, self.connection_manager.registered_connections)


if __name__ == '__main__':
    unittest.main()