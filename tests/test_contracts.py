import unittest
import sys, os

import pandas as pd

import ibapi

import constants
import master


class ContractsTest(unittest.TestCase):
    def setUp(self):
        """ Perform any required set-up before each method call. """
        pass
        
    def tearDown(self):
        """ Remove anything from 'setUp' after each method call. """
        pass

    @classmethod
    def setUpClass(cls):
        """ Perform any required set-up once, before any method is run. 
        
            This method should be used to build any classes or data structures
            that will be used by more than one of the test methods, and
            that cannot be built quickly on-the-fly.
        """
        PORT = constants.PORT_PAPER

        # After execution, TWS will prompt you to accept the connection
        # The ERROR simply confirms that there is a connection to the market data.
        cls.app = master.Master(port=PORT)

    @classmethod
    def tearDownClass(cls):
        """ Perform any required tear-down once, after all methods have been run. 
            
            This method can be used to destroy any structures created in setUpClass.
        """
        cls.app.disconnect()
        del cls.app

    def test_get_contract_for_index(self):
        """ Check that we can retrieve a contract.
        """
        _contract = self.app.get_contract('SPX')
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIsInstance(_contract, ibapi.contract.Contract,
                msg="The contract is not of type Contract.")
            
        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(_contract.exchange, 'CBOE',
                msg="The contract's exchange is not expected.")

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(_contract.secType, 'IND',
                msg="The contract's security type is not expected.")

    def test_get_contract_for_stock(self):
        """ Check that we can retrieve a contract.
        """
        _contract = self.app.get_contract('AAPL')
        ctr = 0
        with self.subTest(i=ctr):        
            self.assertIsInstance(_contract, ibapi.contract.Contract,
                msg="The contract is not of type Contract.")
            
        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(_contract.symbol, 'AAPL',
                msg="The contract's symbol is not expected.")

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(_contract.secType, 'STK',
                msg="The contract's security type is not expected.")

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(_contract.primaryExchange, 'NASDAQ',
                msg="The contract's primary exchange is not expected.")

    def test_find_matching_contracts_for_stock(self):
        """ Check that all retrieved instruments match the requirements.
        """
        # Get the ContractDetails for matching contracts
        contract_details = self.app.find_matching_contracts(symbol='AAPL', 
                                                            exchange='SMART', 
                                                            secType='STK')
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(contract_details, list, msg='Expected a list.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertIsInstance(contract_details[0], ibapi.contract.ContractDetails, 
                                  msg='Expected a ContractDetails object.')

        for ctdt in contract_details:
            ctr += 1
            with self.subTest(i=ctr):
                self.assertTrue('AAPL' == ctdt.contract.symbol, msg='Symbol mismatch.')

            ctr += 1
            with self.subTest(i=ctr):
                self.assertTrue('SMART' == ctdt.contract.exchange, msg='Exchange mismatch.')

            ctr += 1
            with self.subTest(i=ctr):
                self.assertTrue('STK' == ctdt.contract.secType, msg='Security type mismatch.')

    def test_find_best_matching_contract_for_stock(self):
        """ Check that the best matching Contract meets the requirements.
        """
        # Get the best Contract object
        _contract = self.app.find_best_matching_contract(symbol='IBM', 
                                                         exchange='SMART', 
                                                         secType='STK',
                                                         currency='USD')
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(_contract, ibapi.contract.Contract, 
                                  msg='Expected a Contract object.')
        ctr += 1
        with self.subTest(i=ctr):
            self.assertTrue('IBM' == _contract.symbol, msg='Symbol mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertTrue('SMART' == _contract.exchange, msg='Exchange mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertTrue('STK' == _contract.secType, msg='Security type mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertTrue('USD' == _contract.currency, msg='Security type mismatch.')


if __name__ == '__main__':
    unittest.main()