import unittest
import sys, os

import pandas as pd

import ibapi

#ib_path = '/Users/chris/programming/finance/trading'
#sys.path.insert(0, ib_path)
#sys.path.insert(0, os.path.join(ib_path, 'interactivebrokers'))

#import interactivebrokers.constants as constants
#import interactivebrokers.master as master

import constants
import master


class SimpleTest(unittest.TestCase):
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
        del cls.app

    def test_get_total_account_value(self):
        """ Check that the account value can be obtained.
        """
        # Get the account value
        account_val = self.app.get_total_account_value()
        
        # Check that the account value is greater than 0
        self.assertGreater(account_val, 0.0,
                           msg="Total account value should be > 0.")

    def test_get_positions(self):
        """ Check that we can obtain the current positions.
        """
        positions_df, contracts = self.app.get_positions()
        
        # Run sub-tests, using a counter
        ctr = -1

        # Check that the positions information is a DataFrame
        ctr += 1
        with self.subTest(i=ctr):
            self.assertIsInstance(positions_df, pd.DataFrame,
                      msg="The positions info should be a DataFrame.")

        # Check that some columns appear in the DataFrame
        cols = ['account', 'localSymbol', 'symbol', 'secType', 'size', 
                'cost', 'totCost', 'multiplier']
        for col in cols:
            ctr += 1            
            with self.subTest(i=ctr):
                self.assertIn(col, positions_df.columns,
                    msg = f'The position info does not include column: {col}')

        # Check that all contracts are of type Contract
        for c in contracts:
            ctr += 1            
            with self.subTest(i=ctr):
                self.assertIsInstance(c, ibapi.contract.Contract,
                    msg="The contract is notof type Contract.")

        # Check that the index of the positions DataFrame is the localSymbol
        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual('localSymbol', positions_df.index.name,
                msg='The index should be the "localSymbol" information.')

                
        #with self.assertRaises(ValueError):
        #    self.stratObj.initial_positions = [1, 2, 3]


        #with self.subTest(i=k):
        #    self.assertAlmostEqual(correct_final_pos[k], positions.values[-1, k], 
        #                           places=places, msg='Positions are not as expected.')
        #    self.assertAlmostEqual(correct_final_perf[k], performance.values[-1, k],
        #                           places=places, msg='Performance is not as expected.')

if __name__ == '__main__':
    unittest.main()