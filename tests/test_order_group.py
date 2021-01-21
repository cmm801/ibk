import time
import unittest
import sys, os
import collections

import pandas as pd

import ibapi

import ibk.constants
import ibk.orders


class MockApplication():
    """ Create a mock application to simulate placing an order.
    
        This is just so we can test that the internal logic
        of the SingleOrder and OrderGroup classes work properly.
    """
    def __init__(self, port):
        self.port = port
        
    def placeOrder(self, orderId, contract, order):
        pass
        
    def cancelOrder(self, orderId):
        pass


class OrderGroupTest(unittest.TestCase):

    def test_create_single_order(self):
        """ Test that we can create a SingleOrder object.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='SPY')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        single_order = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)

        ctr = 0
        with self.subTest(i=ctr):        
            self.assertEqual(cnt_1, single_order.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(ord_1, single_order.order, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(ibk.orders.STATUS_NOT_PLACED, single_order.status, msg='Unexpected status.')

    def test_create_order_group_multiple(self):
        """ Test that we can create an OrderGroup object with multiple contracts/ibk.orders.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='SPY')
        cnt_2 = self._create_contract(conId=2, symbol='EWJ')        
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')

        order_group = ibk.orders.OrderGroup([cnt_1, cnt_2], [ord_1, ord_2], app=mock_app)
        
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(order_group, ibk.orders.OrderGroup, msg='Incorrect class instance.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual([cnt_1, cnt_2], order_group.contracts, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1, ord_2], order_group.orders, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1.orderId, ord_2.orderId], order_group.order_ids,
                             msg='Order IDs mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(ibk.orders.STATUS_NOT_PLACED, order_group.status, msg='Unexpected status.')

    def test_create_order_group_single(self):
        """ Test that we can create an OrderGroup object with a single contract/order.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='SPY')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        order_group = ibk.orders.OrderGroup(cnt_1, ord_1, app=mock_app)
        
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(order_group, ibk.orders.OrderGroup, msg='Incorrect class instance.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual([cnt_1], order_group.contracts, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1], order_group.orders, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1.orderId], order_group.order_ids,
                             msg='Order IDs mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(ibk.orders.STATUS_NOT_PLACED, order_group.status, msg='Unexpected status.')

    def test_combine_single_orders(self):
        """ Test that we can combine two SingleOrder objects.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='SPY')
        cnt_2 = self._create_contract(conId=2, symbol='EWW')        
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')        
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        so_2 = ibk.orders.SingleOrder(cnt_2, ord_2, mock_app)
        so_3 = so_1 + so_2

        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(so_3, ibk.orders.OrderGroup, msg='Incorrect class instance.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual([cnt_1, cnt_2], so_3.contracts, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1, ord_2], so_3.orders, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1.orderId, ord_2.orderId], so_3.order_ids,
                             msg='Order IDs mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(ibk.orders.STATUS_NOT_PLACED, so_3.status, msg='Unexpected status.')

    def test_cast_single_order(self):
        """ Test that we can cast a SingleOrder object to an OrderGroup object.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='SPY')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')        
        single_order = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        order_group = single_order.to_group()

        ctr = 0
        with self.subTest(i=ctr):        
            self.assertEqual([cnt_1], order_group.contracts, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1], order_group.orders, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(ibk.orders.STATUS_NOT_PLACED, order_group.status, msg='Unexpected status.')

    def test_single_order_place_order(self):
        """ Test that we can place an order for a SingleOrder object.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='ESM0')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)

        with self.subTest(i=0):
            self.assertEqual(ibk.orders.STATUS_NOT_PLACED, so_1.status, msg='Unexpected status.')

        with self.subTest(i=1):
            so_1.place()            
            self.assertEqual(ibk.orders.STATUS_PLACED, so_1.status, msg='Unexpected status.')

        with self.subTest(i=2):
            with self.assertRaises(ValueError):
                so_1.place()
            
    def test_order_group_place_order(self):
        """ Test that we can place an order for a GroupOrder object.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='GLD')
        cnt_2 = self._create_contract(conId=2, symbol='VXX')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')        
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        so_2 = ibk.orders.SingleOrder(cnt_2, ord_2, mock_app)
        group_order = so_1 + so_2

        with self.subTest(i=0):
            self.assertEqual(ibk.orders.STATUS_NOT_PLACED, group_order.status, msg='Unexpected status.')

        with self.subTest(i=1):
            group_order.place()
            self.assertEqual(ibk.orders.STATUS_PLACED, group_order.status, msg='Unexpected status.')

        with self.subTest(i=2):
            with self.assertRaises(ValueError):
                group_order.place()

    def test_single_order_cancel_order(self):
        """ Test that we can place an order for a SingleOrder object.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='EWJ')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        single_order = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)

        with self.subTest(i=0):
            self.assertEqual(ibk.orders.STATUS_NOT_PLACED, single_order.status, msg='Unexpected status.')

        with self.subTest(i=1):
            with self.assertRaises(ValueError):
                single_order.cancel()

        with self.subTest(i=2):
            single_order.place()            
            self.assertEqual(ibk.orders.STATUS_PLACED, single_order.status, msg='Unexpected status.')

        with self.subTest(i=3):
            single_order.cancel()            
            self.assertEqual(ibk.orders.STATUS_CANCELLED, single_order.status, msg='Unexpected status.')

        with self.subTest(i=4):
            with self.assertRaises(ValueError):
                single_order.cancel()

    def test_order_group_cancel_order(self):
        """ Test that we can place an order for a GroupOrder object.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='SPY')
        cnt_2 = self._create_contract(conId=2, symbol='EWJ')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        so_2 = ibk.orders.SingleOrder(cnt_2, ord_2, mock_app)
        group_order = so_1 + so_2

        with self.subTest(i=0):
            self.assertEqual(ibk.orders.STATUS_NOT_PLACED, group_order.status, msg='Unexpected status.')

        with self.subTest(i=1):
            with self.assertRaises(ValueError):
                group_order.cancel()

        with self.subTest(i=2):
            group_order.place()            
            self.assertEqual(ibk.orders.STATUS_PLACED, group_order.status, msg='Unexpected status.')

        with self.subTest(i=3):
            group_order.cancel()            
            self.assertEqual(ibk.orders.STATUS_CANCELLED, group_order.status, msg='Unexpected status.')

        with self.subTest(i=4):
            with self.assertRaises(ValueError):
                group_order.cancel()

    def test_incompatible_status(self):
        """ Test that we cannot combine objects with different "status" flags.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=1, symbol='SPY')
        cnt_2 = self._create_contract(conId=2, symbol='EWJ')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        so_2 = ibk.orders.SingleOrder(cnt_2, ord_2, mock_app)
        
        # Specify that the second order has been placed
        so_2.status = ibk.orders.STATUS_PLACED

        # Check that if the status flags are different, we cannot combine two orders into a GroupOrder
        with self.assertRaises(ValueError):
            so_1 + so_2

    def test_incompatible_status(self):
        """ Test that the status flag reveals differences in underlying SingleOrder objects.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=12, symbol='SPY')
        cnt_2 = self._create_contract(conId=22, symbol='EWJ')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')        
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        so_2 = ibk.orders.SingleOrder(cnt_2, ord_2, mock_app)
        so_3 = so_1 + so_2

        # Place the order
        so_3.single_orders[0].place()

        # The two SingleOrder objects have different status flags,
        #    so when we combine them the status should be 'incompatible'
        self.assertEqual(ibk.orders.STATUS_INCOMPATIBLE, so_3.status, msg='Unexpected status.')

    def test_order_group_from_single_orders(self):
        """ Test that we can create a GroupOrder from a SingleOrder object.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=123, symbol='SPY')
        ord_1 = self._create_order(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        single_order = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        
        # Place the order
        single_order.place()

        # Create GroupOrder
        order_group = ibk.orders.OrderGroup.from_single_orders(single_order)

        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(order_group, ibk.orders.OrderGroup, msg='Incorrect class instance.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual([cnt_1], order_group.contracts, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual([ord_1], order_group.orders, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(ibk.orders.STATUS_PLACED, order_group.status, msg='Status mismatch.')

    def test_combining_objects(self):
        """ Test that we can combine SingleOrder and OrderGroup objects using "+".
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=12, symbol='SPY')
        cnt_2 = self._create_contract(conId=22, symbol='EWJ')
        cnt_3 = self._create_contract(conId=1221, symbol='GLD')
        cnt_4 = self._create_contract(conId=22166, symbol='VXX')        
        ord_1 = self._create_order(orderId=1, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=2, action='SELL', totalQuantity=2, orderType='MKT')
        ord_3 = self._create_order(orderId=3, action='BUY', totalQuantity=2, orderType='LMT')
        ord_4 = self._create_order(orderId=4, action='SELL', totalQuantity=5, orderType='LMT')
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        so_2 = ibk.orders.SingleOrder(cnt_2, ord_2, mock_app)
        so_3 = ibk.orders.SingleOrder(cnt_3, ord_3, mock_app)
        so_4 = ibk.orders.SingleOrder(cnt_4, ord_4, mock_app)

        ctr = 0
        with self.subTest(i=ctr):
            self.assertEqual((so_1 + so_2).order_ids, [1, 2], 
                             msg='Adding two SingleObjects.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(((so_1 + so_2) + so_3).order_ids, [1, 2, 3],
                             msg='Adding OrderGroup to SingleOrder.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual((so_1 + (so_2 + so_3)).order_ids, [1, 2, 3],
                             msg='Adding SingleOrder to OrderGroup.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(((so_1 + so_2) + (so_3 + so_4)).order_ids, [1, 2, 3, 4],
                             msg='Adding two OrderGroup objects.')

    def test_order_group_add(self):
        """ Test the "add" method on OrderGroup.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=12, symbol='SPY')
        cnt_2 = self._create_contract(conId=22, symbol='EWJ')
        cnt_3 = self._create_contract(conId=1221, symbol='GLD')
        cnt_4 = self._create_contract(conId=22166, symbol='VXX')
        ord_1 = self._create_order(orderId=1, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=2, action='SELL', totalQuantity=2, orderType='MKT')
        ord_3 = self._create_order(orderId=3, action='BUY', totalQuantity=2, orderType='LMT')
        ord_4 = self._create_order(orderId=4, action='SELL', totalQuantity=5, orderType='LMT')
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        so_2 = ibk.orders.SingleOrder(cnt_2, ord_2, mock_app)
        so_3 = ibk.orders.SingleOrder(cnt_3, ord_3, mock_app)
        so_4 = ibk.orders.SingleOrder(cnt_4, ord_4, mock_app)

        # Create a group from the first order
        order_group = so_1.to_group()
    
        ctr = 0
        with self.subTest(i=ctr):
            order_group.add(so_2)
            self.assertEqual(order_group.order_ids, [1, 2], msg='Unexpected order_ids.')

        ctr += 1
        with self.subTest(i=ctr):
            order_group.add(so_3)
            self.assertEqual(order_group.order_ids, [1, 2, 3], msg='Unexpected order_ids.')

    def test_unique_order_ids(self):
        """ Test that we cannot create an OrderGroup with repeated order ids.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=12, symbol='SPY')
        cnt_2 = self._create_contract(conId=22, symbol='EWJ')
        cnt_3 = self._create_contract(conId=1221, symbol='GLD')
        cnt_4 = self._create_contract(conId=22166, symbol='VXX')
        ord_1 = self._create_order(orderId=1, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=2, action='SELL', totalQuantity=2, orderType='MKT')
        ord_3 = self._create_order(orderId=3, action='BUY', totalQuantity=2, orderType='LMT')
        ord_4 = self._create_order(orderId=4, action='SELL', totalQuantity=5, orderType='LMT')
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        so_2 = ibk.orders.SingleOrder(cnt_2, ord_2, mock_app)
        so_3 = ibk.orders.SingleOrder(cnt_3, ord_3, mock_app)
        so_4 = ibk.orders.SingleOrder(cnt_4, ord_4, mock_app)

        # Create a group from the first order
        order_group = (so_1 + so_2) + so_3
    
        with self.subTest(i=0):
            self.assertRaises(ValueError, lambda : order_group + so_1)

        with self.subTest(i=1):
            self.assertRaises(ValueError, lambda : order_group + so_2)

        with self.subTest(i=2):
            self.assertRaises(ValueError, lambda : order_group + so_3)

        with self.subTest(i=3):
            self.assertEqual([1, 2, 3, 4], (order_group + so_4).order_ids,
                            msg='Mismatch in order_ids.')

    def test_single_order_are_equal(self):
        """ Test that we compare two SingleOrder objects with '=='.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=12121, symbol='AAPL')
        cnt_2 = self._create_contract(conId=2124142, symbol='IBM')
        ord_1 = self._create_order(orderId=1, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=2, action='SELL', totalQuantity=2, orderType='LMT')
        ord_3 = self._create_order(orderId=2, action='SELL', totalQuantity=2, orderType='LMT')
        so_1 = ibk.orders.SingleOrder(cnt_1, ord_1, mock_app)
        so_2 = ibk.orders.SingleOrder(cnt_2, ord_2, mock_app)
        so_3 = ibk.orders.SingleOrder(cnt_2, ord_3, mock_app)
        so_4 = ibk.orders.SingleOrder(cnt_2, ord_1, mock_app)

        with self.subTest(i=0):
            self.assertNotEqual(so_1, so_2)

        with self.subTest(i=1):
            self.assertEqual(so_2, so_3)

        with self.subTest(i=2):
            self.assertNotEqual(so_1, so_4)

        with self.subTest(i=3):
            self.assertNotEqual(so_2, so_4)

    def test_order_group_are_equal(self):
        """ Test that we compare two OrderGroup objects with '=='.
        """
        print(f"\nRunning test method {self._testMethodName}\n")

        mock_app = MockApplication(port=ibk.constants.PORT_PAPER)
        cnt_1 = self._create_contract(conId=12121, symbol='AAPL')
        cnt_2 = self._create_contract(conId=2124142, symbol='IBM')        
        ord_1 = self._create_order(orderId=1, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = self._create_order(orderId=2, action='SELL', totalQuantity=2, orderType='LMT')
        ord_3 = self._create_order(orderId=3, action='SELL', totalQuantity=2, orderType='LMT')

        single_0 = ibk.orders.SingleOrder(cnt_1, ord_2, app=mock_app)
        group_0 = ibk.orders.OrderGroup(cnt_1, ord_2, app=mock_app)
        group_1 = ibk.orders.OrderGroup([cnt_1, cnt_1], [ord_1, ord_2], app=mock_app)
        group_2 = ibk.orders.OrderGroup([cnt_1, cnt_1], [ord_1, ord_2], app=mock_app)
        group_3 = ibk.orders.OrderGroup([cnt_1, cnt_1], [ord_2, ord_1], app=mock_app)
        group_4 = ibk.orders.OrderGroup([cnt_2, cnt_1], [ord_2, ord_1], app=mock_app)
        group_5 = ibk.orders.OrderGroup([cnt_1, cnt_1, cnt_1], [ord_1, ord_2, ord_3], app=mock_app)

        with self.subTest(i=0):
            self.assertEqual(group_1, group_2)

        with self.subTest(i=1):
            self.assertNotEqual(group_2, group_3)

        with self.subTest(i=2):
            self.assertNotEqual(group_3, group_4)

        with self.subTest(i=3):
            self.assertNotEqual(group_2, group_4)

        with self.subTest(i=4):
            self.assertNotEqual(group_1, group_5)

        with self.subTest(i=4):
            self.assertNotEqual(group_0, single_0)

        with self.subTest(i=4):
            self.assertEqual(group_0, single_0.to_group())
            

    def _create_order(self, **kwargs):
        """ Method to help create Order objects with some populated variables.
        """
        # Create an Order object
        _order = ibapi.order.Order()

        # Set any additional specifications in the Order
        for key, val in kwargs.items():
            if not hasattr(_order, key):
                raise ValueError(f'Unsupported Order variable name was provided: {key}')
            elif val is None:                
                pass # keep the default values in this case
            else:
                _order.__setattr__(key, val)

        # Return the Order object
        return _order
    
    def _create_contract(self, **kwargs):
        """ Method to help create Contract objects with some populated variables.
        """
        # Create a Contract object
        _contract = ibapi.contract.Contract()

        # Set any additional specifications in the Contract
        for key, val in kwargs.items():
            if not hasattr(_contract, key):
                raise ValueError(f'Unsupported Contract variable name was provided: {key}')
            elif val is None:                
                pass # keep the default values in this case
            else:
                _contract.__setattr__(key, val)

        # Return the Order object
        return _contract
    
if __name__ == '__main__':
    unittest.main()