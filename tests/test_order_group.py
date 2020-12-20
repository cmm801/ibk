import time
import unittest
import sys, os
import collections

import pandas as pd

import ibapi

import constants
import master
import orders

# Create a namedtuple to simulate the properties of an Order object
MockOrder = collections.namedtuple('MockOrder', 
                                   ['orderId', 'action', 'totalQuantity', 'orderType'])


class MockApplication():
    """ Create a mock application to simulate placing an order.
    
        This is just so we can test that the internal logic
        of the SingleOrder and OrderGroup classes work properly.
    """
    def __init__(self, port):
        self.port = port
        
    def placeOrder(self, orderId, contract, order):
        pass


class OrderGroupTest(unittest.TestCase):

    def test_create_single_order(self):
        """ Test that we can create a SingleOrder object.
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')        
        single_order = orders.SingleOrder('contract_1', ord_1, mock_app)

        ctr = 0
        with self.subTest(i=ctr):        
            self.assertEqual('contract_1', single_order.contract, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(ord_1, single_order.order, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(orders.STATUS_NOT_PLACED, single_order.status, msg='Unexpected status.')

    def test_create_order_group(self):
        """ Test that we can create an OrderGroup object.
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = MockOrder(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')

        order_group = orders.OrderGroup(['ct_1', 'ct_2'], [ord_1, ord_2], app=mock_app)
        
        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(order_group, orders.OrderGroup, msg='Incorrect class instance.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(['ct_1', 'ct_2'], order_group.contracts, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1, ord_2], order_group.orders, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1.orderId, ord_2.orderId], order_group.order_ids,
                             msg='Order IDs mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(orders.STATUS_NOT_PLACED, order_group.status, msg='Unexpected status.')

    def test_combine_single_orders(self):
        """ Test that we can combine two SingleOrder objects.
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = MockOrder(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')        
        so_1 = orders.SingleOrder('ct_1', ord_1, mock_app)
        so_2 = orders.SingleOrder('ct_2', ord_2, mock_app)
        so_3 = so_1 + so_2

        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(so_3, orders.OrderGroup, msg='Incorrect class instance.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(['ct_1', 'ct_2'], so_3.contracts, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1, ord_2], so_3.orders, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1.orderId, ord_2.orderId], so_3.order_ids,
                             msg='Order IDs mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(orders.STATUS_NOT_PLACED, so_3.status, msg='Unexpected status.')

    def test_cast_single_order(self):
        """ Test that we can cast a SingleOrder object to an OrderGroup object.
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')        
        single_order = orders.SingleOrder('contract_1', ord_1, mock_app)
        order_group = single_order.to_group()

        ctr = 0
        with self.subTest(i=ctr):        
            self.assertEqual(['contract_1'], order_group.contracts, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual([ord_1], order_group.orders, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):        
            self.assertEqual(orders.STATUS_NOT_PLACED, order_group.status, msg='Unexpected status.')

    def test_single_order_place_order(self):
        """ Test that we can place an order for a SingleOrder object.
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        so_1 = orders.SingleOrder('ct_1', ord_1, mock_app)

        # Place the order
        so_1.place()

        # The two SingleOrder objects have different status flags,
        #    so when we combine them the status should be 'incompatible'
        self.assertEqual(orders.STATUS_PLACED, so_1.status, msg='Unexpected status.')

    def test_order_group_place_order(self):
        """ Test that we can place an order for a GroupOrder object.
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = MockOrder(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')        
        so_1 = orders.SingleOrder('ct_1', ord_1, mock_app)
        so_2 = orders.SingleOrder('ct_2', ord_2, mock_app)
        so_3 = so_1 + so_2

        # Place the order
        so_3.place()

        # The two SingleOrder objects have different status flags,
        #    so when we combine them the status should be 'incompatible'
        self.assertEqual(orders.STATUS_PLACED, so_3.status, msg='Unexpected status.')

    def test_incompatible_status(self):
        """ Test that we cannot combine objects with different "status" flags.
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        so_1 = orders.SingleOrder('ct_1', 'ord_1', mock_app)
        so_2 = orders.SingleOrder('ct_2', 'ord_2', mock_app)
        
        # Specify that the second order has been placed
        so_2.status = orders.STATUS_PLACED

        # Check that if the status flags are different, we cannot combine two orders into a GroupOrder
        self.assertRaises(ValueError, lambda : so_1 + so_2, 
                          msg='Should not be able to combine orders with different status.')

    def test_incompatible_status(self):
        """ Test that the status flag reveals differences in underlying SingleOrder objects.
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = MockOrder(orderId=6, action='SELL', totalQuantity=2, orderType='MKT')        
        so_1 = orders.SingleOrder('ct_1', ord_1, mock_app)
        so_2 = orders.SingleOrder('ct_2', ord_2, mock_app)
        so_3 = so_1 + so_2

        # Place the order
        so_3.single_orders[0].place()

        # The two SingleOrder objects have different status flags,
        #    so when we combine them the status should be 'incompatible'
        self.assertEqual(orders.STATUS_INCOMPATIBLE, so_3.status, msg='Unexpected status.')

    def test_order_group_from_single_orders(self):
        """ Test that we can create a GroupOrder from a SingleOrder object.
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=12, action='BUY', totalQuantity=1, orderType='MKT')
        single_order = orders.SingleOrder('ct_1', ord_1, mock_app)
        
        # Place the order
        single_order.place()

        # Create GroupOrder
        order_group = orders.OrderGroup.from_single_orders(single_order)

        ctr = 0
        with self.subTest(i=ctr):
            self.assertIsInstance(order_group, orders.OrderGroup, msg='Incorrect class instance.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(['ct_1'], order_group.contracts, msg='Contract mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual([ord_1], order_group.orders, msg='Order mismatch.')

        ctr += 1
        with self.subTest(i=ctr):
            self.assertEqual(orders.STATUS_PLACED, order_group.status, msg='Status mismatch.')

    def test_combining_objects(self):
        """ Test that we can combine SingleOrder and OrderGroup objects using "+".
        """
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=1, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = MockOrder(orderId=2, action='SELL', totalQuantity=2, orderType='MKT')
        ord_3 = MockOrder(orderId=3, action='BUY', totalQuantity=2, orderType='LMT')
        ord_4 = MockOrder(orderId=4, action='SELL', totalQuantity=5, orderType='LMT')
        so_1 = orders.SingleOrder('ct_1', ord_1, mock_app)
        so_2 = orders.SingleOrder('ct_2', ord_2, mock_app)
        so_3 = orders.SingleOrder('ct_3', ord_3, mock_app)
        so_4 = orders.SingleOrder('ct_4', ord_4, mock_app)

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
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=1, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = MockOrder(orderId=2, action='SELL', totalQuantity=2, orderType='MKT')
        ord_3 = MockOrder(orderId=3, action='BUY', totalQuantity=2, orderType='LMT')
        ord_4 = MockOrder(orderId=4, action='SELL', totalQuantity=5, orderType='LMT')
        so_1 = orders.SingleOrder('ct_1', ord_1, mock_app)
        so_2 = orders.SingleOrder('ct_2', ord_2, mock_app)
        so_3 = orders.SingleOrder('ct_3', ord_3, mock_app)
        so_4 = orders.SingleOrder('ct_4', ord_4, mock_app)

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
        mock_app = MockApplication(port=constants.PORT_PAPER)
        ord_1 = MockOrder(orderId=1, action='BUY', totalQuantity=1, orderType='MKT')
        ord_2 = MockOrder(orderId=2, action='SELL', totalQuantity=2, orderType='MKT')
        ord_3 = MockOrder(orderId=3, action='BUY', totalQuantity=2, orderType='LMT')
        ord_4 = MockOrder(orderId=4, action='SELL', totalQuantity=5, orderType='LMT')
        so_1 = orders.SingleOrder('ct_1', ord_1, mock_app)
        so_2 = orders.SingleOrder('ct_2', ord_2, mock_app)
        so_3 = orders.SingleOrder('ct_3', ord_3, mock_app)
        so_4 = orders.SingleOrder('ct_4', ord_4, mock_app)

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


if __name__ == '__main__':
    unittest.main()