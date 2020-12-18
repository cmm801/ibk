"""
Module to facilitate getting data through Interactive Brokers's API

Starting point was Brent Maranzano's GitHub repo:
see: https://interactivebrokers.github.io/tws-api/index.html

Classes
    IBClient (EClient): Creates a socket to TWS or IBGateway, and handles
        sending commands to IB through the socket.
    IBWrapper (EWrapper): Hanldes the incoming data from IB. Many of these
        methods are callbacks from the request commands.
    IBApp (IBWrapper, IBClilent): This provides the main functionality. Many
        of the methods are over-rides of the IBWrapper commands to customize
        the functionality.
"""
import os
import time
import json
import datetime

import ibapi
import base
import helper
import constants


__CLIENT_ID = 127
MAX_WAIT_TIME = 10  # time in seconds. Large requests are slow

# Declare the path to the file containing saved contract information
FILENAME_CONTRACTS = os.path.join(constants.IB_PATH, 'contract_file.json')


class ContractsApp(base.BaseApp):
    """Main program class. The TWS calls nextValidId after connection, so
    the method is over-ridden to provide an entry point into the program.

    class variables:
    __saved_contracts (dict): keys are symbols, values are dictionaries of
                information to uniquely define a contract used for trading.
    """
    def __init__(self):
        super().__init__()
        self.__saved_partial_contracts = dict()
        self.__saved_contracts = dict()
        self.__contract_details = dict()
        self.__contract_details_request_complete = dict()
        self.__market_rule_info = dict()

        # Load the saved contracts
        self._load_contracts(FILENAME_CONTRACTS)

    def get_contract_details(self, partial_contract, max_wait_time=None):
        """Find all matching contracts given a partial contract.
        Upon execution of IB backend, the EWrapper.reqContractDetails is called,
        which is over-ridden to save the contracts to a class dictionary.
        This function then monitors the class dictionary until
        the contract is found and then returns the contract.

        Arguments:
            partial_contract (Contract): a Contract object with some of
                                                the fields specified

        Returns: (list) Matching contract(s).
        """
        if max_wait_time is None:
            max_wait_time = MAX_WAIT_TIME
        
        # Get the next request ID and initialize data structures to collect the results
        req_id = self._get_next_req_id()
        self.__contract_details[req_id] = []
        self.__contract_details_request_complete[req_id] = False

        # Call EWrapper.reqContractDetails to get all partially matching contracts
        self.reqContractDetails(req_id, partial_contract)

        # Loop until the server has completed the request.
        t0 = time.time()
        while not self.__contract_details_request_complete[req_id] and time.time() - t0 < max_wait_time:
            time.sleep(0.2)
        return self.__contract_details[req_id]

    def get_contract(self, localSymbol: str):
        """Try to find a saved contract with the specified localSymbol.
        Arguments:
            localSymbol (str): a string representing the (unique) local
                            symbol associated with an instrument/contract.

        Returns: (Contract) Matching contract, or None if no match.
        """
        if self.is_saved_contract(localSymbol):
            return self.__saved_contracts[localSymbol]
        else:
            return None

    def is_saved_contract(self, localSymbol):
        return localSymbol in self.__saved_contracts
        
    def add_to_saved_contracts(self, contractList):
        for contract in contractList:
            self.__saved_contracts[contract.localSymbol] = contract
        self._save_contracts()

    def find_matching_contracts(self, max_wait_time=None, **kwargs):
        """Find a list of matching contracts given some desired attributes.

        Arguments:
            max_wait_time (int): the maximum time (in seconds) to wait
                        for a response from the IB API
            kwargs: The key/value pairs of variables that appear in the
                ibapi.contract.Contract class. The user can specify
                as many or as few of these as desired.

        Returns: (list) a list of ContractDetails objects - one for each
            possible matching contract.
        """
        # Create a partially complete Contract object
        partial_contract = self._create_partial_contract(**kwargs)
        
        # Get the details of the matching contracts
        contract_details = self.get_contract_details(partial_contract,
                                        max_wait_time=max_wait_time)

        # Return the matching contract details
        return contract_details

    def find_best_matching_contract(self, max_wait_time=None, **kwargs):
        """Find 'best' contract among possibilities matching desired attributes.

        Arguments:
            max_wait_time (int): the maximum time (in seconds) to wait
                        for a response from the IB API
            kwargs: The key/value pairs of variables that appear in the
                ibapi.contract.Contract class. The user can specify
                as many or as few of these as desired.

        Returns: (Contract) the 'best' matching Contract object.
        """
        # Create a partially complete Contract object
        partial_contract = self._create_partial_contract(**kwargs)

        # Find all contracts matching our partial-specified contract
        contract_details = self.find_matching_contracts(max_wait_time=max_wait_time, 
                                                        **kwargs)

        # If there are multiple matches, select the desired contract
        possible_contracts = [x.contract for x in contract_details]
        ct = self._select_contract(partial_contract, possible_contracts)
        if ct is None:
            s = partial_contract.symbol
            raise ValueError('Partial contract has no matches for symbol: {}'.format(s))
        else:
            # Cache the results
            self.__saved_contracts[ct.localSymbol] = ct
            return ct

    def get_market_rule_info(self, rule_ids, max_wait_time=None):
        """Get market rule information based on rule ids.

           Arguments:
           rule_ids is a list of integers, representing different rule Ids.
           max_wait_time is the max time (in seconds) to wait for a response before timing out.
           """
        if max_wait_time is None:
            max_wait_time = MAX_WAIT_TIME

        for rid in set(rule_ids):
            assert isinstance(rid, int), 'Market rule ids must be integers.'
            if rid not in self.__market_rule_info:
                self.reqMarketRule(rid)

        is_completed = lambda : all([x in self.__market_rule_info for x in set(rule_ids)])
        t0 = time.time()
        while not is_completed() and time.time() - t0 < max_wait_time:
            time.sleep(0.2)
        if is_completed():
            return [self.__market_rule_info[x] for x in rule_ids]
        else:
            raise ValueError('Request has failed.')

    def contractDetails(self, reqId, contractDetailsObject):
        """Callback from reqContractDetails for non-bond contracts."""
        super().contractDetails(reqId, contractDetailsObject)
        self.__contract_details[reqId].append(contractDetailsObject)

    def bondContractDetails(self, reqId, contractDetailsObject):
        """Callback from reqContractDetails, specifically for bond contracts."""
        super().contractDetails(reqId, contractDetailsObject)
        self.__contract_details[reqId].append(contractDetailsObject)

    def contractDetailsEnd(self, reqId):
        super().contractDetailsEnd(reqId)
        self.__contract_details_request_complete[reqId] = True

    def marketRule(self, marketRuleId, priceIncrements):
        super().marketRule(marketRuleId, priceIncrements)
        self.__market_rule_info[marketRuleId] = priceIncrements

    def _create_partial_contract(self, **kwargs):
        """ Create a partial contract from key/value pairs. """
        # Create a contract using the user-provided information
        partial_contract = ibapi.contract.Contract()
        for key, val in kwargs.items():
            if key not in partial_contract.__dict__:
                raise ValueError(f'Unsupported Contract variable name was provided: {key}')
            else:
                partial_contract.__setattr__(key, val)   
        return partial_contract

    def _select_contract(self, contract, contract_details):
        if 'STK' == contract.secType:
            return self._select_equity_contract(contract, contract_details)
        elif 'FUT' == contract.secType:
            return self._select_futures_contract(contract, contract_details)
        elif 'OPT' == contract.secType:
            return self._select_options_contract(contract, contract_details)
        elif 'IND' == contract.secType:
            return self._select_index_contract(contract, contract_details)
        elif 'CASH' == contract.secType:
            return self._select_forex_contract(contract, contract_details)
        elif 'BOND' == contract.secType:
            return self._select_bond_contract(contract, contract_details)
        elif 'CMDTY' == contract.secType:
            return self._select_commodity_contract(contract, contract_details)
        elif 'FUND' == contract.secType:
            return self._select_mutual_fund_contract(contract, contract_details)
        elif 'FOP' == contract.secType:
            return self._select_futures_option_contract(contract, contract_details)
        else:
            raise ValueError('Invalid secType: {}'.format(contract.secType))

    def _select_equity_contract(self, target_contract, contract_details):
        # Select the proper contract
        for contract in contract_details:
            if target_contract.currency == 'USD':
                # NYSE stock
                if contract.primaryExchange in ["NYSE", 'ARCA', 'NASDAQ', 'BATS']:
                    return contract
                # Nasdaq stock
                elif contract.primaryExchange == "NASDAQ.NMS":
                    raise ValueError('This branch was created for unknown reasons on the github repo')
                    # Below is legacy code from the original github
                    #contract.primaryExchange = "ISLAND"
                    #return contract
            else:
                raise NotImplemtedError( 'Currently only supported for USD stocks.' )

    def _filter_derivative_contracts(self, contract_list, target_contract, filter_type='third_friday'):
        """Filter a list of contracts by a particular condition.
        """
        if 'third_friday' == filter_type:
            expiry_string = target_contract.lastTradeDateOrContractMonth
            if len(expiry_string) == 6:
                # Get the expiration year/month from the expiry string
                expiry_ym = datetime.datetime.strptime(expiry_string, "%Y%m")
                third_friday = helper.get_third_friday(expiry_ym.year, expiry_ym.month)
                expiry_date = datetime.datetime.strftime(third_friday, '%Y%m%d')
            elif len(expiry_string) == 8:
                expiry_date = expiry_string
            else:
                raise ValueError('Unsupported length of lastTradeDateOrContractMonth.')
            return [x for x in contract_list if x.lastTradeDateOrContractMonth == expiry_date]
        else:
            raise ValueError('Unsupported filter type: {}'.format(filter_type))

    def _select_futures_contract(self, target_contract, contract_details):
        """Select the desired futures contract in case there are multiple matches."""
        matching_contracts = self._filter_derivative_contracts(contract_details,
                                target_contract, filter_type='third_friday')
        if not matching_contracts:
            return None
        elif len(matching_contracts) == 1:
            return matching_contracts[0]
        else:
            raise ValueError('Multiple matching contracts - the search must be more specific.')

    def _select_options_contract(self, target_contract, contract_details):
        """Select the desired options contract in case there are multiple matches."""
        matching_contracts = self._filter_derivative_contracts(contract_details,
                                target_contract, filter_type='third_friday')
        if not matching_contracts:
            return None
        elif len(matching_contracts) == 1:
            return matching_contracts[0]
        else:
            smart_contracts = [x for x in matching_contracts
                                       if x.exchange == 'SMART']
            if not smart_contracts:
                return None
            elif len(smart_contracts) == 1:
                return smart_contracts[0]
            else:
                raise ValueError('Multiple matching contracts - the search must be more specific.')

    def _select_forex_contract(self, target_contract, contract_details):
        if not contract_details:
            return None
        elif len(contract_details) == 1:
            return contract_details[0]
        else:
            raise ValueError('Multiple matching contracts - the search must be more specific.')

    def _select_index_contract(self, target_contract, contract_details):
        if not contract_details:
            return None
        elif len(contract_details) == 1:
            return contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')

    def _select_bond_contract(self, target_contract, contract_details):
        if not contract_details:
            return None
        elif len(contract_details) == 1:
            return contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')

    def _select_commodity_contract(self, target_contract, contract_details):
        if not contract_details:
            return None
        elif len(contract_details) == 1:
            return contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')

    def _select_mutual_fund_contract(self, target_contract, contract_details):
        if not contract_details:
            return None
        elif len(contract_details) == 1:
            return contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')

    def _select_futures_option_contract(self, target_contract, contract_details):
        if not contract_details:
            return None
        elif len(contract_details) == 1:
            return contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')

    def _load_contracts(self, filename):
        """Load saved contracts.
        """
        try:
            with open(filename, mode='r') as file_obj:
                # Read in all saved contract info
                contract_info = json.load(file_obj)

                # Loop through the top-level keys, which are instrument tickers
                for tkr, info in contract_info.items():
                    # For each ticker, create the Contract object and save it
                    self.__saved_contracts[tkr] = self._get_contract_from_dict(info)
                    ct = ibapi.contract.Contract()
                    for key, val in info.items():
                        if key != 'conId':
                            ct.__setattr__(key, val)
        except FileNotFoundError:
            print('file not found')
            pass

    def _get_contract_from_dict(self, info):
        """Create a Contract object from a dictionary of keys/values."""
        _contract = ibapi.contract.Contract()
        for key, val in info.items():
            _contract.__setattr__(key, val)
        return _contract

    def _copy_contract(self, target_contract):
        """Create a copy of a Contract object"""
        ct_dict = target_contract.__dict__
        return self._get_contract_from_dict(ct_dict)

    def _save_contracts(self, file='contract_file.json', mode='w'):
        """Save contracts.
        """
        with open(file, mode=mode) as file_obj:
            contents = { k: v.__dict__ for k, v in self.__saved_contracts.items()}
            json.dump(contents, file_obj)

    def _clean_position_contracts(self, target_contract):
        """Make changes to contracts that are returned from get_positions in
           order to make them findable within IB's contract universe.
           """
        if 'CASH' == target_contract.secType:
            _contract = ibapi.contract.Contract()
            _contract.symbol = target_contract.symbol
            _contract.currency = target_contract.currency
            _contract.secType = 'CASH'
        else:
            _contract = self._copy_contract(target_contract)
            _contract.exchange = ''
        return _contract
