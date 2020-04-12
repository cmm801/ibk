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

import time
import threading
import json
import datetime

import ibapi

from base import BaseApp
import helper

__CLIENT_ID = 3     # Reserve client id 0 for the main application
MAX_WAIT_TIME = 10  # time in seconds. Large requests are slow

class ContractsApp(BaseApp):
    """Main program class. The TWS calls nextValidId after connection, so
    the method is over-ridden to provide an entry point into the program.

    class variables:
    _saved_contracts (dict): keys are symbols, values are dictionaries of
        information to uniquely define a contract used for trading.
    """
    def __init__(self):
        super().__init__()
        self._saved_partial_contracts = dict()
        self._saved_contracts = dict()        
        self._contract_details = {}
        
        # Load the saved contracts
        self._load_contracts('contract_file.json')
    
    def get_contract(self, localSymbol: str):
        """Try to find a saved contract with the specified localSymbol.
        Arguments:
            localSymbol (str): a string representing the (unique) local
                            symbol associated with an instrument/contract. 

        Returns: (Contract) Matching contract, or None if no match.
        """        
        if localSymbol in self._saved_contracts:
            return self._saved_contracts[localSymbol]
        else:
            return None
    
    def match_contract(self, partial_contract, max_wait_time=MAX_WAIT_TIME):
        """Find the matching contract given a partial contract.

        Arguments:
            partial_contract (Contract): a Contract object with some of
                                                the fields specified
            max_wait_time (int): the maximum time (in seconds) to wait 
                        for a response from the IB API

        Returns: (Contract) Matching contract, or None if no match.
        """
        # If the contract has not already been saved, look it up.
        key = str(partial_contract)
        if key not in self._saved_partial_contracts:
            self.get_all_matching_contracts(partial_contract, 
                                            max_wait_time=max_wait_time)            

            # If there are multiple matches, select the desired contract
            ct = self._select_contract(partial_contract)
            if ct is None:
                s = partial_contract.symbol
                raise ValueError('Partial contract has no matches for symbol: {}'.format(s))
            else:
                # Cache the results
                self._saved_partial_contracts[key] = ct
                self._saved_contracts[ct.localSymbol] = ct
        
        # Return the cached contract
        return self._saved_partial_contracts[key]

    def contractDetails(self, reqId:int, contract_details):
        """Callback from reqContractDetails.
        """
        super().contractDetails(reqId, contract_details)

        # Add all contracts to the to a list that the calling function can access.
        self._contract_details.append(contract_details.contract)

    def _select_contract(self, contract):
        if 'STK' == contract.secType:
            return self._select_equity_contract(contract)
        elif 'FUT' == contract.secType:
            return self._select_futures_contract(contract)
        elif 'OPT' == contract.secType:
            return self._select_options_contract(contract)
        elif 'IND' == contract.secType:
            return self._select_index_contract(contract)
        elif 'CASH' == contract.secType:
            return self._select_forex_contract(contract)
        elif 'BOND' == contract.secType:
            return self._select_bond_contract(contract)
        elif 'CMDTY' == contract.secType:
            return self._select_commodity_contract(contract)
        elif 'FUND' == contract.secType:
            return self._select_mutual_fund_contract(contract)
        elif 'FOP' == contract.secType:
            return self._select_futures_option_contract(contract)
        else:
            raise ValueError('Invalid secType: {}'.format(contract.secType))       
    
    def _select_equity_contract(self, target_contract):
        # Select the proper contract
        for contract in self._contract_details:
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

    def _filter_contracts(self, contract_list, target_contract, filter_type='third_friday'):
        """Filter a list of contracts by a particular condition."""
        if 'third_friday' == filter_type:
            expiry_string = target_contract.lastTradeDateOrContractMonth
            if len(expiry_string) == 6:
                # Get the expiration year/month from the expiry string
                expiry_ym = datetime.datetime.strptime('202004', "%Y%m")
                third_friday = helper.get_third_friday(expiry_ym.year, expiry_ym.month)
                expiry_date = datetime.datetime.strftime( third_friday, '%Y%m%d')
            elif len(expiry_string) == 8:
                expiry_date = expiry_string
            else:
                raise ValueError('Unsupported length of lastTradeDateOrContractMonth.')
            return [x for x in contract_list if x.lastTradeDateOrContractMonth == expiry_date]
        else:
            raise ValueError('Unsupported filter type: {}'.format(filter_type))
        
    def _select_futures_contract(self, target_contract):
        """Select the desired futures contract in case there are multiple matches."""
        matching_contracts = self._filter_contracts(self._contract_details, 
                                target_contract, filter_type='third_friday')
        if not matching_contracts:
            return None
        elif len(matching_contracts) == 1:
            return matching_contracts[0]
        else:
            raise ValueError('Multiple matching contracts - the search must be more specific.')
        
    def _select_options_contract(self, target_contract):
        """Select the desired options contract in case there are multiple matches."""
        matching_contracts = self._filter_contracts(self._contract_details, 
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
        
    def _select_forex_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise ValueError('Multiple matching contracts - the search must be more specific.')
        
    def _select_index_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')
        
    def _select_bond_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')
        
    def _select_commodity_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')
        
    def _select_mutual_fund_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
        else:
            raise NotImplementedError('Multiple matches - needs better implementation.')

    def _select_futures_option_contract(self, target_contract):
        if not self._contract_details:
            return None
        elif len(self._contract_details) == 1:
            return self._contract_details[0]
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
                    self._saved_contracts[tkr] = self._get_contract_from_dict(info)
                    ct = ibapi.contract.Contract()                    
                    for key, val in info.items():
                        if key != 'conId':
                            ct.__setattr__(key, val)
        except FileNotFoundError:
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
            contents = { k: v.__dict__ for k, v in self._saved_contracts.items()}
            json.dump(contents, file_obj)

    def add_to_saved_contracts(self, contractList):
        for contract in contractList:
            self._saved_contracts[contract.localSymbol] = contract
        self._save_contracts()
        
    def get_all_matching_contracts(self, partial_contract, max_wait_time=MAX_WAIT_TIME):
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
        self._contract_details = []

        # The IB server will call contractDetails upon completion.
        req_id = self._get_next_req_id()
        self.reqContractDetails(req_id, partial_contract)

        # Loop until the server has completed the request.
        t0 = time.time()
        while not self._contract_details and time.time() - t0 < max_wait_time:
            time.sleep(0.2)    
        return self._contract_details

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

        
# Declare global variables used to handle the creation of a singleton class
__app = __port = __api_thread = None

def get_instance(port=7497):
    """Entry point into the program.

    Arguments:
    port (int): Port number that IBGateway, or TWS is listening.
    """
    global __app, __port, __api_thread
    if isinstance(__app, BaseApp) and __app.isConnected():
        if __port is None:
            return ValueError('Port information has been lost. Something has gone wrong.')
        elif __port != port:
            raise ValueError('Application is already open on another port.')
        else:
            # The connection is already open
            return __app
    else:
        try:
            __app = ContractsApp()
            __app.connect("127.0.0.1", port=port, clientId=__CLIENT_ID)
            print("serverVersion:%s connectionTime:%s" % (__app.serverVersion(),
                                                          __app.twsConnectionTime()))
            __api_thread = threading.Thread(target=__app.run)
            __api_thread.start()
            
            print('MarketDataApp connecting to IB...')
            while __app.req_id() is None:
                time.sleep(0.2)
            print('MarketDataApp connected.')
            
            __port = port
            return __app
        except KeyboardInterrupt:
            pass