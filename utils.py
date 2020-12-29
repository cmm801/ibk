import os
import constants

def strip_trailing_whitespace_from_files():
    """ Remove trailing whitespace from all files.
    """
    for path, dirs, files in os.walk(constants.IB_PATH):
        for f in files:
            file_name, file_extension = os.path.splitext(f)
            if file_extension == '.py':
                path_name = os.path.join(path, f)
                with open(path_name, 'r') as fh:
                    new = [line.rstrip() for line in fh]
                with open(path_name, 'w') as fh:
                    [fh.write('%s\n' % line) for line in new]

def update_contract_details_for_stocks(app):
    """ Update the saved contract details to make the contracts still exist.
    """
    ctapp = app.contracts_app

    ctr = 0
    delisted = []
    for tkr, saved_ctd in ctapp._saved_contract_details.items():
        ctr += 1

        if ctr % 100 == 0:
            print(ctr, tkr)

        if saved_ctd.contract.secType != 'STK':
            continue

        # Check that a contract still exists for this
        ctd = ctapp.find_best_matching_contract_details(symbol=saved_ctd.contract.symbol, 
                                                        secType='STK',
                                                        currency=saved_ctd.contract.currency)

        if ctd is None:
            # The contract can no longer be found, so we assume it is delisted / aquired
            delisted.append(tkr)

    # The new versions of the contracts should be cached, and now we save the cache to file
    app.save_contracts()
    
    # Return the delisted (or just no longer found) tickers
    return delisted

def download_fundamental_ratios_for_stocks(app):
    """ Download the fundamental ratio data for stocks.
    """
    mdapp = app.marketdata_app
    missing_contracts = []

    ctr = 0
    fund_info = []
    fund_info_map = {}
    delisted = []    
    for tkr, saved_ctd in app.contracts_app._saved_contract_details.items():
        if saved_ctd.secType != 'STK':
            continue

        if tkr not in fund_info_map and tkr not in saved_df.index and tkr not in missing:
            # Check that a contract still exists for this
            ctd = app.contracts_app.find_best_matching_contract_details(conId=saved_ctd.conId)

            if ctd is None:
                # The contract can no longer be found, so we assume it is delisted / aquired
                delisted.append(tkr)
            else:
                time.sleep(0.25)

                if ctr % 100 == 0:
                    print(ctr, tkr)
                ctr += 1

                reqObj = mdapp.create_fundamental_data_request([ctd.contract], 
                                                               report_type='ratios')[0]

                t0 = time.time()
                while len(mdapp.get_open_streams()) > 30:
                    time.sleep(0.5)

                    # disconnect, reconnect and then sleep 60 seconds if IB is not responding
                    if time.time() - t0 < 60:
                        mdapp.disconnect()
                        mdapp = app.marketdata_app
                        t0 = time.time()

                # Try to reconnect if the application has disconnected from the server
                if not mdapp.isConnected():
                    mdapp.disconnect()
                    mdapp = app.marketdata_app

                # Place the request (now that we know we are connected to the server)
                reqObj.place_request()

                # Append the results
                fund_info.append(reqObj)

                
    # Combine the data into a DataFrame
    for reqObj in fund_info:
        pdata = reqObj.get_data()
        if len(pdata):
            fund_info_map[reqObj.contract.localSymbol] = pdata

    # Create a DataFrame
    df = pd.concat(list(fund_info_map.values()), axis=1)    
    
    return df, delisted