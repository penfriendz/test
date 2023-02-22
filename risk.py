import requests
import datetime
import json
from requests_negotiate_sspi import HttpNegotiateAuth

DIVIDEND_LIST = {} #{'939': 0.4259502} # 'stock code'
PORTFOLIO_MAPPING = {'SSO': ['AsiaMM-SSO-Technology-MM-ABNSG','AsiaMM-SSO-Technology-NonMM-ABNSG'], 'SSW': ['AsiaMM-SSO-Warrants-HKFE-MM-ABNSG'], 'SSH': ['AsiaMM-SSO-Technology-StockHedging-ABNSG'], 'SSA': ['AsiaMM-SSO-MMSSOAlphas-SEHK-MM-ABNSG','AsiaMM-SSO-RVSSOAlphas-SEHK-MM-ABNSG'],'SSB': ['AsiaMM-SSO-MMSSOAlphasSA-ABNSG','AsiaMM-SSO-MMIndexAlphasSA-ABNSG']}
SURFACES = ['ALB','MET','MIU','TCH','AIA','PAI','XCC','HKB','HEX','CNC','JDC','CPC','PEC','KST','BYD']
INDEX_SURFACES = ['HSCEI','HSI']
MEASURES = ['TheoreticalValue','Delta','SkewDelta','Gamma','SkewGamma','Theta','Vega','Rho','OptionCount','ParamVega/Ref Vol/1','ParamVega/Event Var/1','ParamVega/Skew/1','ParamVega/Pump/1','ParamVega/pWing/1','ParamVega/cWing/1','ParamVega/pStub/1','ParamVega/pTail/1','ParamVega/pFlip/1','ParamVega/cFlip/1','ParamVega/cTail/1','ParamVega/cStub/1']
CALCULATED_MEASURES = ['CashDelta','CashSkewDelta','CashSkewGamma','PLSkewGamma','FuturesDelta','PL','PLPos','PLDay','Fees','WeightedVega','WeightedRef Vol']
CURRENCY_MEASURES = ['CashDelta','CashSkewDelta','CashSkewGamma','PLSkewGamma','PL','PLPos','PLDay','Fees','WeightedVega','WeightedRef Vol']+['Theta','Vega','Rho','ParamVega/Ref Vol/1','ParamVega/Event Var/1','ParamVega/Skew/1','ParamVega/Pump/1','ParamVega/pWing/1','ParamVega/cWing/1','ParamVega/pStub/1','ParamVega/pTail/1','ParamVega/pFlip/1','ParamVega/cFlip/1','ParamVega/cTail/1','ParamVega/cStub/1']
SUMMARY_MEASURES = {'PL':'PL','PLPos':'PLPos','PLDay':'PLDay','Fees':'Fees','Delta':'Delta','CashDelta':'CashDelta','SkewDelta':'SkewDelta','CashSkewDelta':'CashSkewDelta','FuturesDelta':'FuturesDelta','CashSkewGamma':'CashSkewGamma','PLSkewGamma':'PLSkewGamma','Theta':'Theta','Vega':'Vega','WeightedVega':'WeightedVega','WeightedRef Vol':'WeightedRef Vol','Rho':'Rho','OptionCount':'OptionCount','ParamVega/Ref Vol/1':'Ref Vol','ParamVega/Event Var/1':'EventVar','ParamVega/Skew/1':'Skew','ParamVega/Pump/1':'Pump','ParamVega/pWing/1':'pWing','ParamVega/cWing/1':'cWing','ParamVega/pStub/1':'pStub','ParamVega/pTail/1':'pTail','ParamVega/pFlip/1':'pFlip','ParamVega/cFlip/1':'cFlip','ParamVega/cTail/1':'cTail','ParamVega/cStub/1':'cStub'}

OPTIONS_TIER = {'HSI':'HSI','HHI':'HHI','ALB': 1, 'TCH': 1, 'MET': 1, 'MIU': 1, 'AIA':1,'HKB':2,'PAI':1,'XCC':3,'CNC':3,'PEC':3,'HEX':1,'JDC':1,'CPC':3,'KST':1,'BYD':1}
FUTURES_TIER = {'HSI':'HSI','HHI':'HHI','ALB': 1, 'TCH': 'MM', 'MET': 1, 'MIU': 1,'AIA':1,'HKB':2,'PAI':1,'CCB':3}

OPTIONS_TIER_EXCHANGE_FEE_MM = {1: 1.6, 2: 0.9, 3: 0.5, 'HSI':2.54,'HHI':1.04}
FUTURES_TIER_EXCHANGE_FEE = {1: 3.1, 2: 1.1, 3: 0.6, 'HSI':4.04,'HHI':1.54,'MM':0.1}

OPTIONS_CLEARING_FEE_STOCK = 0.6
OPTIONS_CLEARING_FEE_INDEX = 0.65

FUTURES_CLEARING_FEE_STOCK = 0.6
FUTURES_CLEARING_FEE_INDEX = 0.65

PRODUCT_MAPPING = {'TCA': 'TCH','TCB':'TCH'}
TTX_WEIGHTING = 50/252
CURRENCY_RATE = 7.85

HOLIDAY_LIST = [
'2021-01-01','2021-02-12','2021-02-15','2021-04-02','2021-04-05','2021-04-06','2021-05-19','2021-06-14',
'2021-07-01','2021-09-22','2021-10-01','2021-10-14','2021-12-27','2022-02-01','2022-02-02','2022-02-03',
'2022-04-05','2022-04-15','2022-04-18','2022-05-02','2022-05-09','2022-06-03','2022-07-01','2022-09-12',
'2022-10-04','2022-12-26','2022-12-27','2023-01-02','2023-01-22','2023-01-23','2023-01-24','2023-01-25',
'2023-04-05','2023-04-07','2023-04-08','2023-04-10','2023-05-01','2023-05-26','2023-06-22','2023-07-01',
'2023-09-30','2023-10-02','2023-10-23','2023-12-24','2023-12-25','2023-12-26','2023-12-31'
]

sess = requests.Session()

print("Starting Risk")

yesterday_positions_cache = {} #these two caches assume you restart the script every night
instrument_list_yesterday_cache = {}
while True:
    time_now_utc = datetime.datetime.now(datetime.timezone.utc)
    valuedate = time_now_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    yesterday = time_now_utc - datetime.timedelta(days=1)
    positions_cache = {}
    trades_cache = {}
    pricedriver_cache = {}
    instrument_list_cache = {}
    while yesterday.weekday() > 4 or yesterday.date().strftime("%Y-%m-%d") in HOLIDAY_LIST: # Mon-Fri are 0-4
        yesterday -= datetime.timedelta(days=1)
    for portfolio_group in PORTFOLIO_MAPPING:
        risk_dict = {}
        total_dict = {}
        day_dict = {}
        for surface in (INDEX_SURFACES + sorted(SURFACES)):
            if surface in pricedriver_cache:
                pricedriver = pricedriver_cache[surface]
            else:
                pricedriver = sess.get("http://wheel-hk.mavensecurities.com/pricedriver/" + surface, headers={'accept': 'application/json'}).json()
                pricedriver_cache[surface] = pricedriver
            if surface in instrument_list_cache:
                instrument_list = instrument_list_cache[surface]
            else:
                instrument_list = sess.get("http://stash-hk.mavensecurities.com/user/albert/pricing/" + surface + "_live_measures.json", headers={'accept': 'application/json'}).json()
                instrument_list_cache[surface] = instrument_list
            if surface not in instrument_list_yesterday_cache:
                instrument_list_yesterday_request = sess.get("http://stash-hk.mavensecurities.com/user/albert/pricing/eod_" + surface + "/" + yesterday.strftime('%Y-%m-%d') + "_" + surface + ".json", headers={'accept': 'application/json'})
                if instrument_list_yesterday_request.status_code == 404:
                    instrument_list_yesterday = []
                    print("No yesterday pricing found for " + surface)
                else:
                    instrument_list_yesterday = instrument_list_yesterday_request.json()
                instrument_list_yesterday_cache[surface] = instrument_list_yesterday
            else:
                instrument_list_yesterday = instrument_list_yesterday_cache[surface]
            #try:
            if surface not in INDEX_SURFACES:
                spot_price = pricedriver['SpotPrice']
            else:
                spot_price = pricedriver['FuturesPrices'][pricedriver['SpecificFuture']]
            # except:
                # print("Pricedriver tripping up for surface " + surface + " at time " + valuedate)
                # continue
            for portfolio in PORTFOLIO_MAPPING[portfolio_group]:
                if portfolio not in positions_cache:
                    positions = sess.get("http://matadorfacade-hk.mavensecurities.com/instrumentpositions/" + portfolio, headers={'accept': 'application/json'}).json()
                    positions_cache[portfolio] = positions
                else:
                    positions = positions_cache[portfolio]
                if portfolio not in trades_cache:
                    trades = sess.get("http://matadorfacade-hk.mavensecurities.com/trades/" + portfolio + "/" + time_now_utc.strftime('%Y-%m-%d'), headers={'accept': 'application/json'}).json()
                    trades_cache[portfolio] = trades
                else:
                    trades = trades_cache[portfolio]
                if portfolio not in yesterday_positions_cache:
                    yesterday_positions_request = sess.get("http://matadorfacade-hk.mavensecurities.com/instrumentpositions/"+portfolio+"?to=" + yesterday.strftime('%Y-%m-%d'), headers={'accept': 'application/json'})
                    #yesterday_positions_request = sess.get("http://parameters-intraday-hk.mavensecurities.com/EOD/" + yesterday.strftime('%Y-%m-%d') + "/InstrumentPositions/" + portfolio, headers={'accept': 'application/json'})
                    if yesterday_positions_request.status_code == 404:
                        yesterday_positions = []
                        print("No yesterday_positions found for " + portfolio)
                    else:
                        yesterday_positions = yesterday_positions_request.json()
                    yesterday_positions_cache[portfolio] = yesterday_positions
                else:
                    yesterday_positions = yesterday_positions_cache[portfolio]
                #add brokergrid "http://brokergrid-hk.mavensecurities.com/riskpositions/hongkong"
                for instrument in instrument_list:
                    #if 'Position' not in instrument:
                    #    instrument['Position'] = 0
                    instrument_product_mapped = instrument['Product']
                    if instrument_product_mapped in PRODUCT_MAPPING:
                        instrument_product_mapped = PRODUCT_MAPPING[instrument_product_mapped]
                    if instrument['Id'] in positions:
                        #instrument['Position'] += positions[instrument['Id']]
                        risk_dict.setdefault(instrument['Expiry'],{})
                        risk_dict[instrument['Expiry']].setdefault(surface,{})
                        for measure in MEASURES:
                            risk_dict[instrument['Expiry']][surface].setdefault(measure,0)
                            risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                            if measure in ['OptionCount']:
                                risk_dict[instrument['Expiry']][surface][measure] += positions[instrument['Id']] * instrument[measure]
                            else:
                                risk_dict[instrument['Expiry']][surface][measure] += positions[instrument['Id']] * instrument['ContractSize'] * instrument[measure]
                        for measure in CALCULATED_MEASURES:
                            risk_dict[instrument['Expiry']][surface].setdefault(measure,0)
                            risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                            if measure in ['CashDelta']:
                                risk_dict[instrument['Expiry']][surface][measure] += positions[instrument['Id']] * instrument['ContractSize'] * instrument['Delta'] * spot_price
                            if measure in ['CashSkewDelta']:
                                risk_dict[instrument['Expiry']][surface][measure] += positions[instrument['Id']] * instrument['ContractSize'] * instrument['SkewDelta'] * spot_price
                            if measure in ['CashSkewGamma']:
                                risk_dict[instrument['Expiry']][surface][measure] += positions[instrument['Id']] * instrument['ContractSize'] * instrument['SkewGamma'] * spot_price * spot_price / 100
                            if measure in ['PLSkewGamma']:
                                risk_dict[instrument['Expiry']][surface][measure] += positions[instrument['Id']] * instrument['ContractSize'] * instrument['SkewGamma'] * (spot_price/100) * (spot_price/100) / 2
                            if measure in ['FuturesDelta']:
                                if instrument['Type'] == 'Future':
                                    risk_dict[instrument['Expiry']][surface][measure] += positions[instrument['Id']] * instrument['ContractSize'] * instrument['SkewDelta']
                            if measure in ['WeightedVega']:
                                if instrument['Type'] == 'Option':
                                    if instrument['TTX'] > 0:
                                        risk_dict[instrument['Expiry']][surface][measure] += positions[instrument['Id']] * instrument['ContractSize'] * instrument['Vega'] * (TTX_WEIGHTING / instrument['TTX'])**0.5
                            if measure in ['WeightedRef Vol']:
                                if instrument['Type'] == 'Option':
                                    if instrument['TTX'] > 0:
                                        risk_dict[instrument['Expiry']][surface][measure] += positions[instrument['Id']] * instrument['ContractSize'] * instrument['ParamVega/Ref Vol/1'] * (TTX_WEIGHTING / instrument['TTX'])**0.5

                    #PosPL
                    if instrument['Id'] in yesterday_positions:
                        instrument['YesterdayPosition'] = yesterday_positions[instrument['Id']]
                    else:
                        instrument['YesterdayPosition'] = 0
                    instrument['TotalTheoreticalValueToday'] = 0 
                    instrument['TotalTheoreticalValueYesterday'] = 0
                    if instrument['YesterdayPosition'] != 0:
                        risk_dict.setdefault(instrument['Expiry'],{})
                        risk_dict[instrument['Expiry']].setdefault(surface,{})
                        for measure in MEASURES:
                            risk_dict[instrument['Expiry']][surface].setdefault(measure,0)
                            risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                        for measure in CALCULATED_MEASURES:
                            risk_dict[instrument['Expiry']][surface].setdefault(measure,0)
                            risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                        instrument['TheoreticalValueYesterday'] = 0
                        instrument['ContractSizeYesterday'] = 0
                        for instrument_yesterday in instrument_list_yesterday:
                            if instrument['Id'] == instrument_yesterday['Id']:
                                instrument['TheoreticalValueYesterday'] = instrument_yesterday['TheoreticalValue']
                                instrument['ContractSizeYesterday'] = instrument_yesterday['ContractSize']
                        instrument['TotalTheoreticalValueToday'] = instrument['YesterdayPosition'] * instrument['TheoreticalValue'] * instrument['ContractSize']
                        if instrument['Type'] == 'Equity':
                            if instrument['ExchangeId'] in DIVIDEND_LIST:
                                instrument['TotalTheoreticalValueToday'] += instrument['YesterdayPosition'] * DIVIDEND_LIST[instrument['ExchangeId']] * instrument['ContractSize']
                        instrument['TotalTheoreticalValueYesterday'] = instrument['YesterdayPosition'] * instrument['TheoreticalValueYesterday'] * instrument['ContractSizeYesterday']
                        risk_dict[instrument['Expiry']][surface].setdefault('PLPos',0)
                        risk_dict[instrument['Expiry']][surface]['PLPos'] += instrument['TotalTheoreticalValueToday'] - instrument['TotalTheoreticalValueYesterday']

                    #DayPL
                    instrument['TotalTheoreticalValueDay'] = 0
                    instrument['TotalCostOfTrade'] = 0
                    instrument['TradedFlag'] = 0
                    instrument['Fees'] = 0
                    for trade in trades:
                        if instrument['Id'] == trade['Instrument']:
                            risk_dict.setdefault(instrument['Expiry'],{})
                            risk_dict[instrument['Expiry']].setdefault(surface,{})
                            instrument['TradedFlag'] = 1
                            instrument['TotalTheoreticalValueDay'] += trade['Quantity'] * instrument['TheoreticalValue'] * instrument['ContractSize']
                            instrument['TotalCostOfTrade'] += trade['Quantity'] * trade['Price'] * instrument['ContractSize']
                            for measure in MEASURES:
                                risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                                if measure in ['OptionCount']:
                                    risk_dict[instrument['Expiry']][surface]["Day"+measure] += trade['Quantity'] * instrument[measure]
                                else:
                                    risk_dict[instrument['Expiry']][surface]["Day"+measure] += trade['Quantity'] * instrument['ContractSize'] * instrument[measure]
                            for measure in CALCULATED_MEASURES:
                                risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                                if measure in ['CashDelta']:
                                    risk_dict[instrument['Expiry']][surface]["Day"+measure] += trade['Quantity'] * instrument['ContractSize'] * instrument['Delta'] * spot_price
                                if measure in ['CashSkewDelta']:
                                    risk_dict[instrument['Expiry']][surface]["Day"+measure] += trade['Quantity'] * instrument['ContractSize'] * instrument['SkewDelta'] * spot_price
                                if measure in ['CashSkewGamma']:
                                    risk_dict[instrument['Expiry']][surface]["Day"+measure] += trade['Quantity'] * instrument['ContractSize'] * instrument['SkewGamma'] * spot_price * spot_price / 100
                                if measure in ['PLSkewGamma']:
                                    risk_dict[instrument['Expiry']][surface]["Day"+measure] += trade['Quantity'] * instrument['ContractSize'] * instrument['SkewGamma'] * (spot_price/100) * (spot_price/100) / 2
                                if measure in ['FuturesDelta']:
                                    if instrument['Type'] == 'Future':
                                        risk_dict[instrument['Expiry']][surface]["Day"+measure] += trade['Quantity'] * instrument['ContractSize'] * instrument['SkewDelta']
                                if measure in ['WeightedVega']:
                                    if instrument['Type'] == 'Option':
                                        if instrument['TTX'] > 0:
                                            risk_dict[instrument['Expiry']][surface]["Day"+measure] += trade['Quantity'] * instrument['ContractSize'] * instrument['Vega'] * (TTX_WEIGHTING / instrument['TTX'])**0.5
                                if measure in ['WeightedRef Vol']:
                                    if instrument['Type'] == 'Option':
                                        if instrument['TTX'] > 0:
                                            risk_dict[instrument['Expiry']][surface]["Day"+measure] += trade['Quantity'] * instrument['ContractSize'] * instrument['ParamVega/Ref Vol/1'] * (TTX_WEIGHTING / instrument['TTX'])**0.5
                            if trade['EntryType'] not in ['Internal','CorporateActionMandatory','InternalTransfer']:
                                if trade['Broker'] != 'Internal':
                                    if instrument['Type'] == 'Future':
                                        if surface in INDEX_SURFACES:
                                            instrument['Fees'] += -(FUTURES_TIER_EXCHANGE_FEE[FUTURES_TIER[instrument_product_mapped]] + FUTURES_CLEARING_FEE_INDEX) * abs(trade['Quantity'])
                                        else:
                                            instrument['Fees'] += -(FUTURES_TIER_EXCHANGE_FEE[FUTURES_TIER[instrument_product_mapped]] + FUTURES_CLEARING_FEE_STOCK) * abs(trade['Quantity'])
                                    if instrument['Type'] == 'Option':
                                        if surface in INDEX_SURFACES:
                                            instrument['Fees'] += -(OPTIONS_TIER_EXCHANGE_FEE_MM[OPTIONS_TIER[instrument_product_mapped]] + OPTIONS_CLEARING_FEE_INDEX) * abs(trade['Quantity'])
                                        else:
                                            instrument['Fees'] += -OPTIONS_CLEARING_FEE_STOCK * abs(trade['Quantity'])
                                            if trade['Price'] > 0.01 and surface[:1] != "X":
                                                instrument['Fees'] += -OPTIONS_TIER_EXCHANGE_FEE_MM[OPTIONS_TIER[instrument_product_mapped]] * abs(trade['Quantity'])
                                            if trade['Price'] > 0.001 and surface[:1] == "X":
                                                instrument['Fees'] += -OPTIONS_TIER_EXCHANGE_FEE_MM[OPTIONS_TIER[instrument_product_mapped]] * abs(trade['Quantity'])
                                    if instrument['Type'] == 'Equity':
                                        instrument['Fees'] += -( max(0.5/10000 * trade['Price'] * abs(trade['Quantity']),0.35) + 0.785/10000 * trade['Price'] * abs(trade['Quantity']) + min(max(0.2/10000 * trade['Price'] * abs(trade['Quantity']),2),100) + 5 )
                    
                    if instrument['TradedFlag'] == 1:
                        risk_dict.setdefault(instrument['Expiry'],{})
                        risk_dict[instrument['Expiry']].setdefault(surface,{})
                        for measure in MEASURES:
                            risk_dict[instrument['Expiry']][surface].setdefault(measure,0)
                            risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                        for measure in CALCULATED_MEASURES:
                            risk_dict[instrument['Expiry']][surface].setdefault(measure,0)
                            risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                        risk_dict[instrument['Expiry']][surface]['PLDay'] += instrument['TotalTheoreticalValueDay'] - instrument['TotalCostOfTrade']
                        risk_dict[instrument['Expiry']][surface]['Fees'] += instrument['Fees']
                    
                    if instrument['TradedFlag'] == 1 or instrument['YesterdayPosition'] != 0:
                        risk_dict.setdefault(instrument['Expiry'],{})
                        risk_dict[instrument['Expiry']].setdefault(surface,{})
                        for measure in MEASURES:
                            risk_dict[instrument['Expiry']][surface].setdefault(measure,0)
                            risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                        for measure in CALCULATED_MEASURES:
                            risk_dict[instrument['Expiry']][surface].setdefault(measure,0)
                            risk_dict[instrument['Expiry']][surface].setdefault("Day"+measure,0)
                        risk_dict[instrument['Expiry']][surface]['PL'] += instrument['TotalTheoreticalValueToday'] - instrument['TotalTheoreticalValueYesterday'] + instrument['TotalTheoreticalValueDay'] - instrument['TotalCostOfTrade'] + instrument['Fees']
                #resp = sess.post("http://stash-hk.mavensecurities.com/production/ssomm/risklite/risk_instruments_"+portfolio+"_"+surface+".json", json.dumps(instrument_list), headers={'content-type': 'application/json'},auth=HttpNegotiateAuth())
        sorted_risk_dict = {}
        for key in sorted(risk_dict.keys()):
            sorted_risk_dict[key] = risk_dict[key]

        sorted_risk_dict['Total'] = {}
        sorted_risk_dict['Total']['Total'] = {}
        for expiry in sorted_risk_dict:
            if expiry not in ['Total']:
                sorted_risk_dict[expiry]['Total'] = {}
                for surface in sorted_risk_dict[expiry]:
                    if surface not in ['Total']:
                        for measure in MEASURES:
                            sorted_risk_dict[expiry]['Total'].setdefault(measure,0)
                            sorted_risk_dict[expiry]['Total'].setdefault("Day"+measure,0)
                            sorted_risk_dict[expiry]['Total'][measure] += sorted_risk_dict[expiry][surface][measure]
                            sorted_risk_dict[expiry]['Total']["Day"+measure] += sorted_risk_dict[expiry][surface]["Day"+measure]
                        for measure in CALCULATED_MEASURES:
                            sorted_risk_dict[expiry]['Total'].setdefault(measure,0)
                            sorted_risk_dict[expiry]['Total'].setdefault("Day"+measure,0)
                            sorted_risk_dict[expiry]['Total'][measure] += sorted_risk_dict[expiry][surface][measure]
                            sorted_risk_dict[expiry]['Total']["Day"+measure] += sorted_risk_dict[expiry][surface]["Day"+measure]
                for measure in MEASURES:
                    sorted_risk_dict['Total']['Total'].setdefault(measure,0)
                    sorted_risk_dict['Total']['Total'].setdefault("Day"+measure,0)
                    sorted_risk_dict['Total']['Total'][measure] += sorted_risk_dict[expiry]['Total'][measure]
                    sorted_risk_dict['Total']['Total']["Day"+measure] += sorted_risk_dict[expiry]['Total']["Day"+measure]
                for measure in CALCULATED_MEASURES:
                    sorted_risk_dict['Total']['Total'].setdefault(measure,0)
                    sorted_risk_dict['Total']['Total'].setdefault("Day"+measure,0)
                    sorted_risk_dict['Total']['Total'][measure] += sorted_risk_dict[expiry]['Total'][measure]
                    sorted_risk_dict['Total']['Total']["Day"+measure] += sorted_risk_dict[expiry]['Total']["Day"+measure]

        for expiry in sorted_risk_dict:
            if expiry not in ['Total']:
                for surface in sorted_risk_dict[expiry]:
                    if surface not in ['Total']:
                        sorted_risk_dict['Total'].setdefault(surface,{})
                        for measure in MEASURES:
                            sorted_risk_dict['Total'][surface].setdefault(measure,0)
                            sorted_risk_dict['Total'][surface].setdefault("Day"+measure,0)
                            sorted_risk_dict['Total'][surface][measure] += sorted_risk_dict[expiry][surface][measure]
                            sorted_risk_dict['Total'][surface]["Day"+measure] += sorted_risk_dict[expiry][surface]["Day"+measure]
                        for measure in CALCULATED_MEASURES:
                            sorted_risk_dict['Total'][surface].setdefault(measure,0)
                            sorted_risk_dict['Total'][surface].setdefault("Day"+measure,0)
                            sorted_risk_dict['Total'][surface][measure] += sorted_risk_dict[expiry][surface][measure]
                            sorted_risk_dict['Total'][surface]["Day"+measure] += sorted_risk_dict[expiry][surface]["Day"+measure]
        
        total_dict['Timestamp'] = valuedate
        total_dict['Expiry'] = []
        total_dict['Underlying'] = []
        for measure in SUMMARY_MEASURES:
            total_dict[SUMMARY_MEASURES[measure]] = []

        for expiry in sorted_risk_dict:
            for surface in sorted_risk_dict[expiry]:
                total_dict['Underlying'].append(surface)
                total_dict['Expiry'].append(expiry)
                for measure in SUMMARY_MEASURES:
                    if measure in sorted_risk_dict[expiry][surface]:
                        if measure in CURRENCY_MEASURES:
                            total_dict[SUMMARY_MEASURES[measure]].append(sorted_risk_dict[expiry][surface][measure] / CURRENCY_RATE)
                        else:
                            total_dict[SUMMARY_MEASURES[measure]].append(sorted_risk_dict[expiry][surface][measure])
                    else:
                        total_dict[SUMMARY_MEASURES[measure]].append(0)
        
        day_dict['Timestamp'] = valuedate
        day_dict['Expiry'] = []
        day_dict['Underlying'] = []
        for measure in SUMMARY_MEASURES:
            day_dict[SUMMARY_MEASURES[measure]] = []

        for expiry in sorted_risk_dict:
            for surface in sorted_risk_dict[expiry]:
                day_dict['Underlying'].append(surface)
                day_dict['Expiry'].append(expiry)
                for measure in SUMMARY_MEASURES:
                    if "Day" + measure in sorted_risk_dict[expiry][surface]:
                        if measure in [s for s in CURRENCY_MEASURES]:
                            day_dict[SUMMARY_MEASURES[measure]].append(sorted_risk_dict[expiry][surface]["Day" + measure] / CURRENCY_RATE)
                        else:
                            day_dict[SUMMARY_MEASURES[measure]].append(sorted_risk_dict[expiry][surface]["Day" + measure])
                    else:
                        day_dict[SUMMARY_MEASURES[measure]].append(0)
        
        resp = sess.post("http://stash-hk.mavensecurities.com/production/ssomm/risklite/risk_"+portfolio_group+".json", json.dumps(total_dict), headers={'content-type': 'application/json'},auth=HttpNegotiateAuth())
        resp = sess.post("http://stash-hk.mavensecurities.com/production/ssomm/risklite/dayrisk_"+portfolio_group+".json", json.dumps(day_dict), headers={'content-type': 'application/json'},auth=HttpNegotiateAuth())
