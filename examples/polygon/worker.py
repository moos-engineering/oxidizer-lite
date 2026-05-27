from oxidizer_lite.catalyst import Catalyst, CatalystConnection
from oxidizer_lite.reagent import Reagent

catalyst = CatalystConnection(host="localhost", port=6379, db=0) 

reagent = Reagent(catalyst)


x = {
        'cfi': 'OPASPS', 
        'contract_type': 'put', 
        'exercise_style': 'american', 
        'expiration_date': '2012-09-22', 
        'primary_exchange': 'BATO', 
        'shares_per_contract': 100, 
        'strike_price': 305, 
        'ticker': 'O:AAPL120922P00305000', 
        'underlying_ticker': 'AAPL'
    }


@reagent.react()
def process(data: dict, context: dict):
    layer_id = context.get("layer_id")
    node_id = context.get("node_id")

    if layer_id == "bronze": 
        if node_id == "bronze.ticker_list":
            print("TICKER LIST ")
            tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "DIS" ]
            tickers = ["AAPL"]
            endpoints = []
            for ticker in tickers: 
                endpoint = f"/v3/reference/options/contracts?underlying_ticker={ticker}"
                endpoint = f"/v3/reference/options/contracts?underlying_ticker={ticker}&strike_price=300"
                endpoints.append({"endpoint": endpoint})
            result = endpoints
 


        elif node_id == "bronze.all_contracts_ticker":
            print("HERE: ", data)
            tickers = [] 
            for item in data["ticker_list"]:
                print("ITEM: ", item)
                ticker = item.get("ticker") 
                tickers.append({"ticker": ticker})
            result = tickers
        else:
            result = []
   

    elif layer_id == "silver":
        if "individual_option_data" in data:
            result = data["individual_option_data"]
        else:
            result = []





    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("WORKER: Processing data with context:", context)
    print("WORKER: Received data:", result[0] if len(result) > 0 else result)
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    return result or []