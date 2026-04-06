from oxidizer_lite.catalyst import Catalyst, CatalystConnection
from oxidizer_lite.reagent import Reagent

catalyst = CatalystConnection(host="localhost", port=6379, db=0) 

reagent = Reagent(catalyst)


@reagent.react()
def process(data: dict, context: dict):
    if "fetch_data" in data:
        result = data["fetch_data"]
    elif "transform_data" in data:
        result = data["transform_data"]
    elif "load_data" in data:
        result = data["load_data"]
    else:
        result = []
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("WORKER: Processing data with context:", context)
    print("WORKER: Received data:", len(data))
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    return result or []