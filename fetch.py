import pandas as pd
from duneanalytics import DuneAnalytics
from functools import lru_cache


def to_pandas(data):
    data = data['data']
    columns = data['query_results'][0]['columns']

    collated_data = {}
    for c in columns:
        collated_data[c] = [r['data'][c] for r in data['get_result_by_result_id']]
    
    return pd.DataFrame(collated_data)

@lru_cache(maxsize=None)
def fetch_table(query_id):
    # initialize client
    dune = DuneAnalytics("zoo_money", "wnajsl123!")

    # try to login
    dune.login()

    # fetch token
    dune.fetch_auth_token()
    result_id = dune.query_result_id(query_id=query_id)
    data = dune.query_result(result_id)
    return to_pandas(data)


# df = fetch_table(QUERY_ID['DirectLoanFixedOffer_LoanStarted'])
