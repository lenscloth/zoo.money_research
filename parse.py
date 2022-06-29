import pandas as pd
import datetime

WETH_CONTRACT = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
DAI_CONTRACT = "0x6b175474e89094c44da98b954eedeac495271d0f"


def parse_stringfied_float(s: pd.Series):
    return s.astype(float) / (10 ** 18)

def parse_loan_type(s: pd.Series):
    return s.astype(str).apply(lambda x: "wETH" if x == WETH_CONTRACT else "DAI")

def parse_loan_start_date(s: pd.Series):
    return s.astype(float).\
        apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')).\
        apply(pd.to_datetime).dt.date
    
def to_inverse_slashed(s: pd.Series):
    return s.astype(str).apply(lambda x: f"0{x[1:]}")

def to_days(s: pd.Series):
    return s.astype(float) / (3600 * 24)

def to_liquidated_mask(loan_id: pd.Series, liquidated_id: pd.Series):
    return loan_id.astype(float).apply(lambda x: x in liquidated_id)
