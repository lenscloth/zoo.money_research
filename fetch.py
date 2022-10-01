import os
import numpy as np
import pandas as pd
from duneanalytics import DuneAnalytics
from functools import lru_cache
import json

from parse import parse_stringfied_float, parse_loan_type, parse_loan_start_date, to_inverse_slashed, to_days, to_liquidated_mask


CONTRACT_ADDRESS = dict(
    bayc='0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d',
    cryptopunks='0xb7f7f6c52f2e2fdb1963eab30438024864c313f6',
    artblock1='0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270',
    artblock2='0x059edd72cd353df5106d2b9cc5ab83a52287ac3a',
    larvalands='0xd4e4078ca3495de5b1d4db434bebc5a986197782',
    veefriends='0xa3aee8bce55beea1951ef834b99f3ac60d1abeeb',
    mayc='0x60e4d786628fea6478f785a6d7e704777c86a7c6',
    worldofwomen='0xe785e82358879f061bc3dcac6f0444462d4b5330',
    coolcats='0x1a92f7381b9f03921564a437210bb9396471050c',
    azuki='0xed5af388653567af2f388e6224dc7c4b3241c544',
    clonex='0x49cf6f5d44e70224e2e23fdcdd2c053f30ada28b',
    sandbox='0x50f5474724e0ee42d9a4e711ccfb275809fd6d4a',
    doodles='0x8a90cab2b38dba80c64b7734e58ee1db38b8992e',
)

TRADES_QUERY_ID = dict(
    bayc=937910,
    cryptopunks=937985,
    artblock1=937940,
    artblock2=937943,
    larvalands=937947,
    veefriends=937949,
    mayc=937890,
    worldofwomen=937952,
    coolcats=936591,
    azuki=937964,
    clonex=937969,
    sandbox=937972,
    doodles=937978,
)

NFTFI_QUERY_ID = dict(
    DirectLoanFixedOffer_LoanStarted=936442,
    DirectLoanFixedOffer_LoanLiquidated=936476,
    NFTfi_LoanStarted=936479,
    NFTfi_LoanLiquidated=936482,
)

FP_QUERY_ID=dict(
    bayc=936513,
    cryptopunks=936551,
    artblock1=936704,
    artblock2=936709,
    larvalands=936702,
    veefriends=936696,
    mayc=936695,
    worldofwomen=936641,
    coolcats=936591,
    azuki=936586,
    clonex=936582,
    sandbox=936578,
    doodles=936566,
)


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
    dune = DuneAnalytics("zoo_money", "shstm464")

    # try to login
    dune.login()

    # fetch token
    dune.fetch_auth_token()
    result_id = dune.query_result_id(query_id=query_id)
    data = dune.query_result(result_id)
    return to_pandas(data)


def loadNftFiV1():
    loan_term_df = fetch_table(NFTFI_QUERY_ID['NFTfi_LoanStarted'])
    loan_term_df = loan_term_df[loan_term_df["loanDuration"] > 1000]
    
    loan_liquidated = fetch_table(NFTFI_QUERY_ID['NFTfi_LoanLiquidated'])["loanId"].astype(float)
    loan_liquidated = to_liquidated_mask(loan_term_df["loanId"], loan_liquidated)

    # Compute APR
    loan_amount = parse_stringfied_float(loan_term_df["loanPrincipalAmount"])
    loan_max_amount = parse_stringfied_float(loan_term_df["maximumRepaymentAmount"])
    loan_days = to_days(loan_term_df["loanDuration"])
    apr = (100 * 365 * ((loan_max_amount - loan_amount) / loan_amount) / loan_days).round(decimals=2)
    loan_start_date = parse_loan_start_date(loan_term_df["loanStartTime"])
    return pd.DataFrame({
        "borrower": to_inverse_slashed(loan_term_df["borrower"]),
        "lender": to_inverse_slashed(loan_term_df["lender"]),
        "nftCollateralContract": to_inverse_slashed(loan_term_df["nftCollateralContract"]),
        "nftCollateralId": loan_term_df["nftCollateralId"].astype(float).astype(int),
        "loanDuration": loan_days,
        "loanStartTime":loan_start_date,
        "loanPrincipalAmount": loan_amount,
        "maximumRepaymentAmount": loan_max_amount,
        "apr": apr,
        "loanERC20Denomination": parse_loan_type(to_inverse_slashed(loan_term_df["loanERC20Denomination"])),
        "loanLiquidated": loan_liquidated,
    })

        
def loadNftFiV2():
    loan_terms = fetch_table(NFTFI_QUERY_ID['DirectLoanFixedOffer_LoanStarted'])
    loan_liquidated = fetch_table(NFTFI_QUERY_ID['DirectLoanFixedOffer_LoanLiquidated'])["loanId"].astype(float)
    loan_liquidated = to_liquidated_mask(loan_terms["loanId"], loan_liquidated)
    loan_term_df = pd.DataFrame()
    for pd_row in loan_terms["loanTerms"]:
        json_row = json.loads(pd_row)
        loan_term_df = loan_term_df.append(json_row, ignore_index=True)

    # Compute APR
    loan_amount = parse_stringfied_float(loan_term_df["loanPrincipalAmount"])
    loan_max_amount = parse_stringfied_float(loan_term_df["maximumRepaymentAmount"])
    loan_days = to_days(loan_term_df["loanDuration"])
    apr = (100 * 365 * ((loan_max_amount - loan_amount) / loan_amount) / loan_days).round(decimals=2)


    gatherd_loan_term_df = pd.DataFrame({
        "borrower": to_inverse_slashed(loan_terms["borrower"]),
        "lender": to_inverse_slashed(loan_terms["lender"]),
        "nftCollateralContract": to_inverse_slashed(loan_term_df["nftCollateralContract"]),
        "nftCollateralId": loan_term_df["nftCollateralId"].astype(float).astype(int),
        "loanDuration": loan_days,
        "loanStartTime": parse_loan_start_date(loan_term_df["loanStartTime"]),
        "loanPrincipalAmount": loan_amount,
        "maximumRepaymentAmount": loan_max_amount,
        "apr": apr,
        "loanERC20Denomination": parse_loan_type(to_inverse_slashed(loan_term_df["loanERC20Denomination"])),
        "loanLiquidated": loan_liquidated
    })
    
    # remove weird loan_term
    gatherd_loan_term_df = gatherd_loan_term_df[loan_term_df["loanDuration"] > 1000]
    return gatherd_loan_term_df    


def tryInsertFpAndLtv(loan_info):
    for contract_name in CONTRACT_ADDRESS.keys():
        contract_loan_info = loan_info[loan_info["nftCollateralContract"] == CONTRACT_ADDRESS[contract_name]]
        floor_price = fetch_table(FP_QUERY_ID[contract_name])
        day = floor_price["day"].apply(lambda x: x[:10])
        fp = floor_price["floor_price"].astype(float)
        floor_price = pd.Series(fp.values,index=day).to_dict()

        ltvs = []
        fps = []
        for d, a in zip(contract_loan_info["loanStartTime"], contract_loan_info["loanPrincipalAmount"]):
            s = ("%04d-%02d-%02d") % (d.year, d.month, d.day)
            if s in floor_price:
                fp = floor_price[s]
                fps.append(fp)
                ltv = float(a) / float(fp)
                ltvs.append(ltv)
            else:
                ltvs.append(float('nan'))
                fps.append(float('nan'))
                
        ltv = np.array(ltvs)
        loan_info.loc[contract_loan_info.index, "LTV"] = ltvs
        loan_info.loc[contract_loan_info.index, "FP"] = fps


def loaNFTfiHistory(cache=True):
    def loadDune():
        loan_info_b = loadNftFiV1()
        loan_info_a = loadNftFiV2()
        loan_info = pd.concat([loan_info_a, loan_info_b], axis=0)  
        loan_info = loan_info.reset_index(drop=True)
        loan_info = loan_info[loan_info["loanERC20Denomination"] == "wETH"]
        tryInsertFpAndLtv(loan_info=loan_info)
        return loan_info
    cache_path = "./.cache.pickle"    
    if cache:
        if os.path.exists(cache_path):
            return pd.read_pickle(cache_path)
    
    loan_info = loadDune()
    loan_info.to_pickle(cache_path)

    return loan_info

