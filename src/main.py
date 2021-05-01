import csv
from time import sleep
from bscscan import BscScan
import pandas as pd
import httpx


API_SLEEP_TIMER = 0.5


def bsc_defi_tracker():
    # data read for API keys, wallet addresses, and LP farms
    with open("data/BSCAPIKEY.csv", "r") as bsc_api:
        BSC_API_KEY = bsc_api.readline()

    with open("data/CMCAPIKEY.csv", "r") as cmc_api:
        CMC_API_KEY = cmc_api.readline()

    # convert all wallet addresses to lowercase to match BSC API
    with open("data/WALLETS.csv", "r") as wallets:
        ADDRESSES = [line.strip().lower() for line in wallets]

    # for reading farms, we need to split the underlying token addresses into a list
    all_farms_df = pd.read_csv("data/CONTRACTS.csv").set_index(["site", "pool_name"])
    all_farms_df["underlying_token_addresses"] = all_farms_df[
        "underlying_token_addresses"
    ].str.split(" : ")

    # initialize bsc python client
    bsc = BscScan(BSC_API_KEY)

    # start with empty aggregate DataFrames
    wallet_portfolio_df = pd.DataFrame()
    farms_portfolio_df = pd.DataFrame()

    for user_address in ADDRESSES:
        # first, generate a unique list of every contractAddress the wallet has interacted with.
        # we'll do this using the full token transaction list.
        # normal and internal transactions aren't necessary (think spending approvals, etc.)
        sleep(API_SLEEP_TIMER)
        transactions = bsc.get_bep20_token_transfer_events_by_address(
            user_address, startblock=1, endblock=9999999999, sort="asc"
        )
        transaction_df = data_typer(pd.DataFrame(transactions)).set_index("hash")

        # need an emergency rename of the "from" field here. this is a Python keyword.
        # if we try to iterate over namedtuples, "from" will become "_6"...
        transaction_df.rename(columns={"from": "source"}, inplace=True)

        wallet_df = transaction_df[
            ["contractAddress", "tokenName", "tokenSymbol", "tokenDecimal"]
        ].drop_duplicates(subset=["contractAddress"])
        wallet_df["wallet_address"] = user_address
        wallet_df.set_index(["wallet_address", "contractAddress"], inplace=True)
        unique_contracts = wallet_df.index.tolist()
        farms_df = all_farms_df.copy()

        # start token balances at zero
        wallet_df["api_balance"] = 0.0
        farms_df["api_balance"] = 0.0
        farms_df["tokenDecimal"] = 0

        # add tokenDecimal into the farms_df
        for row in wallet_df.itertuples():
            row_dict = row._asdict()
            farms_df.loc[
                farms_df.lp_token_contract_address == row_dict["Index"][1], "tokenDecimal"
            ] = row_dict["tokenDecimal"]

        # iterate over transaction_df and make necessary updates to wallet/farms accordingly
        for row in transaction_df.itertuples():
            # first copy the namedtuple to dict so we can mutate it
            row_dict = row._asdict()
            # if it's an inbound transaction, add the value to wallet for the correct token.
            # then check to see if it was being unstaked from a farm.
            if row_dict["to"] == user_address:
                wallet_df.loc[
                    (user_address, row_dict["contractAddress"]), "api_balance"
                ] += row_dict["value"]
                # need two checks here to make sure the pool contract AND token match
                if (
                    row_dict["source"] in farms_df.vault_contract_address.values
                ) and any(
                    farms_df.loc[
                        farms_df.vault_contract_address == row_dict["source"],
                        "lp_token_contract_address",
                    ]
                    == row_dict["contractAddress"]
                ):
                    farms_df.loc[
                        (farms_df.vault_contract_address == row_dict["source"])
                        & (
                            farms_df.lp_token_contract_address
                            == row_dict["contractAddress"]
                        ),
                        "api_balance",
                    ] -= row_dict["value"]
            # if it's an outbound transaction, subtract the value from wallet.
            # then check to see if it was being staked on a farm, or simply transferred elsewhere.
            elif row_dict["source"] == user_address:
                wallet_df.loc[
                    (user_address, row_dict["contractAddress"]), "api_balance"
                ] -= row_dict["value"]
                # need two checks here to make sure the pool contract AND token match
                if (row_dict["to"] in farms_df.vault_contract_address.values) and any(
                    farms_df.loc[
                        farms_df.vault_contract_address == row_dict["to"],
                        "lp_token_contract_address",
                    ]
                    == row_dict["contractAddress"]
                ):
                    farms_df.loc[
                        (farms_df.vault_contract_address == row_dict["to"])
                        & (
                            farms_df.lp_token_contract_address
                            == row_dict["contractAddress"]
                        ),
                        "api_balance",
                    ] += row_dict["value"]
            else:
                raise Exception(
                    "The user's wallet wasn't involved in the current transaction!"
                )

        # convert to actual token balance using tokenDecimal.
        for df in [wallet_df, farms_df]:
            df["tokenFactor"] = 10
            df["agg_token_factor"] = df["tokenFactor"] ** df["tokenDecimal"]
            df["balance"] = df["api_balance"] / df["agg_token_factor"]
            # sort wallet by most tokens held (replace with USD value once available)
            df.sort_values(by=["balance"], ascending=False, inplace=True)

        # drop any rows with zero balance at this point.
        wallet_df = wallet_df.loc[wallet_df.balance != 0.0]
        farms_df = farms_df.loc[farms_df.balance != 0.0]

        # time for farm math. we'll need total supply of each token to properly calcuate share.
        # need to call bscscan api
        farms_df["api_total_supply"] = 0.0
        for token in farms_df.lp_token_contract_address.to_list():
            sleep(API_SLEEP_TIMER)
            farms_df.loc[
                farms_df.lp_token_contract_address == token, "api_total_supply"
            ] = float(bsc.get_total_supply_by_contract_address(token))
            farms_df["total_supply"] = (
                farms_df["api_total_supply"] / farms_df["agg_token_factor"]
            )

        # for the first wallet address, fill the portfolio dataframe.
        # for the second+ wallet, concat existing to new.
        if wallet_portfolio_df.empty:
            wallet_portfolio_df = wallet_df.copy()
        else:
            wallet_portfolio_df = pd.concat([wallet_portfolio_df, wallet_df])

        # do the same for farms.
        if farms_portfolio_df.empty:
            farms_portfolio_df = farms_df.copy()
        else:
            farms_portfolio_df = pd.concat([farms_portfolio_df, farms_df])

    # what's next?
    # formula_for_lp_pools.png. might need bsc API pro to get USD values
    # transaction log to track gain/loss, then some sort of FIFO queue.
    # add up fees?

    # symbol list for CMC. remove known unsupported values for now
    unsupported_tokens = [
        "mooAutoCAKE-BNB",
        "mooCakeSmart",
        "mooSwampyCAKE-BNB",
        "sBGO",
        "SYRUP",
        "TOAD",
        "mooPancakeLINK-BNB",
        "mooPancakeBAND-BNB",
        "mooPancakeWATCH-BNB",
        "mooPancakeODDZ-BNB",
        "mooCakeV2ODDZ-BNB",
        "mooCakeV2COS-BNB",
        "mooCakeV2LTO-BNB",
        "mooCakeV2BAND-BNB",
        "mooCakeV2LINK-BNB",
        "MDEX LP",
        "BUSD-T",
    ]
    symbol_list = list(wallet_df.tokenSymbol.unique())
    for token in unsupported_tokens:
        if token in symbol_list:
            symbol_list.remove(token)

    # now we'll get the account balance for each of these
    # (sleeping for half a second so we don't overload the API)
    for contract in unique_contracts:
        sleep(API_SLEEP_TIMER)
        wallet_df.loc[
            contract, "api_balance"
        ] = bsc.get_acc_balance_by_token_contract_address(contract, ADDRESSES[0])

    # data type cleanup
    wallet_df = data_typer(wallet_df)

    # actual token balance
    wallet_df["tokenFactor"] = 10
    wallet_df["balance"] = wallet_df["api_balance"] / (
        wallet_df["tokenFactor"] ** wallet_df["tokenDecimal"]
    )

    # now we need prices.
    with httpx.Client() as client:
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": CMC_API_KEY,
        }

        # this call will grab the map of CMC ID to each crypto, but I think we can just use symbol.
        # r = client.get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/map", headers=headers)
        # cmc_df = pd.DataFrame(r.json().get("data"))
        symbol_string = ",".join(symbol_list)
        r = client.get(
            f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol={symbol_string}",
            headers=headers,
        )
        cmc_df = pd.DataFrame(r.json().get("data")).transpose()
        expanded_quote_json = cmc_df.quote.apply(json_to_pd_series)

        # axis=1 expands the JSON field horizontally. very cool
        cmc_df = pd.concat([cmc_df, expanded_quote_json], axis=1)

        print("test")

    # join price info back to main dataset
    wallet_df = wallet_df.merge(
        cmc_df[["symbol", "price"]],
        left_on="tokenSymbol",
        right_on="symbol",
        how="left",
    )
    wallet_df["value"] = wallet_df["balance"] * wallet_df["price"]
    wallet_df.to_csv("output/portfolio.csv")

    # Cake-LPs are something we'll have to track ourselves using the contract address and the assets that go into the transaction.
    return


def data_typer(df: pd.DataFrame):
    """Handles data typing for all API call conversions to DataFrame.

    Args:
        df (pd.DataFrame): DataFrame that needs dtype cleanup!
    """

    # run pandas automatic conversion first
    df = df.convert_dtypes()

    int_columns = [
        "blockNumber",
        "tokenDecimal",
        "nonce",
        "transactionIndex",
        "gas",
        "gasPrice",
        "gasUsed",
        "cumulativeGasUsed",
        "confirmations",
    ]
    float_columns = ["value", "balance", "api_balance"]

    # integer conversions
    for column in int_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], downcast="integer")

    # float conversions
    # we need astype here because numeric will break with really big numbers.
    # https://stackoverflow.com/questions/45696492/pandas-to-numeric-couldnt-convert-string-values-to-integers
    for column in float_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column].astype(float), downcast="float")

    return df.copy()


def json_to_pd_series(json_dict):
    """Great solution from
    https://stackoverflow.com/questions/25511765/pandas-expand-json-field-across-records

    """

    keys = json_dict.get("USD").keys()
    values = json_dict.get("USD").values()
    return pd.Series(values, index=keys)


if __name__ == "__main__":
    bsc_defi_tracker()
