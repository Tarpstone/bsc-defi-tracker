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

    with open("data/WALLETS.csv", "r") as wallets:
        ADDRESSES = [line.strip() for line in wallets]

    # for reading farms, we need to split the underlying token addresses into a list
    farms = pd.read_csv("data/CONTRACTS.csv")
    farms["underlying_token_addresses"] = farms["underlying_token_addresses"].str.split(":")

    # initialize bsc python client
    bsc = BscScan(BSC_API_KEY)

    # first, generate a unique list of every contractAddress the wallet has interacted with.
    # we'll do this using the full token transaction list.
    # normal and internal transactions aren't necessary (think spending approvals, etc.)
    transaction_df = pd.DataFrame()
    for address in ADDRESSES:
        sleep(API_SLEEP_TIMER)
        transactions = bsc.get_bep20_token_transfer_events_by_address(
            address, startblock=1, endblock=9999999, sort="asc"
        )
        transaction_df = pd.DataFrame(transactions)

        # need an emergency rename of the "from" field here. this is a Python keyword.
        # if we try to iterate over namedtuples, "from" will become "_6". investigate =]
        transaction_df.rename(columns={"from": "source"}, inplace=True)

        wallet_df = (
            transaction_df[["contractAddress", "tokenName", "tokenSymbol", "tokenDecimal"]]
            .drop_duplicates(subset=["contractAddress"])
            .set_index("contractAddress")
        )
        unique_contracts = wallet_df.index.tolist()

        # iterate over transaction_df and make necessary updates to wallet/farms accordingly
        for row in transaction_df.itertuples():
            # first copy the namedtuple to dict so we can mutate it
            row_dict = row._asdict()
            print(row_dict['source'])

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

        # start token balances at zero
        wallet_df["api_balance"] = 0

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

    int_columns = ["tokenDecimal"]
    float_columns = ["balance", "api_balance"]

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
