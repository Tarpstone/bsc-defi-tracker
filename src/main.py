from typing import List, Dict
from time import sleep
from bscscan import BscScan
import pandas as pd
import httpx
from datetime import datetime
import plotly.express as px


API_SLEEP_TIMER = 3


def yield_watch_tracker():
    # read wallet info
    wallet_dict = read_wallets_from_csv("data/address_table.csv")

    # set up an empty dict to store query results
    data_dict = {}
    for wallet, data in wallet_dict.items():
        data_dict[wallet] = query_yield_watch(
            wallet_address=data.get("address"),
            platform=",".join(data.get("platform_list")),
        )

    # extract USD info from each wallet (deposit, yield, and wallet balance)
    money_dict = {}
    for wallet, data in data_dict.items():
        money_dict[wallet] = extract_usd_info(data_dict=data)

    # convert to pandas dictionary
    wallet_df = pd.DataFrame(money_dict).transpose()
    wallet_df["full_balance"] = wallet_df["usd_total"] + wallet_df["usd_wallet_balance"]

    # write query results to each data file
    for row in wallet_df.itertuples():
        write_yield_watch_to_file(
            filename=row.Index,
            net_worth=row.full_balance,
            usd_yield=row.usd_yield,
            wallet_balance=row.usd_wallet_balance,
        )

    # read all wallet data and process
    data_df = read_yield_watch_from_file([
        'data/auto_wallet.csv',
        'data/beefy_wallet.csv',
        'data/binance_wallet.csv',
        'data/bunny_wallet.csv',
        'data/pancake_wallet.csv',
        'data/sushi_wallet.csv',
        'data/swamp_wallet.csv',
    ])

    # create charts
    create_charts(data_df)
    print("Done!")


def read_wallets_from_csv(wallet_path: str):
    wallet_dict = {}
    with open(wallet_path) as w:
        for line in w.readlines():
            next_wallet = line.strip().split(",")
            # dictionary has CSV filename as key, info as dictionary values
            wallet_dict[next_wallet[0]] = {
                "address": next_wallet[1],
                "platform_list": next_wallet[2:],
            }

    return wallet_dict


def query_yield_watch(wallet_address: str, platform: str):
    wallet_data = {}
    r = httpx.get(
        f"https://www.yieldwatch.net/api/all/{wallet_address}?platforms={platform}",
        timeout=30,
    )
    sleep(API_SLEEP_TIMER)
    # throw out records for platforms with no data
    # (i.e. if there's no data for PancakeSwap because you don't use that platform)
    cleaned_result = {k: v for k, v in r.json().get("result").items() if v}
    for data, value in cleaned_result.items():
        if data in ["watchBalance", "currencies"]:
            pass
        elif data in ["walletBalance"]:
            wallet_data["wallet_balance"] = value
        else:
            wallet_data[data] = value
    return wallet_data


def extract_usd_info(data_dict: Dict, money_dict: Dict = None):
    # create a money dict if it doesn't exist (top level call)
    if not money_dict:
        money_dict = {
            "usd_deposit": 0,
            "usd_yield": 0,
            "usd_total": 0,
            "usd_wallet_balance": 0,
        }
    for key, value in data_dict.items():
        if key == "deposit":
            money_dict["usd_deposit"] += value
        elif key == "yield":
            money_dict["usd_yield"] += value
        elif key == "total":
            money_dict["usd_total"] += value
        elif key == "wallet_balance":
            money_dict["usd_wallet_balance"] += value.get("totalUSDValue")
        # if there's a key that isn't 'deposit' or 'yield',
        # it must be nested data, so we call this function recursively.
        elif isinstance(value, dict):
            money_dict = extract_usd_info(data_dict=value, money_dict=money_dict)

    return money_dict


def write_yield_watch_to_file(
    filename: str, net_worth: float, usd_yield: float, wallet_balance: float
):
    current_time = datetime.now().strftime('%m-%d-%y %H:%M:%S')
    new_line_list = [current_time, filename, str(net_worth), str(usd_yield), str(wallet_balance)]
    with open("data/" + filename, 'a') as data:
        data.write("\n" + ','.join(new_line_list))
    
    return


def read_yield_watch_from_file(file_list: List[str]):
    df_list = []
    for wallet in file_list:
        data_df = pd.read_csv(wallet)
        # fill blanks with zeroes
        data_df.fillna(0, inplace=True)
        # convert dates to pandas datetime
        data_df['Datetime'] = pd.to_datetime(data_df['Datetime'])
        # convert dollar columns to pandas numeric
        for column in ['Net Worth', 'Yield', 'Wallet Balance']:
            if data_df[column].dtype != 'float64':
                data_df[column] = pd.to_numeric(data_df[column].str.strip().str.replace('[\$,]', '', regex=True))
        # resample to daily values
        resampled_df = data_df.set_index('Datetime').resample('1D').bfill()
        df_list.append(resampled_df)

    # concat all data frames
    data_df = pd.concat(df_list)
    return data_df


def create_charts(data_df):
    # group results by day
    grouped_df = data_df.groupby('Datetime').sum()
    # print the most recent line of data
    print(grouped_df[-1:])
    # plot on a simple bar chart
    fig = px.line(grouped_df, y="Net Worth")
    fig.write_image("output/net_worth_plot.png")
    return


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
                farms_df.lp_token_contract_address == row_dict["Index"][1],
                "tokenDecimal",
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
    yield_watch_tracker()
