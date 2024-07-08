import os
import time
import csv
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from tabulate import tabulate
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Constants
API_URL = os.getenv('API_URL')
NETWORK = os.getenv('NETWORK')
ACCOUNT = os.getenv('ACCOUNT')
API_KEY = os.getenv('API_KEY')
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'

CSV_HEADERS = ["Timestamp (UTC)", "Slot", "Status", "Fee", "Compute Unit", "Token Name", "Token In", "Profit", "Memo", "Signature"]

# Create 'csv' folder if it doesn't exist
csv_folder = 'csv'
os.makedirs(csv_folder, exist_ok=True)

# Set up logging
logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_account():
    global ACCOUNT
    if not ACCOUNT:
        ACCOUNT = input("Please enter the account address: ")
    return ACCOUNT

def get_latest_transaction_signature(api_url, network, account):
    logger.debug(f"Fetching latest transaction for account: {account}")
    headers = {"x-api-key": API_KEY}
    params = {
        "network": network,
        "account": account,
        "tx_num": 1,
        "enable_raw": "true",
        "enable_events": "true"
    }
    response = requests.get(api_url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        if data.get("result"):
            signature = data["result"][0]["signatures"][0]
            block_time = data["result"][0]["raw"]["blockTime"]
            logger.debug(f"Latest transaction signature: {signature}, block time: {block_time}")
            return signature, block_time
    logger.error("Failed to fetch the latest transaction")
    return None, None

def fetch_and_parse_transactions(api_url, network, account, time_delta):
    latest_signature, latest_block_time = get_latest_transaction_signature(api_url, network, account)
    if not latest_signature:
        logger.error("Failed to fetch the latest transaction.")
        return []

    end_time = datetime.fromtimestamp(latest_block_time)
    start_time = end_time - time_delta if time_delta else datetime.min
    logger.debug(f"Fetching transactions from {start_time} to {end_time}")

    transactions = []
    before_tx_signature = latest_signature
    api_calls = 0
    continue_fetching = True

    while continue_fetching:
        api_calls += 1
        logger.debug(f"API call #{api_calls}, before_tx_signature: {before_tx_signature}")
        params = {
            "network": network,
            "account": account,
            "tx_num": 100,
            "enable_raw": "true",
            "enable_events": "true",
            "before_tx_signature": before_tx_signature
        }
        response = requests.get(api_url, headers={"x-api-key": API_KEY}, params=params)
        
        if response.status_code != 200:
            logger.error(f"Error in API request: {response.status_code}, {response.text}")
            break

        data = response.json()
        batch = data.get("result", [])

        if not batch:
            logger.debug("No more transactions to fetch")
            break

        logger.debug(f"Fetched {len(batch)} transactions in this batch")

        batch_start_time = datetime.fromtimestamp(batch[-1]["raw"]["blockTime"])
        batch_end_time = datetime.fromtimestamp(batch[0]["raw"]["blockTime"])

        for tx in batch:
            tx_time = datetime.fromtimestamp(tx["raw"]["blockTime"])
            if start_time <= tx_time <= end_time:
                transactions.append(parse_transaction(tx))
            elif tx_time < start_time:
                continue_fetching = False
                break

        if continue_fetching and batch:
            before_tx_signature = batch[-1]["signatures"][0]
        else:
            break

        # Update progress
        progress_msg = f"\rAPI calls: {api_calls}, Parsed transactions from {batch_start_time} to {batch_end_time}"
        sys.stdout.write(progress_msg)
        sys.stdout.flush()

    print()  # New line after progress updates
    transactions.reverse()  # Reverse to get chronological order
    logger.info(f"Total API calls made: {api_calls}")
    logger.info(f"Total transactions fetched: {len(transactions)}")
    return transactions

def parse_transaction(tx):
    block_time = tx.get('raw', {}).get('blockTime')
    blocktime_utc = datetime.utcfromtimestamp(block_time).strftime('%Y-%m-%d %H:%M:%S') if block_time else "N/A"
    signature = tx.get('signatures', ['N/A'])[0]
    slot = tx.get('raw', {}).get('slot', 'N/A')
    status = tx.get('status', 'N/A')
    compute_unit = tx.get('raw', {}).get('meta', {}).get('computeUnitsConsumed', 'N/A')
    fee = tx.get('raw', {}).get('meta', {}).get('fee', 'N/A')
   
    token_name = 'N/A'
    token_in = 'N/A'
    profit = 'N/A'
    if tx.get('actions') and len(tx['actions']) > 0:
        action = tx['actions'][0]
        token_info = action.get('info', {}).get('tokens_swapped', {}).get('in', {})
        token_name = token_info.get('symbol', 'N/A')
        token_in = token_info.get('amount', 'N/A')
       
        token_out = action.get('info', {}).get('tokens_swapped', {}).get('out', {}).get('amount', 'N/A')
        if token_in != 'N/A' and token_out != 'N/A':
            try:
                profit = float(token_out) - float(token_in)
            except ValueError:
                profit = 'N/A'
   
    memo = 'N/A'
    instructions = tx.get('raw', {}).get('transaction', {}).get('message', {}).get('instructions', [])
    for instruction in instructions:
        if instruction.get('parsed'):
            parsed = instruction['parsed']
            if isinstance(parsed, str) and parsed.strip():
                memo = parsed.strip()
                break
   
    return [blocktime_utc, slot, status, fee, compute_unit, token_name, token_in, profit, memo, signature]

def generate_csv_filename(account, timestamp):
    wallet_prefix = account[:5]
    return f"parse_{wallet_prefix}_{timestamp}.csv"

def save_to_csv(transactions, filename, headers, mode='w'):
    logger.info(f"Saving {len(transactions)} transactions to {filename}")
    with open(filename, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if mode == 'w':
            writer.writerow(headers)
        writer.writerows(transactions)
    logger.info(f"Successfully saved transactions to {filename}")

def generate_stats(df):
    logger.debug("Generating statistics from DataFrame")
    def analyze_memo_type(df, memo_type):
        if memo_type == "TOTAL":
            total = len(df)
            success = len(df[df['Status'] == 'Success'])
            fail = len(df[df['Status'] == 'Fail'])
        elif memo_type == "N/A":
            total = len(df[df['Memo'].isna() | (df['Memo'] == 'N/A')])
            success = len(df[(df['Status'] == 'Success') & (df['Memo'].isna() | (df['Memo'] == 'N/A'))])
            fail = len(df[(df['Status'] == 'Fail') & (df['Memo'].isna() | (df['Memo'] == 'N/A'))])
        else:
            total = len(df[df['Memo'] == memo_type])
            success = len(df[(df['Status'] == 'Success') & (df['Memo'] == memo_type)])
            fail = len(df[(df['Status'] == 'Fail') & (df['Memo'] == memo_type)])
        
        success_rate = (success / total * 100) if total > 0 else 0
        fail_rate = (fail / total * 100) if total > 0 else 0
        
        display_name = memo_type
        if memo_type != "TOTAL" and memo_type != "N/A":
            display_name += " (jito)" if "RPC" not in str(memo_type) else ""
        
        return [display_name, total, success, fail, f"{success_rate:.2f}%", f"{fail_rate:.2f}%"]

    # Convert 'Memo' column to string and replace NaN with 'N/A'
    df['Memo'] = df['Memo'].fillna('N/A').astype(str)

    # Get unique memos
    unique_memos = df['Memo'].unique().tolist()
    unique_memos = [memo for memo in unique_memos if memo != 'N/A']
    memo_types = ["TOTAL", "N/A"] + sorted(unique_memos)

    results = [analyze_memo_type(df, memo_type) for memo_type in memo_types]

    # Sort results: TOTAL and N/A first, then non-Jito, then Jito
    sorted_results = results[:2]  # TOTAL and N/A
    non_jito = sorted([r for r in results[2:] if "jito" not in str(r[0]).lower()], key=lambda x: str(x[0]))
    jito = sorted([r for r in results[2:] if "jito" in str(r[0]).lower()], key=lambda x: str(x[0]))
    sorted_results.extend(non_jito)
    sorted_results.extend(jito)

    logger.debug("Statistics generation completed")
    return sorted_results

def print_results(results):
    logger.debug("Printing results")
    headers = ["Memo Type", "Total", "Success", "Fail", "Success %", "Fail %"]
    non_zero_results = [row for row in results if row[1] > 0]

    print(tabulate(non_zero_results, headers=headers, tablefmt="grid"))

def main():
    account = get_account()
    logger.info(f"Analysis for account: {account}")

    while True:
        print("\nChoose a time range for statistics:")
        print("1. 5 minutes")
        print("2. 10 minutes")
        print("3. 20 minutes")
        print("4. 30 minutes")
        print("5. 1 hour")
        print("6. 5 hours")
        print("7. 12 hours")
        print("8. 24 hours")
        print("9. 48 hours")
        print("10. 7 days")
        print("11. Exit")

        choice = input("Enter your choice (1-11): ")

        if choice == '11':
            logger.info("Exiting the program")
            print("Exiting the program. Goodbye!")
            break

        time_ranges = {
            '1': timedelta(minutes=5),
            '2': timedelta(minutes=10),
            '3': timedelta(minutes=20),
            '4': timedelta(minutes=30),
            '5': timedelta(hours=1),
            '6': timedelta(hours=5),
            '7': timedelta(hours=12),
            '8': timedelta(hours=24),
            '9': timedelta(hours=48),
            '10': timedelta(days=7)  # 7 days
        }

        if choice not in time_ranges:
            logger.warning(f"Invalid choice: {choice}")
            print("Invalid choice. Please try again.")
            continue

        time_delta = time_ranges[choice]
        logger.info(f"Selected time range: {time_delta if time_delta else 'All time'}")

        print("Fetching and parsing transactions...")
        transactions = fetch_and_parse_transactions(API_URL, NETWORK, account, time_delta)

        if transactions:
            timestamp = int(time.time())
            csv_filename = generate_csv_filename(account, timestamp)
            csv_path = os.path.join(csv_folder, csv_filename)
            save_to_csv(transactions, csv_path, CSV_HEADERS)

            print(f"Analyzing {len(transactions)} transactions...")
            df = pd.read_csv(csv_path)
            results = generate_stats(df)
            print_results(results)
        else:
            print("No transactions found for the selected time range.")

if __name__ == "__main__":
    main()