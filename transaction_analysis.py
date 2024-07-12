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
from prettytable import PrettyTable
from collections import defaultdict

# Load environment variables
load_dotenv()

# Constants
API_URL = os.getenv('API_URL')
NETWORK = os.getenv('NETWORK')
ACCOUNT = os.getenv('ACCOUNT')
API_KEY = os.getenv('API_KEY')
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'
CSV_ANALYSIS = os.getenv('CSV_ANALYSIS', 'true').lower() != 'false'  # True by default


CSV_HEADERS = ["Timestamp (UTC)", "Slot", "Status", "Fee", "Compute Unit", "Token Name", "Token In", "Profit", "Memo", "Signature"]

# Create 'csv' and 'logs' folders if they don't exist
csv_folder = 'csv'
logs_folder = 'logs'
os.makedirs(csv_folder, exist_ok=True)
os.makedirs(logs_folder, exist_ok=True)

# Set up logging
logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def display_ascii_art():
    ascii_art = """
 # ##### #######   #####    # #####  ######  ##   ##   #####    #####   ##   ##  ###  ### 
## ## ##  ##   #  ##   ##  ## ## ##    ##    ###  ##  ##   ##  ##   ##  ##   ##   ##  ##  
   ##     ##      ##          ##       ##    #### ##  ##       ##       ##   ##    ####   
   ##     ####     #####      ##       ##    #######  ## ####  ## ####  ##   ##     ##    
   ##     ##           ##     ##       ##    ## ####  ##   ##  ##   ##  ##   ##     ##    
   ##     ##   #  ##   ##     ##       ##    ##  ###  ##   ##  ##   ##  ##   ##     ##    
  ####   #######   #####     ####    ######  ##   ##   #####    #####    #####     ####   
                                                                                          
    """
    print(ascii_art)
    print("Version 0.2-beta")
    print()

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
    logger.debug(f"Total API calls made: {api_calls}")
    logger.debug(f"Total transactions fetched: {len(transactions)}")
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
    logger.debug(f"Saving {len(transactions)} transactions to {filename}")
    with open(filename, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if mode == 'w':
            writer.writerow(headers)
        writer.writerows(transactions)
    logger.debug(f"Successfully saved transactions to {filename}")

def save_analysis_to_txt(results, account, time_delta, transactions_count, csv_analysis_results=None):
    timestamp = int(time.time())
    wallet_prefix = account[:5]
    txt_filename = f"parse_{wallet_prefix}_{timestamp}.txt"
    txt_path = os.path.join(logs_folder, txt_filename)

    logger.debug(f"Saving analysis results to {txt_path}")

    with open(txt_path, 'w', encoding='utf-8') as txt_file:
        txt_file.write(f"Analysis for account: {account}\n")
        txt_file.write(f"Time range: {time_delta}\n")
        txt_file.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        txt_file.write(f"Total transactions analyzed: {transactions_count}\n\n")

        headers = ["Memo Type", "Total", "Success", "Fail", "Success %", "Fail %"]
        txt_file.write(tabulate(results, headers=headers, tablefmt="grid"))
        txt_file.write("\n\n")

        if csv_analysis_results:
            txt_file.write("CSV Analysis Results:\n")
            txt_file.write(csv_analysis_results)

    logger.debug(f"Analysis results saved to {txt_path}")
    return txt_path

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

def ask_for_csv_analysis():
    while True:
        choice = input("Do you want to perform further CSV analysis? (y/n): ").lower()
        if choice in ['y', 'n']:
            return choice == 'y'
        print("Invalid input. Please enter 'y' or 'n'.")

def perform_csv_analysis(csv_path):
    analysis_results = f"Performing further analysis on {csv_path}\n\n"

    def analyze_csv(file_path):
        timestamps = []
        slot_stats = defaultdict(lambda: {'total': 0, 'success': 0, 'failed': 0})
        
        with open(file_path, 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                timestamp = datetime.strptime(row['Timestamp (UTC)'], '%Y-%m-%d %H:%M:%S')
                timestamps.append(timestamp)
                
                slot = row['Slot']
                status = row['Status']
                
                slot_stats[slot]['total'] += 1
                if status.lower() == 'success':
                    slot_stats[slot]['success'] += 1
                else:
                    slot_stats[slot]['failed'] += 1
        
        if not timestamps:
            return 0, 0, []
        
        earliest_timestamp = min(timestamps)
        latest_timestamp = max(timestamps)
        time_difference = latest_timestamp - earliest_timestamp
        total_seconds = time_difference.total_seconds()
        total_minutes = total_seconds / 60
        
        total_transactions = len(timestamps)
        
        if total_seconds > 0:
            avg_transactions_per_second = total_transactions / total_seconds
            avg_transactions_per_minute = total_transactions / total_minutes
        else:
            avg_transactions_per_second = total_transactions  # All transactions happened in less than a second
            avg_transactions_per_minute = total_transactions * 60  # Theoretical per-minute rate
        
        slot_stats_list = [{'slot': slot, **stats} for slot, stats in slot_stats.items()]
        
        return avg_transactions_per_minute, avg_transactions_per_second, slot_stats_list

    def create_table(data, sort_key, limit=10):
        sorted_data = sorted(data, key=lambda x: x[sort_key], reverse=True)[:limit]
        table = PrettyTable()
        table.field_names = ["Slot No", "Total Txn", "Success", "Failed"]
        for stat in sorted_data:
            table.add_row([
                stat['slot'],
                stat['total'],
                stat['success'],
                stat['failed']
            ])
        return table

    # Analyze the CSV file and generate results
    per_minute, per_second, slot_stats = analyze_csv(csv_path)
    
    analysis_results += f"Average transactions per minute: {per_minute:.4f}\n"
    analysis_results += f"Average transactions per second: {per_second:.4f}\n\n"
    
    # Create and format the two tables side by side
    table_total = create_table(slot_stats, 'total', 10)
    table_success = create_table(slot_stats, 'success', 10)
    
    analysis_results += "Top 10 Slots - Transaction Statistics:\n"
    analysis_results += "Sorted by Total Transactions (Descending)    |    Sorted by Successful Transactions (Descending)\n"
    analysis_results += "-" * 100 + "\n"
    
    total_lines = table_total.get_string().splitlines()
    success_lines = table_success.get_string().splitlines()
    
    for total_line, success_line in zip(total_lines, success_lines):
        analysis_results += f"{total_line:<50} | {success_line}\n"

    return analysis_results

def main():
    display_ascii_art()
    account = get_account()
    logger.info(f"Analysis for account: {account}")

    while True:
        print("\n PLEASE CONSIDER DONATING (SOL)- uGGim2n46EhwfU5X6eUB6rWQCmm3zJdpmLG7ZbHuisS ")        
        print("\nChoose a time range for statistics:")
        print("1. 5 minutes")
        print("2. 10 minutes")
        print("3. 20 minutes")
        print("4. 30 minutes")
        print("5. 1 hour")
        print("6. 6 hours")  # New option
        print("7. 12 hours")
        print("8. 24 hours")
        print("9. 48 hours")
        print("10. 7 days")
        print("11. Exit")  # Changed from 10 to 11

        choice = input("Enter your choice (1-11): ")  # Updated to 1-11

        if choice == '11':  # Updated to 11
            logger.info("Exiting the program")
            print("Exiting the program. Goodbye!")
            break

        time_ranges = {
            '1': timedelta(minutes=5),
            '2': timedelta(minutes=10),
            '3': timedelta(minutes=20),
            '4': timedelta(minutes=30),
            '5': timedelta(hours=1),
            '6': timedelta(hours=6),  # New option
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
        logger.info(f"Selected time range: {time_delta}")

        print("Fetching and parsing transactions...")
        transactions = fetch_and_parse_transactions(API_URL, NETWORK, account, time_delta)

        if transactions:
            # Save to CSV
            timestamp = int(time.time())
            csv_filename = generate_csv_filename(account, timestamp)
            csv_path = os.path.join(csv_folder, csv_filename)
            save_to_csv(transactions, csv_path, CSV_HEADERS)
            print(f"Transaction data saved to: {csv_path}")

            # Analyze and print results
            print(f"Analyzing {len(transactions)} transactions...")
            df = pd.DataFrame(transactions, columns=CSV_HEADERS)
            results = generate_stats(df)
            print_results(results)

            csv_analysis_results = None
            if CSV_ANALYSIS:
                # Perform CSV analysis
                csv_analysis_results = perform_csv_analysis(csv_path)
                print(csv_analysis_results)

            # Save all analysis results to txt file
            txt_path = save_analysis_to_txt(results, account, time_delta, len(transactions), csv_analysis_results)
            print(f"Analysis results saved to: {txt_path}")

        else:
            print("No transactions found for the selected time range.")

    print("Exiting the program. Goodbye!")
if __name__ == "__main__":
    main()