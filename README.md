### **PLEASE CONSIDER DONATING (SOL)- uGGim2n46EhwfU5X6eUB6rWQCmm3zJdpmLG7ZbHuisS**

# Solana Transaction Analyzer

This Python script analyzes transactions for a given Solana account, providing statistics on transaction memos and their success rates.

## Features

- Fetch transactions for a specified time range
- Parse transaction data
- Generate statistics based on transaction memos
- Save transaction data to CSV files
- Display results in a tabular format

## EXAMPLE
```
Choose a time range for statistics:
1. 5 minutes
2. 10 minutes
3. 20 minutes
4. 30 minutes
5. 1 hour
6. 5 hours
7. 12 hours
8. 24 hours
9. 48 hours
10. 7 days
11. Exit
Enter your choice (1-11): 7
2024-07-08 20:25:49,613 - INFO - Selected time range: 12:00:00
Fetching and parsing transactions...
API calls: 15, Parsed transactions from 2024-07-08 08:55:38 to 2024-07-08 10:05:05
2024-07-08 20:27:05,829 - INFO - Total API calls made: 16
2024-07-08 20:27:05,829 - INFO - Total transactions fetched: 1525
2024-07-08 20:27:05,833 - INFO - Saving 1525 transactions to csv\parse_GDGDGGDC_1720450625.csv
2024-07-08 20:27:05,843 - INFO - Successfully saved transactions to csv\parse_GFRFWFC_1720450625.csv
Analyzing 1525 transactions...
+-------------+---------+-----------+--------+-------------+----------+
| Memo Type   |   Total |   Success |   Fail | Success %   | Fail %   |
+=============+=========+===========+========+=============+==========+
| TOTAL       |    1525 |      1525 |      0 | 100.00%     | 0.00%    |
+-------------+---------+-----------+--------+-------------+----------+
| N/A         |      18 |        18 |      0 | 100.00%     | 0.00%    |
+-------------+---------+-----------+--------+-------------+----------+
| 1 (jito)   |     410 |       410 |      0 | 100.00%     | 0.00%    |
+-------------+---------+-----------+--------+-------------+----------+
| 3 (jito)   |     568 |       568 |      0 | 100.00%     | 0.00%    |
+-------------+---------+-----------+--------+-------------+----------+
| 4 (jito)   |     529 |       529 |      0 | 100.00%     | 0.00%    |
+-------------+---------+-----------+--------+-------------+----------+
```
## Prerequisites

- Python 3.7 or higher
- pip (Python package installer)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/Lucifer1590/solana-transaction-analyzer.git
   cd solana-transaction-analyzer
   ```

2. Install the required dependencies:
   ```
   pip3 install requests pandas python-dotenv tabulate
   ```

## Configuration

1. Create a `.env` file in the root directory of the project with the following content:
   or edit the provided one from env.example to .env

   ```
   #DONT CHANGE THIS
   API_URL=https://api.shyft.to/sol/v1/transaction/history
   #THIS TOO 
   NETWORK=mainnet-beta
   #KEEPING THIS BLANK THE SCRIPT WILL ASK YOU FOR ACCOUNT EVERYTIME ITS RUN
   ACCOUNT=
   #YOUR SHYFT.IO API KEY
   API_KEY=your_api_key_here
   #FOR ADDITIONAL DEBUGGING
   DEBUG=false
   ```

2. Replace `your_api_key_here` with your actual Shyft API key.
3. Optionally, set an account address in the `ACCOUNT` field, or leave it blank to be prompted for it when running the script.

## Usage

Run the script using Python 3:

```
python3 transaction_analysis.py
```

Follow the on-screen prompts to select a time range for analysis.

## Functions Overview

- `get_account()`: Retrieves the account address from environment variables or user input.
- `get_latest_transaction_signature()`: Fetches the latest transaction signature for the account.
- `fetch_and_parse_transactions()`: Retrieves and parses transactions for the specified time range.
- `parse_transaction()`: Extracts relevant information from a single transaction.
- `generate_csv_filename()`: Creates a filename for the CSV output.
- `save_to_csv()`: Saves parsed transactions to a CSV file.
- `generate_stats()`: Calculates statistics based on transaction memos.
- `print_results()`: Displays the analysis results in a tabular format.

## Output

The script will create CSV files in a `csv` folder within the project directory. Each file contains the parsed transaction data for a specific analysis run.

The analysis results will be displayed in the console, showing statistics for different memo types, including success and failure rates.

## Debugging

Set `DEBUG=true` in the `.env` file to enable detailed logging.

## Contributing

Contributions, issues, and feature requests are welcome. Feel free to check [issues page](https://github.com/Lucifer1590/solana-transaction-analyzer/issues) if you want to contribute.

## Author

Lucifer1590 - [Testingguy](https://github.com/Lucifer1590)
contact Discord - testingguy2
## Acknowledgments

- [Shyft API](https://shyft.to/) for providing transaction data
- [Solana](https://solana.com/) blockchain
