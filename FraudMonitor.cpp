#include "FraudMonitor.hpp"

#include <fstream>
#include <functional>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <vector>

// ~~~~~~~~~~~~~~~~~~~~~~ HELPER FUNCTIONS ~~~~~~~~~~~~~~~~~~~~~~
std::string ltrim(const std::string& str) {
	std::string s(str);
	s.erase(s.begin(), find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(isspace))));
	return s;
}

std::string rtrim(const std::string& str) {
	std::string s(str);
	s.erase(find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(isspace))).base(), s.end());
	return s;
}

std::vector<std::string> split(const std::string& str, char delim) {
	std::vector<std::string> tokens;
	std::string::size_type start = 0;
	std::string::size_type end = 0;

	while ((end = str.find(delim, start)) != std::string::npos) {
		tokens.push_back(str.substr(start, end - start));
		start = end + 1;
	}
	tokens.push_back(str.substr(start));
	return tokens;
}

// ~~~~~~~~~~~~~~~~~~~~~~ MEMBER FUNCTIONS ~~~~~~~~~~~~~~~~~~~~~~
// Constructor
FraudMonitor::FraudMonitor(const std::string& acct_info_csv_path, const std::string& transaction_csv_path, const std::string& amount_fraud_log_path, 
	const std::string& state_fraud_log_path, const std::string& account_table_headers, const std::string& transaction_table_headers) :
	mAccountInfoCsvPath(acct_info_csv_path),
	mTransactionCsvPath(transaction_csv_path),
	mAmountFraudLogPath(amount_fraud_log_path),
	mStateFraudLogPath(state_fraud_log_path),
	mAccountTableHeaders(account_table_headers),
	mTransactionTableHeaders(transaction_table_headers),
	mDisplayFieldWidth(32) {
	this->initialize();
}

void FraudMonitor::initialize() {
	mConnection = mysql_init(0);
	mConnection = mysql_real_connect(mConnection, "localhost", "root", "SaveProject", "save_project_data", 3306, NULL, 0);
	if (!mConnection) {
		puts("Connection to database has failed!");
		return;
	}
	puts("Successful connection to database!");

	dropDatabaseTables();

	parseStateNamesTable();
}

// Clear the existing account info and transaction data
void FraudMonitor::dropDatabaseTables() {
	if (mysql_query(mConnection, "DELETE FROM transactions") || mysql_query(mConnection, "DELETE FROM account_info;")) {
		std::cout << "Failed to DELETE existing transacitons or account_info table: " << mysql_error(mConnection) << std::endl;
		return;
	}
}

void FraudMonitor::parseStateNamesTable() {
	// Query state name mapping
	std::string state_name_query = "SELECT s.code,s.state FROM state_names s";
	if (mysql_query(mConnection, state_name_query.c_str())) {
		std::cout << "State name query failed: " << mysql_error(mConnection) << std::endl;
		return;
	}
	MYSQL_ROW row;
	MYSQL_RES* res = mysql_store_result(mConnection);
	while (row = mysql_fetch_row(res)) {
		mStateNameMap.insert(std::pair<std::string, std::string>(row[0], row[1]));
	}
}

// Loads and cleans csv data as it is populated into the account_info and transactions tables
void FraudMonitor::populateDatabaseTables() {
	// Parse account data and insert into database
	std::ifstream accounts_file = std::ifstream(mAccountInfoCsvPath);
	if (!accounts_file.is_open()) {
		throw std::runtime_error("Could not open file");
	}
	std::vector<std::string> account_col_names;
	parseHeader(accounts_file, account_col_names);
	parseAccountData(accounts_file, account_col_names);

	// Parse transaction data and insert into database
	std::ifstream transactions_file = std::ifstream(mTransactionCsvPath);
	if (!transactions_file.is_open()) {
		throw std::runtime_error("Could not open file");
	}
	std::vector<std::string> tranactions_col_names;
	parseHeader(transactions_file, tranactions_col_names);
	parseTransactionData(transactions_file, tranactions_col_names);

	accounts_file.close();
	transactions_file.close();
}

// Read the column names
void FraudMonitor::parseHeader(std::ifstream& fs, std::vector<std::string>& col_names) {
	std::string col_name;
	std::string line;
	
	if (fs.good()) {
		// Extract the first line in the file
		std::getline(fs, line);
		std::stringstream ss(line);
		// Extract each column name
		while (std::getline(ss, col_name, ',')) {
			col_names.push_back(rtrim(ltrim(col_name)));
		}
	}
	//std::cout << "Found " << col_names.size() << " columns." << std::endl;
}

void FraudMonitor::parseAccountData(std::ifstream& accounts_file, const std::vector<std::string>& col_names) {
	std::string line;

	// Read data, line by line
	while (getline(accounts_file, line)) {
		std::stringstream ss(line);
		std::vector<std::string> results;

		// Delimit comma separated values
		results = split(line, ',');
		std::stringstream insert_query;
		insert_query << "INSERT INTO account_info (" << mAccountTableHeaders << ") VALUES (";
		for (uint32_t field_idx = 0; field_idx < results.size(); ++field_idx) {
			// Reformat DOB to YYYY-MM-DD
			if (col_names[field_idx] == "dob") {
				std::vector<std::string> dob_as_mm_dd_yyyy = split(results[field_idx], '/');
				results[field_idx] = dob_as_mm_dd_yyyy[2] + '-' + dob_as_mm_dd_yyyy[0] + '-' + dob_as_mm_dd_yyyy[1];
				//std::cout << "Reformated DOB to " << results[field_idx] << std::endl;
			}

			// Reformat state code (e.g. TX) to state name (e.g. Texas)
			if (col_names[field_idx] == "state") {
				results[field_idx] = mStateNameMap[results[field_idx]];
				//std::cout << "Reformated state name to " << results[field_idx] << std::endl;
			}

			if (results[field_idx].empty()) {
				insert_query << "NULL";
			}
			else {
				insert_query << "'" << results[field_idx] << "'";
			}
			// If not the last field, add comma
			if (field_idx != results.size() - 1) {
				insert_query << ",";
			}
		}
		insert_query << ")";

		if (mysql_query(mConnection, insert_query.str().c_str())) {
			std::cout << "Query failed: " << mysql_error(mConnection) << std::endl;
		}

		insert_query.clear();
	}
}

void FraudMonitor::parseTransactionData(std::ifstream& transactions_file, const std::vector<std::string>& col_names) {
	std::string line;

	// Read data, line by line
	while (getline(transactions_file, line)) {
		std::stringstream ss(line);

		// Delimit comma separated values
		std::vector<std::string> results = split(line, ',');
		std::stringstream insert_query;
		insert_query << "INSERT INTO transactions (" << mTransactionTableHeaders << ") VALUES (";
		for (uint32_t field_idx = 0; field_idx < results.size(); ++field_idx) {
			// Reformat DATETIME to YYYY-MM-DD HH:MM:SS from MMDDYYYY HH:MM:SS
			if (col_names[field_idx] == "transaction_datetime") {
				std::vector<std::string> transaction_date_time = split(results[field_idx], ' '); // [date,time]
				std::string mm = transaction_date_time[0].substr(0, 2);
				std::string dd = transaction_date_time[0].substr(2, 2);
				std::string yyyy = transaction_date_time[0].substr(4, 8);
				results[field_idx] = yyyy + '-' + mm + '-' + dd + " " + transaction_date_time[1];
				//std::cout << "Reformated transaction_datetime to " << results[field_idx] << std::endl;
			}

			// Reformat postfixed sign in transaction_amount to prefix
			else if (col_names[field_idx] == "transaction_amount") {
				std::string sign_str = results[field_idx].substr(results[field_idx].size() - 1);
				std::string unsigned_amt = results[field_idx].substr(0, results[field_idx].size() - 1);
				results[field_idx] = sign_str + unsigned_amt;
				//std::cout << "Transcation amount: " << results[field_idx] << std::endl;
			}

			// Reformat post_date column from MMDDYYYY to YYYY-MM-DD
			else if (col_names[field_idx] == "post_date") {
				std::string mm = results[field_idx].substr(0, 2);
				std::string dd = results[field_idx].substr(2, 2);
				std::string yyyy = results[field_idx].substr(4, 8);
				std::vector<std::string> post_date_mmddyyyy = split(results[field_idx], '/');
				results[field_idx] = yyyy + '-' + mm + '-' + dd;
				//std::cout << "Reformated post_date to " << results[field_idx] << std::endl;
			}

			// Parse merchant description
			if (col_names[field_idx] == "merchant_description") {
				// Escape all apostraphe's in the merchant_description
				std::size_t found_apostraphe = results[field_idx].find("\'");
				while (found_apostraphe != std::string::npos) {
					results[field_idx].insert(found_apostraphe, "\\");
					found_apostraphe = results[field_idx].find("\'", found_apostraphe + 2); // Increment by two to account for the character just added
					//std::cout << "Found at " << found_apostraphe << " in " << results[field_idx] << std::endl;
				}

				// BRIEF: Do our best to pull the merchant name from the merchant_description. It would be nice if there was a database of pretty names to merchant_number, but alas.
				std::string merchant_name;
				std::vector<std::string> mrch_desc_tokens = split(results[field_idx], ' ');
				// Store name is before tab when present
				if (results[field_idx].find('\t') != std::string::npos) {
					merchant_name = results[field_idx].substr(0, results[field_idx].find('\t'));
				}
				else {
					// Add merchant_name column. grab the first element as the merchant name by default
					merchant_name = mrch_desc_tokens[0];

					// If there's more than two tokens, clean it more
					if (mrch_desc_tokens.size() > 2) {
						uint32_t num_nonempty_elements = 0;
						// Count the number of non-empty elements
						for (std::size_t i = 0; i < mrch_desc_tokens.size(); ++i) {
							std::vector<std::string> mrch_desc_tab_split = split(mrch_desc_tokens[i], '\t');
							mrch_desc_tokens[i] = ltrim(rtrim(mrch_desc_tokens[i]));
							if (!mrch_desc_tokens[i].empty()) {
								++num_nonempty_elements;
							}
						}

						uint32_t nonempty_count = 1;
						// Add tokens except for the last two non-empty, which are typically city, state
						for (std::size_t i = 1; nonempty_count < num_nonempty_elements - 2; ++i) {
							// Only add non-empty elements
							if (!mrch_desc_tokens[i].empty()) {
								++nonempty_count;
								//	Break on <, #, tab
								if (mrch_desc_tokens[i].find('<') != std::string::npos ||
									mrch_desc_tokens[i].find('#') != std::string::npos ||
									mrch_desc_tokens[i].find('\t') != std::string::npos) {
									break;
								}
								else {
									merchant_name += " ";
									merchant_name += mrch_desc_tokens[i];
								}
							}
						}
					}
				}

				std::string state_code = (mrch_desc_tokens.back().size() > 2) ? mrch_desc_tokens.back().substr(0, 2) : mrch_desc_tokens.back();
				insert_query << "'" << results[field_idx] << "','" << merchant_name << "','" << mStateNameMap[state_code] << "',";
				//std::cout << "Reformated merchant_description to state: " << state << " name: " << merchant_name << std::endl; 
			}
			else {
				if (results[field_idx].empty()) {
					insert_query << "NULL";
				}
				else {
					insert_query << "'" << results[field_idx] << "'";
				}
				// If not the last field, add comma
				if (field_idx != results.size() - 1) {
					insert_query << ",";
				}
			}
		}
		insert_query << ")";
		if (mysql_query(mConnection, insert_query.str().c_str())) {
			std::cout << "Query failed: " << mysql_error(mConnection) << std::endl;
		}
		insert_query.clear();
	}
}

void FraudMonitor::runFraudCheck() {
	std::ofstream amount_fraud_log(mAmountFraudLogPath); // Fraud rule  #1
	std::ofstream state_fraud_log(mStateFraudLogPath);   // Fraud rule  #2
	std::stringstream ss;

	// Rule 1 Header: Amount Fraud
	ss << std::left << std::setw(mDisplayFieldWidth) << "Name "
		<< std::setw(mDisplayFieldWidth) << "Account Number "
		<< std::setw(mDisplayFieldWidth) << "Transaction Number "
		<< std::setw(mDisplayFieldWidth) << "Merchant "
		<< std::setw(mDisplayFieldWidth) << "Transaction Amount";
	amount_fraud_log << ss.str() << "\n"; // Log to file
	ss.str(std::string()); // Clear the output stringsteam

	// Rule 2 Header: State Fraud
	ss << std::left << std::setw(mDisplayFieldWidth) << "Name "
		<< std::setw(mDisplayFieldWidth) << "Account Number "
		<< std::setw(mDisplayFieldWidth) << "Transaction Number "
		<< std::setw(mDisplayFieldWidth) << "Expected Transaction Location "
		<< std::setw(mDisplayFieldWidth) << "Actual Transaction Location";
	state_fraud_log << ss.str() << "\n"; // Log to file
	ss.str(std::string()); // Clear the output stringsteam

	std::string fraud_check_query = "SELECT a.last_name,a.first_name,a.state,t.account_number,t.transaction_amount,t.merchant_number,t.merchant_name,t.transaction_state,t.transaction_number FROM account_info a INNER JOIN transactions t ON t.account_number = a.account_number";
	if (mysql_query(mConnection, fraud_check_query.c_str())) {
		std::cout << "Rule query failed: " << mysql_error(mConnection) << std::endl;
		return;
	}

	MYSQL_ROW row;
	MYSQL_RES* res = mysql_store_result(mConnection);

	// NOTE: If a transaction amount is X times greater than the average historical transaction for that merchant, then mark it as fraudulent
	double fraud_threshold_multiplier = 30.0; // NOTE: This can be tuned depending on how sensitive we want the fraud detection for rule 1.
	double alpha; // Used for Exponential Moving Average (EMA) of merchant transaction amounts
	// To prevent false fraud detections, only check transactions that have an amount greater than a threshold ($100)
	double fraud_check_min_transaction_amt = 100.0;
	// This could be made more efficient by mapping merchant numbers to an enum or actual number, instead of doing the lookup based on string.
	std::unordered_map<std::string, std::pair<uint32_t, double>> avg_transaction_amt_by_merchant; // Key: merchant_number Value: {num_transactions_at_merchant, EMA_at_merchant}

	// Loop through each row in transaction query
	while (row = mysql_fetch_row(res)) {
		std::string name = std::string(row[1]) + " " + std::string(row[0]);
		std::string acct_state = row[2];
		std::string acct_num = row[3];
		double transaction_amt = -1.0 * std::stod(row[4]); // Flip the sign because these are credit transactions
		std::string merchant_num = row[5];
		std::string merchant_name = row[6];
		std::string transaction_state = row[7];
		std::string transaction_num = row[8];

		// If this transaction was a refund, do not check it for fraud
		if (transaction_amt > 0.0) {
			// Update the transaction amount EMA for this merchant
			auto& transaction_ema_itr = avg_transaction_amt_by_merchant[merchant_num];
			++(transaction_ema_itr.first); // Increment the number of historical transactions at this merchant
			alpha = 2.0 / (transaction_ema_itr.first + 1.0); // Update alpha to account for number of transactions that have occurred at this merchant

			// If this was the first transaction at a merchant, we will not check it for fraud, but use it to seed the EMA
			if (transaction_ema_itr.first == 1) {
				transaction_ema_itr.second = (alpha * transaction_amt) + (1.0 - alpha) * transaction_ema_itr.second;
			}

			// Check fraud rule 1: If the current transaction is greater than a threshhold_gain times the historical EMA transaction amount at that merchant, mark it as fraudulent.
			if (transaction_amt > fraud_check_min_transaction_amt && transaction_amt > fraud_threshold_multiplier * transaction_ema_itr.second) {
				ss << std::setw(mDisplayFieldWidth) << name
					<< std::setw(mDisplayFieldWidth) << acct_num
					<< std::setw(mDisplayFieldWidth) << transaction_num
					<< std::setw(mDisplayFieldWidth) << merchant_name
					<< "$" << std::setw(mDisplayFieldWidth) << std::fixed << std::setprecision(2) << transaction_amt;
				amount_fraud_log << ss.str() << std::endl;
				ss.str(std::string()); // Clear the output stringsteam
			}
			else {
				// Calculate the EMA for transactions amounts at each merchant excluding fraudulent transactions from the EMA
				transaction_ema_itr.second = (alpha * transaction_amt) + (1.0 - alpha) * transaction_ema_itr.second;
			}
		}

		// Check fraud rule 2
		if (acct_state != transaction_state) {
			ss << std::setw(mDisplayFieldWidth) << name
				<< std::setw(mDisplayFieldWidth) << acct_num
				<< std::setw(mDisplayFieldWidth) << transaction_num
				<< std::setw(mDisplayFieldWidth) << acct_state
				<< std::setw(mDisplayFieldWidth) << transaction_state;
			state_fraud_log << ss.str() << "\n"; // Log to file
			ss.str(std::string()); // Clear the output stringsteam
		}
	}
	amount_fraud_log.close();
	state_fraud_log.close();
}

// ~~~~~~~~~~~~~~~~~~~~~~~~ GETTERS ~~~~~~~~~~~~~~~~~~~~~~~~
const std::string& FraudMonitor::getAmountFraudLogPath() {
	return mAmountFraudLogPath;
}

const std::string& FraudMonitor::getStateFraudLogPath() {
	return mStateFraudLogPath;
}