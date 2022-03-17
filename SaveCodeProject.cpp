// SaveCodeProject.cpp : Save interview project code. Cleans and inserts account and transaction logs into a MySQL database, which is then used for fraud detection according to two rules.
#include "FraudMonitor.hpp"
#include <iostream>
#include <fstream>
#include <string>

// Read in and print all file contents to stdout line by line
void printFileContents(const std::string& file_path) {
	std::ifstream fs;
	fs.open(file_path, std::ios::in);
	if (fs.is_open()) {
		std::string line;
		while (std::getline(fs, line)) {
			std::cout << line << "\n";
		}
		fs.close();
	}
	std::cout << std::endl;
}

int main() {
	const std::string acct_info_csv_path        = "C:\\Users\\hanse\\Desktop\\SaveCodeProject\\account_info.csv";
	const std::string transaction_csv_path      = "C:\\Users\\hanse\\Desktop\\SaveCodeProject\\transactions.csv";
	const std::string amount_fraud_log_path     = "amount_fraud_log.txt";
	const std::string state_fraud_log_path      = "state_fraud_log.txt";
	const std::string account_table_headers     = "last_name, first_name, street_address, unit, city, state, zip, dob, ssn, email_address, mobile_number, account_number";
	const std::string transaction_table_headers = "account_number, transaction_datetime, transaction_amount, post_date, merchant_number, merchant_description, merchant_name, transaction_state, merchant_category_code, transaction_number";

	FraudMonitor fraud_monitor(acct_info_csv_path, transaction_csv_path, amount_fraud_log_path, state_fraud_log_path, account_table_headers, transaction_table_headers);

	fraud_monitor.populateDatabaseTables();
	fraud_monitor.runFraudCheck();

	// NOTE: For the sake of efficiency I am doing one query of the transaction table and only looping 
	// through that data once. Hence, if we print the fraud detections as we write it to the file , they will appear interleaved.
	// Since looping through the detections is shorter than looping through all transactions twice, I am reading the files back 
	// so I can print the data grouped by detection type.
	// Read the files back to stdout
	printFileContents(fraud_monitor.getAmountFraudLogPath());
	printFileContents(fraud_monitor.getStateFraudLogPath());

	return 0;
}

