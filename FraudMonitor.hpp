#pragma once
#include <mysql.h>
#include <string>
#include <unordered_map>

class FraudMonitor
{
public:
	FraudMonitor(const std::string& acct_info_csv_path, 
		const std::string& transaction_csv_path, 
		const std::string& amount_fraud_log_path, 
		const std::string& state_fraud_log_path, 
		const std::string& account_table_headers,
		const std::string& transaction_table_headers);

	void populateDatabaseTables();

	void runFraudCheck();

	// Getters
	const std::string& getStateFraudLogPath();

	const std::string& getAmountFraudLogPath();

private:
	void dropDatabaseTables();

	void initialize();

	void parseStateNamesTable();

	void parseHeader(std::ifstream& fs, std::vector<std::string>& col_names);

	void parseAccountData(std::ifstream& accounts_file, const std::vector<std::string>& col_names);

	void parseTransactionData(std::ifstream& transactions_file, const std::vector<std::string>& col_names);

	MYSQL* mConnection;

	uint32_t mDisplayFieldWidth; // Width of print columns
	std::string mAccountInfoCsvPath;
	std::string mTransactionCsvPath;
	std::string mAmountFraudLogPath;
	std::string mStateFraudLogPath;
	std::string mAccountTableHeaders;
	std::string mTransactionTableHeaders;

	std::unordered_map<std::string, std::string> mStateNameMap; // types< state_code, state_name >
};
