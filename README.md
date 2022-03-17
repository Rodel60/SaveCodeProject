Author: Mark Hansen
Purpose: Save Interview Houston, TX

Configuration
* MySQL 8.0
* Microsoft Visual Studio Community 2019 Version 16.11.11

Assumptions Made
1) Since there were no requirements for max length of the fields (e.g. merchant_name or merchant_description), I assumed reasonable lengths that supported at least the longest values in the current dataset.
2) Assuming that we want real-time fraud checkings, so the fraudulent transaction detections should be causal, only relying on historical transacitons rather than using the full data in aggregate. As a result a fraudulent first transaction at a given merchant will never be detected because there is no history.
3) Using a threshold gain and exponential moving average of previous transactions amounts to detect fraudulent transactions under rule 1.
