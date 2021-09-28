SELECT 
    addr_state,
    -- purpose,
    application_type,
    sum(loan_amnt) total_loan_amnt,
    avg(loan_amnt) avg_loan_amnt
FROM delta_adb_essentials.loans_silver_partitioned
WHERE addr_state NOT IN ("debt_consolidation")
-- Change date range to see cache in action
AND loan_issue_date > "2015-12-01"
GROUP BY 1, 2
HAVING total_loan_amnt > 1
ORDER BY total_loan_amnt DESC