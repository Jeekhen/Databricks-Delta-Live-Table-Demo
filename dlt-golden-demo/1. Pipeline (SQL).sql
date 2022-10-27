-- Databricks notebook source
CREATE INCREMENTAL LIVE TABLE BZ_raw_txs
COMMENT "New raw loan data incrementally ingested from cloud object storage landing zone"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files('/home/jk.wong@databricks.com/dlt_demo/landing', 'json')

-- COMMAND ----------

CREATE LIVE TABLE ref_accounting_treatment
COMMENT "Lookup mapping for accounting codes"
AS SELECT * FROM delta.`/home/jk.wong@databricks.com/dlt_demo/ref_accounting_treatment/`

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE BZ_reference_loan_stats
COMMENT "Raw historical transactions"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files('/databricks-datasets/lending-club-loan-stats/LoanStats_*', 'csv')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE SV_cleaned_new_txs (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date > date('2020-12-31')),
  CONSTRAINT `Balance should be positive`    EXPECT (balance > 0 AND arrears_balance > 0) ON VIOLATION DROP ROW,
  CONSTRAINT `Cost center must be specified` EXPECT (cost_center_code IS NOT NULL) ON VIOLATION FAIL UPDATE
  -- Roadmap: Quarantine
)
COMMENT "Livestream of new transactions, cleaned and compliant"
TBLPROPERTIES ("quality" = "silver")
AS SELECT txs.*, rat.id as accounting_treatment FROM stream(LIVE.BZ_raw_txs) txs
INNER JOIN live.ref_accounting_treatment rat ON txs.accounting_treatment_id = rat.id

-- COMMAND ----------

CREATE LIVE TABLE SV_historical_txs
COMMENT "Historical loan transactions"
TBLPROPERTIES ("quality" = "silver")
AS SELECT a.* FROM LIVE.BZ_reference_loan_stats a
INNER JOIN LIVE.ref_accounting_treatment b USING (id)

-- COMMAND ----------

CREATE LIVE TABLE GL_total_loan_balances_1
COMMENT "Combines historical and new loan data for unified rollup of loan balances"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "location_code"
)
AS SELECT sum(revol_bal)  AS bal, addr_state   AS location_code FROM live.SV_historical_txs  GROUP BY addr_state
UNION SELECT sum(balance) AS bal, country_code AS location_code FROM live.SV_cleaned_new_txs GROUP BY country_code

-- COMMAND ----------

CREATE LIVE TABLE GL_total_loan_balances_2
COMMENT "Combines historical and new loan data for unified rollup of loan balances"
TBLPROPERTIES ("quality" = "gold")
AS SELECT sum(revol_bal)  AS bal, addr_state   AS location_code FROM live.SV_historical_txs  GROUP BY addr_state
UNION SELECT sum(balance) AS bal, country_code AS location_code FROM live.SV_cleaned_new_txs GROUP BY country_code

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW GL_new_loan_balances_by_cost_center
/*
COMMENT "Live table of new loan balances for consumption by different cost centers"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "cost_center_code"
)
*/
AS SELECT sum(balance), cost_center_code
FROM live.SV_cleaned_new_txs
GROUP BY cost_center_code

-- COMMAND ----------

CREATE LIVE VIEW GL_new_loan_balances_by_country
COMMENT "Live voew of new loan balances per country"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "country_code"
)
AS SELECT sum(count), country_code
FROM live.SV_cleaned_new_txs
GROUP BY country_code
