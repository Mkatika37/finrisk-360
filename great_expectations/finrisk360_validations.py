import great_expectations as gx
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import os
import json
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_data():
    """Fetch loan records for validation."""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'FINRISK360'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FINRISK_WH'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS')
        )
        df = pd.read_sql("SELECT * FROM LOAN_RISK_SCORES LIMIT 10000", conn)
        conn.close()
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        print(f"ERROR connecting to Snowflake: {e}")
        return pd.DataFrame()

def run_validations():
    print("Running FinRisk 360 Data Quality Checks...")
    df = get_snowflake_data()
    if df.empty:
        return {"total": 0, "passed": 0, "failed": 1, "success_rate": 0, "report_data": {}}
    
    context = gx.get_context()
    ds = context.data_sources.add_pandas(name="finrisk_ds")
    asset = ds.add_dataframe_asset(name="finrisk_asset")
    batch_def = asset.add_batch_definition_whole_dataframe(name="finrisk_batch_def")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    
    suite = context.suites.add(gx.ExpectationSuite(name="finrisk360_suite"))
    validator = context.get_validator(batch=batch, expectation_suite=suite)
    
    print("Validating data layers...")
    
    # LAYER 1: SCHEMA & NULLS
    cols = ['loan_amount', 'ltv', 'dti', 'interest_rate', 'income', 'ltv_score', 
            'dti_score', 'rate_spread_score', 'macro_stress_score', 'risk_score', 
            'risk_tier', 'processing_date']
    for c in cols:
        validator.expect_column_to_exist(c)
    
    validator.expect_column_values_to_not_be_null('risk_score')
    validator.expect_column_values_to_not_be_null('risk_tier')
    validator.expect_column_values_to_not_be_null('loan_amount')
    
    # LAYER 1: RANGES
    validator.expect_column_values_to_be_between('risk_score', 0.0, 1.0)
    validator.expect_column_values_to_be_between('ltv', 0.0, 205.0)
    validator.expect_column_values_to_be_between('dti', 0.0, 100.0)
    validator.expect_column_values_to_be_between('interest_rate', 0.0, 30.0)
    validator.expect_column_values_to_be_in_set('risk_tier', ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'])
    validator.expect_table_row_count_to_be_between(100, 1000000)
    validator.expect_column_mean_to_be_between('risk_score', 0.05, 0.95)
    
    # LAYER 2: DISTRIBUTION
    dist = df['risk_tier'].value_counts(normalize=True).to_dict()
    avg_loan = df['loan_amount'].mean()
    dist_total, dist_passed = 5, 0
    if 0.0 <= dist.get('CRITICAL', 0) <= 0.30: dist_passed += 1
    if 0.0 <= dist.get('HIGH', 0) <= 0.50: dist_passed += 1
    if 0.20 <= dist.get('MEDIUM', 0) <= 0.99: dist_passed += 1
    if 0.0 <= dist.get('LOW', 0) <= 0.20: dist_passed += 1
    if 50000 <= avg_loan <= 2000000: dist_passed += 1
    
    # LAYER 3: FRESHNESS
    fresh_total, fresh_passed = 1, 0
    if 'processing_date' in df.columns:
        last_dt = pd.to_datetime(df['processing_date']).max()
        now = datetime.now(last_dt.tzinfo) if last_dt.tzinfo else datetime.now()
        if (now - last_dt).days <= 30: # Relaxed for sample data
            fresh_passed += 1

    results = validator.validate()
    ge_total = results['statistics']['evaluated_expectations']
    ge_passed = results['statistics']['successful_expectations']
    
    final_total = ge_total + dist_total + fresh_total
    final_passed = ge_passed + dist_passed + fresh_passed
    sr = round((final_passed / final_total) * 100, 1)

    print(f"✅ Structural Checks: {ge_passed}/{ge_total} PASSED")
    print(f"✅ Distribution checks: {dist_passed}/5 PASSED")
    print(f"✅ Freshness check: {'PASSED' if fresh_passed else 'FAILED'}")

    return {
        "total": final_total,
        "passed": final_passed,
        "failed": final_total - final_passed,
        "success_rate": sr,
        "report_data": results.to_json_dict()
    }

def main():
    res = run_validations()
    print("\n" + "="*50)
    print("FINRISK 360 DATA QUALITY REPORT")
    print("="*50)
    print(f"Total Expectations: {res['total']}")
    print(f"Passed: {res['passed']} ✅")
    print(f"Failed: {res['failed']} ❌")
    print(f"Success Rate: {res['success_rate']}%")
    print("="*50)
    
    path = f"logs/ge_report_{datetime.now().strftime('%Y%m%d%H%M')}.json"
    os.makedirs('logs', exist_ok=True)
    with open(path, 'w') as f:
        json.dump(res['report_data'], f, indent=2)
    print(f"Detailed logs: {path}")

if __name__ == "__main__":
    main()
