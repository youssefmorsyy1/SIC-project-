from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
import subprocess
import time

def wait_for_all_files_in_hdfs(**context):
    """
    Poll HDFS until all expected files are present
    """
    expected_files = [
        '/staging_zone/nifi/weather_sales_data/2024.csv',
        '/staging_zone/nifi/weather_sales_data/uk_retail_with_city.parquet',
        '/staging_zone/nifi/weather_sales_data/ghcnd-stations.txt',
        '/staging_zone/nifi/weather_sales_data/gb.csv'
    ]
    
    max_attempts = 20  # Max 20 attempts
    poke_interval = 30  # Check every 30 seconds
    
    for attempt in range(max_attempts):
        print(f"Attempt {attempt + 1}/{max_attempts}: Checking HDFS...")
        
        missing_files = []
        for file_path in expected_files:
            # Check if file exists in HDFS
            result = subprocess.run(
                f"hdfs dfs -test -e {file_path}",
                shell=True,
                capture_output=True
            )
            
            if result.returncode != 0:
                missing_files.append(file_path)
                print(f"  - Missing: {file_path}")
            else:
                print(f"  - Found: {file_path}")
        
        # If all files found, succeed
        if not missing_files:
            print("SUCCESS: All files found in HDFS!")
            return True
        
        # Wait before next attempt
        if attempt < max_attempts - 1:
            print(f"Waiting {poke_interval} seconds before next check...")
            time.sleep(poke_interval)
    
    # Timeout reached
    raise Exception(f"Timeout: Still missing files after {max_attempts * poke_interval} seconds: {missing_files}")


default_args = {
    'owner': 'group_17',
    'start_date': days_ago(0),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG('final_project_workflow_fn', 
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    spark_submit_cmd1 = """
        spark-submit \
        --master yarn \
        --deploy-mode client \
        --name arrow_spark \
        --conf spark.driver.memory=1g \
        --conf spark.driver.cores=1 \
        --conf spark.executor.memory=2g \
        --conf spark.executor.cores=2 \
        --conf spark.executor.instances=2 \
        --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
        /root/airflow/dags/scripts/sic_gp_transformations.py
        """
    
    spark_submit_cmd2 = """
        spark-submit \
        --master yarn \
        --deploy-mode client \
        --name arrow_spark \
        --packages net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2 \
        --conf spark.driver.memory=1g \
        --conf spark.driver.cores=1 \
        --conf spark.executor.memory=2g \
        --conf spark.executor.cores=2 \
        --conf spark.executor.instances=2 \
        --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
        /root/airflow/dags/scripts/sic_gp_apply_modeling.py
        """
    
    send_data_to_nifi = BashOperator(
        task_id='send_files_to_nifi',
        bash_command="""
        
        cp /data/2024.csv /shared/
        cp /data/uk_retail_with_city.parquet /shared/
        cp /data/ghcnd-stations.txt /shared/
        cp /data/gb.csv /shared/
        
        echo "Files sent to NiFi input directory"
        """
    )
    
    wait_for_ingestion = PythonOperator(
        task_id='wait_for_nifi_ingestion',
        python_callable=wait_for_all_files_in_hdfs,
        provide_context=True
    )

    run_spark_transformations = BashOperator(
        task_id='run_spark_transformations',
        bash_command=spark_submit_cmd1
    )

    run_spark_modeling = BashOperator(
        task_id='run_spark_modeling',
        bash_command=spark_submit_cmd2
    )
    
    send_data_to_nifi >> wait_for_ingestion >> run_spark_transformations >> run_spark_modeling