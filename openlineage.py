from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

create_table_query= '''
CREATE TABLE IF NOT EXISTS animal_adoptions_combined (date DATE, type VARCHAR, name VARCHAR, age INTEGER);
'''

combine_data_query= '''
INSERT INTO animal_adoptions_combined (date, type, name, age) 
SELECT * 
FROM adoption_center_1
UNION 
SELECT *
FROM adoption_center_2;
'''


with DAG('lineage-combine-postgres',
         start_date=datetime(2020, 6, 1),
         max_active_runs=1,
         schedule_interval='@daily',
         default_args = {
            'retries': 1,
            'retry_delay': timedelta(minutes=1)
        },
         catchup=False
         ) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql=create_table_query
    ) 

    insert_data = PostgresOperator(
        task_id='combine',
        postgres_conn_id='postgres_default',
        sql=combine_data_query
    ) 

    create_table >> insert_data
    
    
    
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

aggregate_reporting_query = '''
INSERT INTO adoption_reporting_long (date, type, number)
SELECT c.date, c.type, COUNT(c.type)
FROM animal_adoptions_combined c
GROUP BY date, type;
'''

with DAG('lineage-reporting-postgres',
         start_date=datetime(2020, 6, 1),
         max_active_runs=1,
         schedule_interval='@daily',
         default_args={
            'retries': 1,
            'retry_delay': timedelta(minutes=1)
        },
         catchup=False
         ) as dag:

    create_table = PostgresOperator(
        task_id='create_reporting_table',
        postgres_conn_id='postgres_default',
        sql='CREATE TABLE IF NOT EXISTS adoption_reporting_long (date DATE, type VARCHAR, number INTEGER);',
    ) 

    insert_data = PostgresOperator(
        task_id='reporting',
        postgres_conn_id='postgres_default',
        sql=aggregate_reporting_query
    ) 

    create_table >> insert_data