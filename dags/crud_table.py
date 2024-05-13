from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
# Create DAG default arguments
args = {
 "owner": "airflow",
 "start_date": datetime(2023, 3, 10, 12, 00)
}
# Create DAG object
dag = DAG(
 dag_id="snowflake_crud_example",
 default_args=args,
 schedule_interval=None
)
# Create query new table in snowflake
create_table = '''
 create or replace table dbt_dev.public.customer (
 cust_id int,
 first_name varchar(50),
 last_name varchar(50),
 email varchar(50),
 city varchar(50)
 );
'''

# create query for insert data to snowflake
insert_data = '''
INSERT INTO dbt_dev.public.customer VALUES
(001,'Dini','Herman','dini_herman@gmail.com','jakarta'),
(002,'Firman','Ambara','firman@gmail.com','bogor'),
(003,'Dono','Ferdi','dono@gmail.com','bandung'),
(004,'Budi','Dermawan','budi@gmail.com','surabaya'),
(005,'Dodi','Man','dodi@gmail.com','padang');
'''

# create query for update data to snowflake
update_data = '''
UPDATE dbt_dev.public.customer SET city = 'Bandung' WHERE cust_id = 005
'''

# Connect to Snowflake
with dag:
    create_new_table = SnowflakeOperator(
        task_id="create_table",
        sql=create_table,
        snowflake_conn_id="snowflake_conn"
    )

    insert_data_table = SnowflakeOperator(
        task_id="insert_data_table",
        sql=insert_data,
        snowflake_conn_id="snowflake_conn"
    )

    update_data_table = SnowflakeOperator(
        task_id="update_data_table",
        sql=update_data,
        snowflake_conn_id="snowflake_conn"
    )

# Execute DAG
create_new_table >> insert_data_table >> update_data_table