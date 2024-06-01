# Project 5: Airflow, Snowflake, and Docker Integration

## AIRFLOW create table in DAG
**Click pada example, kemudian jalakan script dengan click icon run**
![Alt image](https://github.com/lolipop20/project5/blob/main/pics/create-table-step-dag.png)

```
 create or replace table dbt_dev.public.example_table (
 id int,
 first_name varchar(50),
 last_name varchar(50)
 );
```

**Check pada Snowflake**
![Alt image](https://github.com/lolipop20/project5/blob/main/pics/create-table.png)

## AIRFLOW CRUD
**Click pada snowflake_crud_example, kemudian script jalankan dengan click icon run**
![Alt snowflake image](https://github.com/lolipop20/project5/blob/main/pics/snowflake_crud_example_DAG.png)

**Check pada logs create table**
![Alt image](https://github.com/lolipop20/project5/blob/main/pics/snowflake_crud_create_table_DAG.png)

**Check pada logs insert table**
![Alt logs image](https://github.com/lolipop20/project5/blob/main/pics/snowflake_crud_insert_DAG.png)

**Check pada logs update table**
![Alt image](https://github.com/lolipop20/project5/blob/main/pics/snowflake_crud_update_DAG.png)

**Check pada Snowflake**
![Alt image](https://github.com/lolipop20/project5/blob/main/pics/snowflake_crud_example_snowflake.png)

## AIRFLOW CONNECTOR_3
**Check pada logs create table**

```
def count1(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("select count(*) from dbt_dev.public.customer")
    logging.info("Number of rows in `dbt_dev.public.customer`  - %s", result[0])
```

**hasil logs**
![Alt logs image](https://github.com/lolipop20/project5/blob/main/pics/snowflake_connector_count_query_DAG.png)

**check count rows in snowflake**
![alt image](https://github.com/lolipop20/project5/blob/main/pics/snowflake_connector_count_query_snowflake.png)

**check show table in database**
```
query1 = [
    """select 1;""",
    """show tables in database dbt_dev;""",
]
```
![alt image](https://github.com/lolipop20/project5/blob/main/pics/snowflake_connector_DAG.png)
