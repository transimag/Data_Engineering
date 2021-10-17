from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common.Postgres_To_Disk_Operator import PostgresToDiskOperator

db_tasks = []

dag = DAG(
    dag_id="home_work4",
    description="Домашнее задание 4",
    start_date=datetime(2021, 10, 15, 14, 30),
    end_date=datetime(2022, 10, 20, 14, 30),
    schedule_interval='@daily'
)

dummy1 = DummyOperator(task_id="start_dag", dag=dag)
dummy2 = DummyOperator(task_id="end_dag", dag=dag)

db_tasks.append(PostgresToDiskOperator(
    task_id = "home_work4_aisles",
    dag=dag,
    postgres_conn_id='pg_conn',
    save_path=True,
    sql = 'COPY public.aisles(aisle_id, aisle) TO STDOUT WITH HEADER CSV',
    file_path = '/home/user/shared_folder/',
    table_name = 'aisles'
))

db_tasks.append(PostgresToDiskOperator(
    task_id = "home_work4_clients",
    dag=dag,
    postgres_conn_id='pg_conn',
    save_path=True,
    sql = 'COPY public.clients(client_id, fullname, location_area_id) TO STDOUT WITH HEADER CSV',
    file_path = '/home/user/shared_folder/',
    table_name = 'clients'
))

db_tasks.append(PostgresToDiskOperator(
    task_id = "home_work4_departments",
    dag=dag,
    postgres_conn_id='pg_conn',
    save_path=True,
    sql = 'COPY public.departments(department_id, department) TO STDOUT WITH HEADER CSV',
    file_path = '/home/user/shared_folder/',
    table_name = 'departments'
))

db_tasks.append(PostgresToDiskOperator(
    task_id = "home_work4_orders",
    dag=dag,
    postgres_conn_id='pg_conn',
    save_path=True,
    sql = 'COPY public.orders(order_id, product_id, client_id, store_id, quantity, order_date) TO STDOUT WITH HEADER CSV',
    file_path = '/home/user/shared_folder/',
    table_name = 'orders'
))

db_tasks.append(PostgresToDiskOperator(
    task_id = "home_work4_products",
    dag=dag,
    postgres_conn_id='pg_conn',
    save_path=True,
    sql = 'COPY public.products(product_id, product_name, aisle_id, department_id) TO STDOUT WITH HEADER CSV',
    file_path = '/home/user/shared_folder/',
    table_name = 'products'
))

dummy1>>db_tasks>>dummy2
