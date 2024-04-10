import airflow
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

from importlib.machinery import SourceFileLoader

main = SourceFileLoader("main", "/home/patchwork/Documents/projects_becode/immo_eliza_goats/main.py").load_module()
cleaner = SourceFileLoader("cleaner", "/home/patchwork/Documents/projects_becode/immo_eliza_goats/clean.py").load_module()


dag_scrape = DAG(
        dag_id = 'dag_scrape',
        start_date = datetime(2024,4,8),
        schedule = None
        )

data_dirpath = "/home/patchwork/Documents/projects_becode/immo_eliza_goats/data"


is_raw_data_here_pre = FileSensor(
        task_id = "is_raw_data_here_pre",
        filepath = f"{data_dirpath}/raw/data.csv",
        dag = dag_scrape
        )

is_clean_data_here_pre = FileSensor(
        task_id = "is_clean_data_here_pre",
        filepath = f"{data_dirpath}/clean/data.csv",
        dag = dag_scrape
        )


archive_file = f"gzip {data_dirpath}/clean/data.csv && \
                mv {data_dirpath}/clean/data.csv.gz {data_dirpath}/archive/clean_data_prev.csv.gz && \
                rm {data_dirpath}/raw/data.csv"

archive = BashOperator(
        task_id = "archive",
        bash_command = archive_file,
        dag = dag_scrape
        )

#scrape_immo = f"{dirpath}/main.py"
scrape = PythonOperator(
        task_id = "scrape",
        python_callable = main.async_run_main,
        dag = dag_scrape
        )

is_raw_data_here_post = FileSensor(
        task_id = "is_raw_data_here_post",
        filepath = f"{data_dirpath}/raw/data.csv",
        dag = dag_scrape
        )

clean = PythonOperator(
        task_id = 'clean',
        python_callable = cleaner.run_clean,
        dag = dag_scrape
        )

is_clean_data_here_post = FileSensor(
        task_id = "is_clean_data_here_post",
        filepath = f"{data_dirpath}/clean/data.csv",
        dag = dag_scrape
        )


is_raw_data_here_pre >> is_clean_data_here_pre >> archive >> scrape >> is_raw_data_here_post >> clean >> is_clean_data_here_post
