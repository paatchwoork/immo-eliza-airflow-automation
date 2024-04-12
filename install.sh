#!/bin/bash

sudo apt-get install pip

pip install -r requirements.txt

workdir = $(pwd)

echo "import airflow
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

from importlib.machinery import SourceFileLoader

workdir = "/home/patchwork/Documents/projects_becode/immo_eliza_goats"
data_dirpath = f"{workdir}/data"

main = SourceFileLoader("main", f"{workdir}/src/main.py").load_module()
cleaner = SourceFileLoader("cleaner", f"{workdir}/src/clean.py").load_module()
trainer = SourceFileLoader("trainer", f"{workdir}/src/train.py").load_module()


dag_scrape = DAG(
        dag_id = 'dag_scrape',
        start_date = datetime(2024,4,8),
        schedule = @daily
        )



is_prev_data_here = FileSensor(
        task_id = 'is_prev_data_here',
        filepath = f'{data_dirpath}/clean/data.csv',
        poke_interval = 1,
        timeout = 5,
        dag = dag_scrape
        )

archive_file = f'gzip -k {workdir}/data/clean/data.csv && \
                mv {workdir}/data/clean/data.csv.gz {workdir}/data/archive/clean_data_prev.csv.gz && \
                rm {workdir}/data/raw/data.csv'

archive_prev_data = BashOperator(
        task_id = 'archive_prev_data',
        bash_command = archive_file,
        dag = dag_scrape
        )

scrape = PythonOperator(
        task_id = 'scrape',
        python_callable = main.async_run_main,
        op_kwargs = {'workdir': workdir},
        dag = dag_scrape
        )

is_raw_data_here = FileSensor(
        task_id = 'is_raw_data_here',
        filepath = f'{workdir}/data/raw/data.csv',
        poke_interval = 1,
        timeout = 5,
        dag = dag_scrape
        )

clean = PythonOperator(
        task_id = 'clean',
        python_callable = cleaner.run_clean,
        dag = dag_scrape
        )

is_clean_data_here = FileSensor(
        task_id = 'is_clean_data_here',
        filepath = f'{workdir}/data/clean/data.csv',
        poke_interval = 1,
        timeout = 5,
        dag = dag_scrape
        )

is_prev_model_here = FileSensor(
        task_id = 'is_prev_model_here',
        filepath = f'{workdir}/model/*.joblib',
        poke_interval = 1,
        timeout = 5,
        dag = dag_scrape
        )

archive_model = f'tar -czvf {workdir}/model_prev.tar.gz {workdir}/model/'
archive_prev_model = BashOperator(
        task_id = 'archive_prev_model',
        bash_command = archive_model,
        dag = dag_scrape
        )

train = PythonOperator(
        task_id = 'train',
        python_callable = trainer.train,
        op_kwargs = {'workdir': workdir},
        dag = dag_scrape
        )

update_api = BashOperator(
        task_id = 'update_api',
        bash_command = f'cp {workdir}/model/*joblib {workdir}/api/',
        dag = dag_scrape
        )

push_folder = BashOperator(
        task_id = 'push_folder',
        bash_command = f'cd {workdir} && git add . \
                && git commit -m 'Airflow run ended \$(date)' \
                && git push git@github.com:paatchwoork/immo-eliza-airflow-automation.git',
        dag = dag_scrape
        )

is_prev_data_here >> archive_prev_data 
archive_prev_data >> scrape >> is_raw_data_here 
is_raw_data_here >> clean >> is_clean_data_here 
is_clean_data_here >> is_prev_model_here >> archive_prev_model 
archive_prev_model >> train 
train >> update_api >> push_folder" > dag_scrape.py

mkdir ~/airflow/dags/

cp dag_scrape.py ~/airflow/dags/
