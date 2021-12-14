#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Used for unit tests"""
"""Example DAG demonstrating the usage of the PythonOperator."""
import time
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='DAG_update_hourly_data',
    default_args=args,
    start_date = datetime(year=2021, month=12, day=14, hour=18, minute=25, second=0),
    schedule_interval='*/2 * * * *',
    tags=['recent_trades'],
) as dag:

    # [START howto_operator_python]
    def print_context():
        pprint('Start task')
        return 'Start task'

    run_this = PythonOperator(
        task_id='start_tasks',
        python_callable=print_context,
    )
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    from func_update_hourly_data import update_hourly_data

    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    task = PythonOperator(
            task_id='update_hourly_data',
            python_callable=update_hourly_data
        )

    run_this >> task
    # [END howto_operator_python_kwargs]

# Для бесконечного запуска
# run_thisTrigger = TriggerDagRunOperator(
#     task_id='rerun',
#     trigger_dag_id='DAG_update_rowPrices_Bitmex',
#     dag=dag
#     ) 
# run_thisTrigger