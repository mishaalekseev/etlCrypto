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

symbols_to_update = ['BTCUSDT','ETHUSDT','BNBUSDT']

with DAG(
    dag_id='DAG_update_rowPrices_binance',
    default_args=args,
    start_date = datetime(year=2021, month=12, day=14, hour=18, minute=25, second=0),
    schedule_interval='*/2 * * * *',
    tags=['recent_trades'],
) as dag:

    # [Стартовый таск]
    def print_context():
        pprint('Start task')
        return 'Start task'

    run_this = PythonOperator(
        task_id='start_tasks',
        python_callable=print_context,
    )
    # [END]

    # [START Импорт функции]
    from func_update_ticker_price_binance import update_ticker_price_binance
    # Генерация нескольких тасков для каждого тикера
    for ticker in symbols_to_update:
        task = PythonOperator(
            task_id='update_recenttrades_' + str(ticker),
            python_callable=update_ticker_price_binance,
            op_kwargs={'symbolTicker': ticker},
        )

        run_this >> task
    # [END]

# run_thisTrigger = TriggerDagRunOperator(
#     task_id='rerun',
#     trigger_dag_id='DAG_update_rowPrices_binance',
#     dag=dag
#     ) 
# run_thisTrigger