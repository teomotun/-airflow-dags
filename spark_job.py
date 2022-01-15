# -*- coding: utf-8 -*-
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

from datetime import timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator \
        import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator \
        import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator \
        import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator
import boto3


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# Get Cluster ID
client = boto3.client('emr')
try:
    lst = client.list_clusters()
    cluster_id = list(filter(lambda x: x['Status']['State'] == 'WAITING', lst['Clusters']))[0]['Id']
    print(cluster_id)
except:
    "Cluster ID wasn't retrieved, assigning arbitrary ID"
    cluster_id = "j-2NM10FZ9Q464V"
CLUSTER_ID = cluster_id


# Get Artifact location from S3
session = boto3.Session()
s3 = session.resource('s3')
bucket_name = "data-ingestion-scala-artifacts"
my_bucket = s3.Bucket(bucket_name)
jar_artifact = max([str(my_bucket_object.key) for my_bucket_object in my_bucket.objects.all() if \
                     str(my_bucket_object.key).endswith("jar")])
jar_artifact_location = "s3://" + bucket_name + "/" + jar_artifact

def retrieve_s3_file(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location'] 
    kwargs['ti'].xcom_push( key = 's3location', value = s3_location)

SPARK_TEST_STEPS = [
    {
        'Name': 'datajob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit', 
                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode','cluster',
                '--num-executors','2',
                '--driver-memory','512m',
                '--executor-memory','3g',
                '--executor-cores','2',
                #'s3://demo-wcd/wcd_final_project_2.11-0.1.jar',
                jar_artifact_location,
                '-p','Data-Ingestion-Project',
                '-i','Csv',
                '-o','parquet',
                #'-s','s3a://demo-wcd/banking.csv',
                '-s', "{{ task_instance.xcom_pull('parse_request', key='s3location') }}",
                '-d','s3://data-ingestion-output-path/',
                '-c','Year',
                '-m','append',
                '--input-options','header=true'
            ]
        }
    }
]


dag = DAG(
    'emr_job_flow_manual_steps_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)


parse_request = PythonOperator(task_id='parse_request',
                             provide_context=True,
                             python_callable=retrieve_s3_file,
                             dag=dag)


step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id=CLUSTER_ID,
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id=CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

step_adder.set_upstream(parse_request)
step_checker.set_upstream(step_adder)
