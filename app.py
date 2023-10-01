# [START composer_spark]
"""Example Airflow DAG that creates a Cloud Dataproc cluster, runs the spark
wordcount code, and deletes the cluster.
This DAG relies on three Airflow variables
https://airflow.apache.org/concepts.html#variables
* gcp_project - Google Cloud Project to use for the Cloud Dataproc cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataproc cluster should be
  created.
* gcs_bucket - Google Cloud Storage bucket to use for result of spark job.
  See https://cloud.google.com/storage/docs/creating-buckets for creating a
  bucket.
"""
import datetime
import os
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

# Output file for Cloud Dataproc job.
output_file = os.path.join(
    models.Variable.get('gcs_bucket'), 'wordcount',
    datetime.datetime.now().strftime('%Y%m%d-%H%M%S')) + os.sep

# # Path to Spark wordcount example available on every Dataproc cluster.
# SPARK_JAR = (
#     '/usr/lib/spark/jars/wordCount.jar'
# )

# Arguments to pass to Cloud Dataproc job.
input_file = 'gs://pub/shakespeare/rose.txt'
spark_args = ['wordcount', input_file, output_file]
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}
# [START composer_spark_schedule]
with models.DAG(
        'composer_dataproc_spark',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # [END composer_spark_schedule]

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/code.html#default-variables
        cluster_name='composer-spark-cluster-{{ ds_nodash }}',
        num_workers=2,
        region='asia-southeast2',
        zone=models.Variable.get('gce_zone'),
        image_version='2.0',
        master_machine_type='e2-standard-2',
        worker_machine_type='e2-standard-2')
    
    # Run the Spark wordcount code that have been upload on the Cloud Dataproc cluster
    # master node.
    run_dataproc_spark = dataproc_operator.DataProcSparkOperator(
        task_id='run_spark_job',
        main_class='',  # Leave this empty for Python-based Spark jobs
        cluster_name='composer-spark-cluster-{{ ds_nodash }}',
        region='asia-southeast2',
        dataproc_spark_python='/usr/lib/spark/python/bin/spark-submit',  # Path to the spark-submit script
        dataproc_pyspark_main='gs://path/to/your/wordCount.py',  # Replace with the GCS path  Python script
        dataproc_pyspark_args=spark_args,  # Replace with  Python script arguments
        dag=dag,
    )


    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        region='asia-southeast2',
        cluster_name='composer-spark--cluster-{{ ds_nodash }}',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)
    # [START composer_spark_steps]
    
    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster
    # [END composer_spark_steps]
# [END composer_spark_tutorial]