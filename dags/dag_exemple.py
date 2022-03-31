from airflow import models, utils
import datetime
from airflow.contrib.operators import dataproc_operator

from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
#from airflow.operators.bash import BashOperator
from airflow.models import Variable
import os


import logging



DAG_NAME = 'process-drugs'

CLUSTER_NAME= 'cluster-' + DAG_NAME



#Get composer environement variables

BDD_PROJECT_NAME  = str(Variable.get('bdd_project_name')) 
BDD_HOSTNAME      = str(Variable.get('bdd_hostname')) 
BDD_INSTANCE_NAME = str(Variable.get('bdd_instance_name'))  
BDD_NAME          =	str(Variable.get('bdd_name'))  
BDD_PORT          = str(Variable.get('bdd_port'))  
BDD_USER          = str(Variable.get('bdd_user')) 
PWD_PSQL          = str(Variable.get('pwd_psql')) 
BUCKET_PROCESSING =	str(Variable.get('bucket_processing')) 
BUCKET_REPORTING  =	str(Variable.get('bucket_reporting')) 
BUCKET_CREDENTIAL =	str(Variable.get('bucket_credential')) 
BUCKET_DELIVERY   =	str(Variable.get('bucket_delivery')) 
KEY_NAME          =	str(Variable.get('key_name')) 
PROJECT_NAME      = str(Variable.get('project_name')) 
PROJECT_REGION    = str(Variable.get('project_region'))	
PROJECT_PREFIX    = str(Variable.get('project_prefix'))	

#on défini le start date pour notre dag
args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime.datetime(2022, 2, 23, 0, 0, 0),
    'depends_on_past': False,
    'email': ["sidali.diouani.ext@relevanc.com"],
    'email_on_failure': True
}


spark_args_standard = {
    'spark.executor.instances': '4',
    'spark.executor.cores': '5',
    'spark.executor.memory': '8G',
    'spark.executor.memoryOverhead': '3G'
}

dataproc_properties = {
    'spark:spark.scheduler.mode' : 'FIFO'
}

#1 Instantiation du cluster dataproc en utilisant un script init (pour communiquer les env vars composer/airflow) + selection d'une custom image familly
#2 Lancement du premier traitement (géneration du JSON resutlat)
#3 Lancement du traitement adhoc
#4 decomissionnement du cluster crée en 1
with DAG('drugs.process_data_drugs', default_args=args, max_active_runs=1, concurrency=1,
         ) as subdag_data_drugs:


    create_cluster=dataproc_operator.DataprocClusterCreateOperator(
        task_id='create1-%s' % CLUSTER_NAME,
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_NAME,
        num_masters=1,
        num_workers=2,
        master_machine_type='n1-highmem-8',
        worker_machine_type='n1-highmem-8',
        subnetwork_uri='projects/%s/regions/%s/subnetworks/default' %(PROJECT_NAME, PROJECT_REGION),
        custom_image='family/drugs-mesure',
        init_actions_uris=["gs://%s/scripts/init_env_vars.sh" %BUCKET_PROCESSING],
        metadata={  'BDD_PROJECT_NAME' :    BDD_PROJECT_NAME  ,
                    'BDD_HOSTNAME'  :       BDD_HOSTNAME      ,
                    'BDD_INSTANCE_NAME'	:   BDD_INSTANCE_NAME ,
                    'BDD_NAME' :	        BDD_NAME          ,
                    'BDD_PORT' :	        BDD_PORT          ,
                    'BDD_USER' :	        BDD_USER          ,
                    'PWD_PSQL' :            PWD_PSQL          ,
                    'BUCKET_PROCESSING' :	BUCKET_PROCESSING ,
                    'BUCKET_REPORTING'  :	BUCKET_REPORTING  ,
                    'BUCKET_CREDENTIAL' :	BUCKET_CREDENTIAL ,
                    'BUCKET_DELIVERY'   :	BUCKET_DELIVERY   ,
                    'KEY_NAME'       :	    KEY_NAME          ,
                    'PROJECT_NAME'   :	    PROJECT_NAME      ,
                    'PROJECT_REGION' :	    PROJECT_REGION    ,
                    'PROJECT_PREFIX' :	    PROJECT_PREFIX    
        },
        storage_bucket='%s' %(BUCKET_PROCESSING),
        region='%s' %PROJECT_REGION,
        zone='%s-b' %PROJECT_REGION,
        auto_delete_ttl=18000,
        properties=dataproc_properties
        )   


    task_processing = dataproc_operator.DataProcPySparkOperator(
        task_id='task_processing',
        project_id=PROJECT_NAME,
        cluster_name='%s' % (CLUSTER_NAME),
        region=PROJECT_REGION,
        main='gs://%s/scripts/drugs-processing/main-drugs.py' % BUCKET_PROCESSING,
        pyfiles=['gs://%s/scripts/drugs-processing/utils.zip' % BUCKET_PROCESSING],
        dataproc_pyspark_properties=spark_args_standard,
        arguments=['--date',  "{{ prev_ds  }}" ],
        
        properties=dataproc_properties            
        )

   
    task_adhoc = dataproc_operator.DataProcPySparkOperator(
        task_id='task_adhoc',
        project_id=PROJECT_NAME,
        cluster_name='%s' % (CLUSTER_NAME),
        region=PROJECT_REGION,
        main='gs://%s/scripts/drugs--adhoc/adhoc.py' % BUCKET_PROCESSING,
        pyfiles=['gs://%s/scripts/drugs--adhoc/utils.zip' % BUCKET_PROCESSING],
        dataproc_pyspark_properties=spark_args_standard,
        arguments=['--date', "{{ prev_ds }}" ],
        properties=dataproc_properties
        )


    delete_cluster=dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete-%s' % CLUSTER_NAME,
        project_id=PROJECT_NAME,
        cluster_name=CLUSTER_NAME,
        region=PROJECT_REGION,
        zone='%s-b' %PROJECT_REGION,
        trigger_rule='all_done'
    )

create_cluster.dag = subdag_data_drugs
create_cluster.set_downstream(task_processing)
task_processing.set_downstream(task_adhoc)
task_adhoc.set_downstream(delete_cluster)

with models.DAG('mesure-drugs',
          schedule_interval='0 4 * * *',
          default_args=args, max_active_runs=1) as dag :

    data_drugs = SubDagOperator(
        task_id='process_data_drugs',
        subdag= subdag_data_drugs,
        default_args=args,
        dag=dag
    )

    data_drugs
