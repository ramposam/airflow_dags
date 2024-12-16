import json
import os
from datetime import datetime
from airflow import DAG
from importlib import import_module

from config_reader import ConfigReader

dag_template = """
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

"""
default_args = """
# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

date_pattern = datetime.strftime(datetime.today(), "%Y-%m-%d")

"""

# Main program
def main(dag_template):

    dataset_name = 'csse_covid_19_daily_reports'  # Path to your config file
    configs_root_dir = os.path.join(os.getcwd(), "configs", dataset_name)
    config = ConfigReader(configs_root_dir,dataset_name).read_configs()
    print(config)
    for task in config["tasks"]:
        if task == "acq_task":
            dag_template += "from operators.acquisition_operator import AcquisitionOperator" +"\n"
        elif task == "download_task":
            dag_template += "from operators.download_operator import DownloadOperator" +"\n"
        elif task == "load_task":
            dag_template += "from operators.load_operator import LoadOperator" +"\n"
        elif task == "copy_task":
            dag_template += "from operators.snowflake_copy_operator import SnowflakeCopyOperator" +"\n"

    dag_template += default_args

    dag_body = f"""
    
# Define the DAG
with DAG(
    dag_id="{config["dataset_name"]}_dag",
    default_args=default_args,
    description="A simple DAG with a Data ingestion",
    schedule_interval={config["schedule_interval"]},  # No schedule, triggered manually
    start_date=datetime({config["start_date"]}),
    catchup={config["load_historical_data"]},
) as dag:

    start = EmptyOperator(
        task_id="start"
    )


    # End task
    end = EmptyOperator(
        task_id="end"
    )

    """

    dag_template += dag_body
    if "acq_task" in config["tasks"]:
        dag_template +="""
     # Task 1: Using the AcquisitionOperator
    acq_task = AcquisitionOperator(
        task_id="s3_file_check",
        s3_conn_id="S3_CONN_ID",
        bucket_name="rposam-devops-airflow",
        dataset_dir=f"datasets/{date_pattern}/",
        file_pattern="Netflix_Movies_and_TV_Shows.csv"
    ) 
        """
    if "download_task" in config["tasks"]:
         dag_template +="""
    download_task = DownloadOperator(
        task_id="download_file_from_s3",
        s3_conn_id="S3_CONN_ID",
        bucket_name="rposam-devops-airflow",
        dataset_dir=f"datasets/{date_pattern}/",
        file_name="Netflix_Movies_and_TV_Shows.csv"
    )
        """
    if "load_task" in config["tasks"]:
        dag_template +="""
    load_task = LoadOperator(
        task_id="move_file_to_snowflake",
        snowflake_conn_id="SNOWFLAKE_CONN_ID",
        stage_name="DEVOPS_DB.DEVOPS.STG_DATASET_DUMP"
    )
        """
    if "copy_task" in config["tasks"]:
        dag_template += """
    copy_task = SnowflakeCopyOperator(
        task_id="copy_file_from_stage",
        snowflake_conn_id="SNOWFLAKE_CONN_ID",
        stage_name="DEVOPS_DB.DEVOPS.STG_DATASET_DUMP",
        table_name="DEVOPS_DB.DEVOPS.T_NETFLIX_DATA"
    )
        """

    dag_tasks = f"""  
    # Define task dependencies
    start >>  {" >> ".join(config["tasks"])} >> end

    """
    dag_template += dag_tasks
    return dag_template

if __name__ == "__main__":
    dag_data = main(dag_template)
    print(dag_data)
