import os

from core_utils.generate_configs import ConfigTemplate
from core_utils.dag_generator import DagGenerator


if __name__ == "__main__":

    file_path = r"C:\Users\Asus\PycharmProjects\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\01-01-2021.csv"
    ConfigTemplate(bucket= "rposam-devops-airflow",file_path=file_path,
                   start_date="2021,1,1",catchup=True,datetime_format="MM-DD-YYYY").generate_configs()

    configs_dir = os.path.join(os.getcwd(),"configs")

    dag_gen = DagGenerator(configs_dir=configs_dir,dataset_name="csse_covid_19_daily_reports")
    dag_gen.generate_dag_ddls()