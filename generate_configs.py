import os

from helper_utils.file_utils import read_and_infer, write_to_json_file
from meta_classes import DatasetConfigs, DatasetVersion, DatasetMirror, DatasetStage
from pathlib import  Path


class ConfigTemplate():
    def __init__(self, file_path):
        self.file_path = file_path

    def get_mirror_schema(self, columns):
        schema = {}
        for col_name in columns:
            schema[col_name.replace(" ", "_").upper()] = "STRING"
        return schema

    def get_stage_schema(self, data_types):
        schema = {}
        for col_name, col_dtypes in data_types.items():
            schema[col_name.replace(" ", "_").upper()] = col_dtypes["snowflake_dtype"]
        return schema

    def generate_configs(self):
        delimiter, columns, data_types = read_and_infer(self.file_path)
        mirror_schema = self.get_mirror_schema(data_types)
        stage_schema = self.get_stage_schema(data_types)

        dataset_name = os.path.basename(os.path.dirname(self.file_path))

        configs_root_dir = os.path.join(os.getcwd(),"generated_configs")
        Path(configs_root_dir).mkdir(parents=True, exist_ok=True)

        dataset_dir = os.path.join(configs_root_dir,dataset_name)
        Path(dataset_dir).mkdir(parents=True, exist_ok=True)

        dataset_configs_path = os.path.join(dataset_dir,f"ds_{dataset_name}.json")
        ds_configs = DatasetConfigs(dataset_name=dataset_name,
                                    snowflake_stage_name=f"STG_{dataset_name}".upper())

        write_to_json_file(data=ds_configs.__dict__,file_path=dataset_configs_path)

        dataset_mirror_dir = os.path.join(dataset_dir, "mirror")
        Path(dataset_mirror_dir).mkdir(parents=True, exist_ok=True)

        dataset_configs_mirror_ver_path = os.path.join(dataset_mirror_dir, f"ds_{dataset_name}_mirror_ver.json")
        ds_mirror_ver_configs = DatasetVersion(dataset_name=dataset_name)

        write_to_json_file(data=ds_mirror_ver_configs.__dict__, file_path=dataset_configs_mirror_ver_path)

        file_format = {
                    "delimiter": delimiter,
                    "skip_header": 1,
                    "compressed": True
                }
        dataset_configs_mirror_v1_path = os.path.join(dataset_mirror_dir, f"ds_{dataset_name}_mirror_v1.json")
        ds_mirror_v1_configs = DatasetMirror(table_name = f"T_ML_{dataset_name}".upper(),
                            table_schema = mirror_schema,
                            file_format = file_format,
                            file_schema = mirror_schema,
                            file_name_pattern = "{file_date_pattern}.csv",
                            file_path = f"rposam-devops-airflow/datasets/{dataset_name}",
                            file_date_pattern =  "DD-MM-YYYY")

        write_to_json_file(data=ds_mirror_v1_configs.__dict__, file_path=dataset_configs_mirror_v1_path)

        dataset_stg_dir = os.path.join(dataset_dir, "stage")
        Path(dataset_stg_dir).mkdir(parents=True, exist_ok=True)

        dataset_configs_stage_ver_path = os.path.join(dataset_stg_dir, f"ds_{dataset_name}_stage_ver.json")
        ds_stage_ver_configs = DatasetVersion(dataset_name=dataset_name)

        write_to_json_file(data=ds_stage_ver_configs.__dict__, file_path=dataset_configs_stage_ver_path)

        dataset_configs_stage_v1_path = os.path.join(dataset_stg_dir, f"ds_{dataset_name}_stage_v1.json")
        ds_stage_v1_configs = DatasetStage(table_name=f"T_STG_{dataset_name}".upper(),
                                             table_schema=mirror_schema)

        write_to_json_file(data=ds_stage_v1_configs.__dict__, file_path=dataset_configs_stage_v1_path)


file_path = r"C:\Users\Asus\PycharmProjects\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\01-01-2021.csv"

ConfigTemplate(file_path).generate_configs()