 CREATE DATABASE IF NOT EXISTS META_DB;
 CREATE SCHEMA IF NOT EXISTS META;
 CREATE OR REPLACE TABLE META_DB.META.T_FILE_META_DETAILS ( 
            DATABASE STRING,
            SCHEMA STRING,
            TABLE_NAME STRING,
            DATASET_NAME STRING,
            VERSION STRING,
            ACTIVE_FL STRING,
            START_DATE DATE,
            END_DATE DATE,
            FILE_SCHEMA VARIANT,
            CREATED_BY STRING,
            CREATED_DATE TIMESTAMP);
         
        CREATE TABLE IF NOT EXISTS META_DB.META.T_FILE_VALIDATION (
          VALIDATION_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
          DATASET_NAME VARCHAR(16777216), 
          STATUS VARCHAR(16777216), 
          TYPE VARCHAR(16777216), 
          CREATED_DTS TIMESTAMP_NTZ(9), 
          CREATED_BY VARCHAR(16777216)
        );
            
        CREATE TABLE IF NOT EXISTS META_DB.META.T_FILE_VALIDATION_DETAILS (
          VALIDATION_DETAIL_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
          VALIDATION_ID NUMBER, 
          DATASET_NAME STRING,
          MSG VARCHAR(16777216), 
          DETAILS VARIANT,
          CREATED_DTS TIMESTAMP_NTZ(9), 
          CREATED_BY VARCHAR(16777216)
        );
         INSERT INTO META_DB.META.T_FILE_META_DETAILS
            SELECT *,PARSE_JSON('[{"FIPS": "STRING"}, {"ADMIN2": "STRING"}, {"PROVINCE_STATE": "STRING"}, {"COUNTRY_REGION": "STRING"}, {"LAST_UPDATE": "STRING"}, {"LAT": "STRING"}, {"LONG_": "STRING"}, {"CONFIRMED": "STRING"}, {"DEATHS": "STRING"}, {"RECOVERED": "STRING"}, {"ACTIVE": "STRING"}, {"COMBINED_KEY": "STRING"}, {"INCIDENT_RATE": "STRING"}, {"CASE_FATALITY_RATIO": "STRING"}]'),current_user(),current_timestamp()  from values
            ('MIRROR_DB','MIRROR','T_ML_CSSE_COVID_19_DAILY_REPORTS_TR','CSSE_COVID_19_DAILY_REPORTS','V1','Y','2021-01-01','9999-12-31');
        
 CREATE DATABASE IF NOT EXISTS MIRROR_DB;
 CREATE SCHEMA IF NOT EXISTS MIRROR;
  CREATE TABLE IF NOT EXISTS MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS_TR (
    FIPS STRING,
    ADMIN2 STRING,
    PROVINCE_STATE STRING,
    COUNTRY_REGION STRING,
    LAST_UPDATE STRING,
    LAT STRING,
    LONG_ STRING,
    CONFIRMED STRING,
    DEATHS STRING,
    RECOVERED STRING,
    ACTIVE STRING,
    COMBINED_KEY STRING,
    INCIDENT_RATE STRING,
    CASE_FATALITY_RATIO STRING,
    file_date TIMESTAMP,
    filename STRING,
    file_row_number STRING,
    file_last_modified TIMESTAMP,
    CREATED_DTS TIMESTAMP,
    CREATED_BY STRING
);
 CREATE DATABASE IF NOT EXISTS MIRROR_DB;
 CREATE SCHEMA IF NOT EXISTS MIRROR;
  CREATE TABLE IF NOT EXISTS MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS (
    FIPS STRING,
    ADMIN2 STRING,
    PROVINCE_STATE STRING,
    COUNTRY_REGION STRING,
    LAST_UPDATE STRING,
    LAT STRING,
    LONG_ STRING,
    CONFIRMED STRING,
    DEATHS STRING,
    RECOVERED STRING,
    ACTIVE STRING,
    COMBINED_KEY STRING,
    INCIDENT_RATE STRING,
    CASE_FATALITY_RATIO STRING,
    file_date TIMESTAMP,
    filename STRING,
    file_row_number STRING,
    file_last_modified TIMESTAMP,
    CREATED_DTS TIMESTAMP,
    CREATED_BY STRING
);
 CREATE DATABASE IF NOT EXISTS STAGE_DB;
 CREATE SCHEMA IF NOT EXISTS STAGE;
  CREATE TABLE IF NOT EXISTS STAGE_DB.STAGE.T_STG_CSSE_COVID_19_DAILY_REPORTS (
    FIPS FLOAT,
    ADMIN2 STRING,
    PROVINCE_STATE STRING,
    COUNTRY_REGION STRING,
    LAST_UPDATE STRING,
    LAT FLOAT,
    LONG_ FLOAT,
    CONFIRMED NUMBER,
    DEATHS NUMBER,
    RECOVERED NUMBER,
    ACTIVE NUMBER,
    COMBINED_KEY STRING,
    INCIDENT_RATE FLOAT,
    CASE_FATALITY_RATIO FLOAT,
    CREATED_DTS TIMESTAMP,
    CREATED_BY STRING,
    UPDATED_DTS TIMESTAMP,
    UPDATED_BY STRING,
    ACTIVE_FL STRING,
    EFFECTIVE_START_DATE TIMESTAMP,
    EFFECTIVE_END_DATE TIMESTAMP,
    UNIQUE_HASH_ID STRING,
    ROW_HASH_ID STRING
);

 CREATE STAGE IF NOT EXISTS MIRROR_DB.MIRROR.STG_CSSE_COVID_19_DAILY_REPORTS_S3
  URL='s3://rposam-devops-airflow/datasets/csse_covid_19_daily_reports/'
  CREDENTIALS=(AWS_KEY_ID='AWS_KEY_ID' AWS_SECRET_KEY='AWS_SECRET_KEY')
  ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID = 'aws/key');
  
 CREATE OR REPLACE FILE FORMAT MIRROR_DB.MIRROR.FF_CSSE_COVID_19_DAILY_REPORTS
        TYPE = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1      
        TRIM_SPACE=TRUE,
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT='YYYY-MM-DD',
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
        ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
        COMPRESSION = NONE;
        

 CREATE PIPE IF NOT EXISTS  MIRROR_DB.MIRROR.PIPE_CSSE_COVID_19_DAILY_REPORTS
    AUTO_INGEST = TRUE     
AS COPY INTO MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS_TR
        FROM (
            SELECT $1 as FIPS,$2 as ADMIN2,$3 as PROVINCE_STATE,$4 as COUNTRY_REGION,$5 as LAST_UPDATE,$6 as LAT,$7 as LONG_,$8 as CONFIRMED,$9 as DEATHS,$10 as RECOVERED,$11 as ACTIVE,$12 as COMBINED_KEY,$13 as INCIDENT_RATE,$14 as CASE_FATALITY_RATIO,$15 as FILE_DATE,metadata$filename as filename,metadata$file_row_number as file_row_number,metadata$file_last_modified as file_last_modified,
            current_timestamp as created_dts, current_user as created_by
            FROM '@MIRROR_DB.MIRROR.STG_CSSE_COVID_19_DAILY_REPORTS_S3'
        )
        PATTERN = '.*\.csv$'
        FILE_FORMAT = (FORMAT_NAME=MIRROR_DB.MIRROR.FF_CSSE_COVID_19_DAILY_REPORTS)
        ON_ERROR = 'CONTINUE'  ;

-- To load all the available files under the path(Historical Data)
ALTER PIPE MIRROR_DB.MIRROR.PIPE_CSSE_COVID_19_DAILY_REPORTS REFRESH;

-- Create event notification on corresponding bucket using the arn.
-- Get the arn using below query.
-- Unless you create event notification, snowpipe is not going to copy data.
select  SYSTEM$PIPE_STATUS('MIRROR_DB.MIRROR.PIPE_CSSE_COVID_19_DAILY_REPORTS');

-- Creating table to log snowpipe failures
CREATE TABLE IF NOT EXISTS MIRROR_DB.MIRROR.T_SNOWPIPE_ERRORS (
    PIPE_NAME STRING,
    FILE_NAME STRING,
    ERROR_MESSAGE STRING,
    TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Logging errors into a snowpipe error table, so that every pipeline status would be knowing.
 CREATE TASK IF NOT EXISTS  MIRROR_DB.MIRROR.TASK_LOG_SNOWPIPE_ERRORS
SCHEDULE = '1 MINUTE'
AS
INSERT INTO MIRROR_DB.MIRROR.T_SNOWPIPE_ERRORS (PIPE_NAME, FILE_NAME, ERROR_MESSAGE)
SELECT 
    'PIPE_CSSE_COVID_19_DAILY_REPORTS' AS pipe_name,
    FILE_NAME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.LOAD_HISTORY_BY_PIPE('PIPE_CSSE_COVID_19_DAILY_REPORTS'))
WHERE STATUS = 'LOAD_FAILED';


CREATE OR REPLACE STREAM MIRROR_DB.MIRROR.STREAM_CSSE_COVID_19_DAILY_REPORTS_TR ON TABLE MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS_TR
         append_only = true; 
         
CREATE OR REPLACE TASK MIRROR_DB.MIRROR.TASK_CSSE_COVID_19_DAILY_REPORTS_TR_VALIDATION
            WAREHOUSE = COMPUTE_WH
            AFTER MIRROR_DB.MIRROR.TASK_LOG_SNOWPIPE_ERRORS            
            WHEN SYSTEM$STREAM_HAS_DATA('MIRROR_DB.MIRROR.STREAM_CSSE_COVID_19_DAILY_REPORTS_TR') -- Skips execution if the stream has no data
            AS
            CALL META_DB.META.VALIDATE_FILE_AND_TABLE(
                'CSSE_COVID_19_DAILY_REPORTS',
                'STG_CSSE_COVID_19_DAILY_REPORTS_S3',
                'MIRROR_DB',
                'MIRROR',
                'T_ML_CSSE_COVID_19_DAILY_REPORTS_TR',
                '',
                1
            );
        
CREATE OR REPLACE TASK MIRROR_DB.MIRROR.TASK_CSSE_COVID_19_DAILY_REPORTS
            SCHEDULE = 'USING CRON 0 23 * * 1-5 UTC'
            WAREHOUSE = 'COMPUTE_WH'
            -- without condition, always try to execute the task
            WHEN
             SYSTEM$STREAM_HAS_DATA('MIRROR_DB.MIRROR.STREAM_CSSE_COVID_19_DAILY_REPORTS_TR') -- skips when stream has no data
            AS
            -- you could write merge statement incase you wanted upsert target, src as stream
            INSERT INTO MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS
            SELECT *  exclude (METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID)
            FROM MIRROR_DB.MIRROR.STREAM_CSSE_COVID_19_DAILY_REPORTS_TR; 
 ALTER TASK MIRROR_DB.MIRROR.TASK_CSSE_COVID_19_DAILY_REPORTS RESUME;
        
CREATE OR REPLACE STREAM STAGE_DB.STAGE.STREAM_CSSE_COVID_19_DAILY_REPORTS ON TABLE MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS
         append_only = true; 
         
CREATE OR REPLACE TASK STAGE_DB.STAGE.TASK_CSSE_COVID_19_DAILY_REPORTS
            SCHEDULE = 'USING CRON 0 23 * * 1-5 UTC'
            WAREHOUSE = 'COMPUTE_WH'
            -- without condition, always try to execute the task
            WHEN
             SYSTEM$STREAM_HAS_DATA('STAGE_DB.STAGE.STREAM_CSSE_COVID_19_DAILY_REPORTS') -- skips when stream has no data
            AS
            -- you could write merge statement incase you wanted upsert target, src as stream
            INSERT INTO STAGE_DB.STAGE.T_STG_CSSE_COVID_19_DAILY_REPORTS
            SELECT *  exclude (METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID)
            FROM STAGE_DB.STAGE.STREAM_CSSE_COVID_19_DAILY_REPORTS; 
 ALTER TASK STAGE_DB.STAGE.TASK_CSSE_COVID_19_DAILY_REPORTS RESUME;
        


CREATE OR REPLACE PROCEDURE META_DB.META.VALIDATE_FILE_AND_TABLE(dataset_name STRING,stage_name STRING,database STRING,schema STRING,  table_name STRING,run_date STRING,header_row_no NUMBER)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'compare_csv_with_table'
AS $$

import json
import os

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, upper
from snowflake.snowpark.types import StructType, StringType, StructField
from datetime import datetime

def compare_csv_with_table(session: Session,dataset_name, stage_name,database,schema,run_date, table_name,header_row_no=1):
    # Construct the full path to the file in the stage
    executed_sqls = ""
    list_of_files = session.sql(f"list @{database}.{schema}.{stage_name}").collect()
    for full_file_path in list_of_files:
        file_path = f"""@{database}.{schema}.{stage_name}/{"/".join(full_file_path["name"].split("/")[1:])}"""

        executed_sqls += "file path query: " + file_path + "\n"

        # Read staged file into a DataFrame
        stage_df = session.read.option("FIELD_OPTIONALLY_ENCLOSED_BY", '"').csv(file_path)

        # Extract the third row as header
        header_row = stage_df.limit(header_row_no).collect()[-1]

        executed_sqls += "header_row: " + str(header_row) + "\n"

        if run_date in ["",None]:
            run_date_str = datetime.today().strftime("%Y-%m-%d")
        else:
            run_date_str = run_date

        file_defined_cols_query = f"""
            select file_schema from META_DB.META.T_FILE_META_DETAILS
            where database = '{database}'
            and schema = '{schema}'
            and table_name = '{table_name}'
            and active_fl = 'Y'
            and '{run_date_str}' between start_date and end_date """

        executed_sqls +=  "file_defined_cols_query: " + file_defined_cols_query + "\n"

        configured_file_schema = session.sql(file_defined_cols_query).collect()[0]["FILE_SCHEMA"]

        executed_sqls += "configured_file_schema: " + configured_file_schema + "\n"

        file_schema = json.loads(configured_file_schema)
        configured_file_cols = ",".join([key for schema in file_schema for key in schema.keys()])

        executed_sqls += "configured_file_cols: " + configured_file_cols + "\n"

        received_file_cols_dict = header_row.asDict().items()
        received_file_cols_str = ",".join([col_name.strip('"').replace(" ","_").upper() for col, col_name in received_file_cols_dict])

        executed_sqls += "received_file_cols_str: " + received_file_cols_str + "\n"

        if configured_file_cols != received_file_cols_str:
            insert_query = f""" INSERT INTO META_DB.META.T_FILE_VALIDATION(DATASET_NAME, STATUS, TYPE ,CREATED_DTS,CREATED_BY)
                    SELECT '{dataset_name.upper()}','ERROR','SCHEMA_MISMATCH',CURRENT_TIMESTAMP(),CURRENT_USER()
                """
            executed_sqls += insert_query + " \n"
            _ = session.sql(insert_query).collect()

        index=0
        datatypes = []
        for key, value in received_file_cols_dict:
            if value is None:
                index+=1
                datatypes.append(StructField(f"None_{index}".upper(), StringType()))
            else:
                datatypes.append(StructField(value.replace(" ","_").upper(), StringType()) )

        snowpark_schema = StructType(datatypes)

        executed_sqls += "snowpark_schema: " + str(snowpark_schema) + "\n"

        # Convert column names to uppercase
        schema_stage_df = session.read.option("FIELD_OPTIONALLY_ENCLOSED_BY", '"').options({"SKIP_HEADER":header_row_no}).schema(schema=snowpark_schema).csv(file_path)

        table_cols_query = f"""
        SELECT LISTAGG(COLUMN_NAME,',') WITHIN GROUP(ORDER BY ORDINAL_POSITION)  AS COLS
        FROM {database}.INFORMATION_SCHEMA.COLUMNS C
        WHERE TABLE_SCHEMA ='{schema}'
        AND TABLE_NAME = '{table_name}'
        AND COLUMN_NAME NOT IN ('CREATED_BY','CREATED_DTS','FILE_DATE','FILE_LAST_MODIFIED','FILE_ROW_NUMBER','FILENAME',
        'ROW_HASH_ID','UNIQUE_HASH_ID','UPDATED_DTS','UPDATED_BY')
        """

        executed_sqls += "table_cols_query: " + table_cols_query + "\n"

        table_cols = session.sql(table_cols_query).collect()[0]["COLS"]

        executed_sqls += "table_cols: " + table_cols + "\n"

        tr_table_query = f"""select {table_cols} from {database}.{schema}.{table_name} where FILE_DATE = '{run_date_str}' """

        executed_sqls += "tr_table_query: " + tr_table_query + "\n"

        # Load the Snowflake table data
        tr_table_df = session.sql(tr_table_query)

        # Find differences: Rows in CSV but not in the table
        data_diff = schema_stage_df.minus(tr_table_df)

        executed_sqls += f"tr_table_query: Has no of mismatch records count: {data_diff.count()} \n"

        if data_diff.count()>0:
            sample_10_records_mismatch = data_diff.limit(10).collect()

            insert_query = f""" INSERT INTO META_DB.META.T_FILE_VALIDATION_DETAILS(DATASET_NAME, MSG, DETAILS ,CREATED_DTS,CREATED_BY)
                                SELECT '{dataset_name.upper()}','Has no of mismatch records count:{data_diff.count()}',PARSE_JSON('{json.dumps(sample_10_records_mismatch)}'),CURRENT_TIMESTAMP(),CURRENT_USER()
                            """
            executed_sqls += insert_query + " \n"
            _ = session.sql(insert_query).collect()

        remove_stage_file = f"remove {file_path}"
        executed_sqls += remove_stage_file + " \n"

        session.sql(remove_stage_file).collect()

        return executed_sqls

$$;


 /* # Copy Procedure Python code to a file add following lines at the end of the file and debug incase procedure has errors or not working as expected 

                # Connect to Snowflake
                connection_parameters = {
                    "user":os.getenv("SNOWFLAKE_USER"),
                    "password":os.getenv("SNOWFLAKE_PASSWORD"),
                    "account":os.getenv("SNOWFLAKE_ACCOUNT"),
                    "database":os.getenv("SNOWFLAKE_DATABASE"),
                    "schema":os.getenv("SNOWFLAKE_SCHEMA"),
                    "warehouse":os.getenv("SNOWFLAKE_WAREHOUSE"),
                    "role":os.getenv("SNOWFLAKE_ROLE")}


                # Create a Snowpark session
                session = Session.builder.configs(connection_parameters).create()

                result = compare_csv_with_table(session=session,dataset_name='NETFLIX_MOVIES_AND_TV_SHOWS',
                                       database="MIRROR_DB",schema="MIRROR",
                                       stage_name = "STG_NETFLIX_MOVIES_AND_TV_SHOWS",
                                       table_name = 'T_ML_NETFLIX_MOVIES_AND_TV_SHOWS_TR',
                                        run_date="2024-12-21"
                    )

                print(result) */
                