from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime

@dataclass
class DatasetConfigs:
    dataset_name: str
    snowflake_stage_name: str
    tasks: List[str] = field(
        default_factory=lambda: ["acq_task",
                                 "download_task",
                                 "load_task",
                                 "copy_task"])
    mirror_layer: Dict = field(default_factory=lambda: {"database": "MIRROR_DB", "schema": "mirror"})
    stage_layer: Dict = field(default_factory=lambda: {"database": "STAGE_DB", "schema": "STAGE"})
    s3_connection_id: str = field(default="S3_CONN_ID")
    snowflake_connection_id: str = field(default="SNOWFLAKE_CONN_ID")
    start_date: str = field(default=datetime.strftime(datetime.today(),"%Y,%m,%d"))
    load_historical_data : str = field(default=False)
    schedule_interval: Optional[str] = None


@dataclass
class DatasetVersion:
    dataset_name: str
    versions: List[Dict] = field(default_factory=lambda: [{
        "start_date": "2021-01-01",
        "end_date": "9999-12-31"}])


@dataclass
class DatasetMirror:
    table_name: str
    table_schema: Dict
    file_format: Dict
    file_schema: Dict
    file_name_pattern: str
    file_path: str
    file_date_pattern: str = field(default="YYYY-MM-DD")


@dataclass
class DatasetStage:
    table_name: str
    table_schema: Dict
    transformations: List[Dict] = field(default_factory=lambda: [{"name": "select_expr",
                                                                  "expr": ["*"]}])
