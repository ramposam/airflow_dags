 CREATE DATABASE IF NOT EXISTS MIRROR_DB;
 CREATE SCHEMA IF NOT EXISTS MIRROR;
 CREATE OR REPLACE TABLE MIRROR_DB.MIRROR.T_ML_NETFLIX_MOVIES_AND_TV_SHOWS_TR (
    TITLE STRING,
    TYPE STRING,
    GENRE STRING,
    RELEASE_YEAR STRING,
    RATING STRING,
    DURATION STRING,
    COUNTRY STRING,
    file_date TIMESTAMP,
    filename STRING,
    file_row_number STRING,
    file_last_modified TIMESTAMP,
    CREATED_DTS TIMESTAMP,
    CREATED_BY STRING
);