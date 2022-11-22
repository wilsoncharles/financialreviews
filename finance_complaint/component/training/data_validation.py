import os
import sys
from collections import namedtuple
from typing import List, Dict 
from pyspark.sql import DataFrame


from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger 
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
from finance_complaint.entity.config_entity import DataValidationConfig
from finance_complaint.entity.artifact_entity import DataValidationArtifact
from finance_complaint.entity.schema import FinanceDataSchema
from finance_complaint.config.spark_manager import spark_session


COMPLAINT_TABLE = "complaint"
ERROR_MESSAGE = "error_msg"
MissingReport = namedtuple("MissingReport", ["total_row", "missing_row", "missing_percentage"])

class DataValidation(FinanceDataSchema):

    def __init__(self,
                 data_validation_config : DataValidationConfig,
                 data_ingestion_artifact : DataIngestionArtifact,
                 table_name : COMPLAINT_TABLE,
                 schema = FinanceDataSchema()):
        try:
            super().__init__()
            self.data_ingestion_artifact: DataIngestionArtifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.table_name = table_name
            self.schema = schema
        except Exception as e:
            raise FinanceException(e,sys)

    def read_data(self) -> DataFrame:  
        try:
            dataframe: DataFrame = spark_session.read.parquet(
                self.data_ingestion_artifact.feature_store_file_path
            ).limit(10000)
            logger.info(f"Data frame is created using file {self.data_ingestion_artifact.feature_store_file_path}")   
            logger.info(f"Number of rows: {dataframe.count()} and column: {len(dataframe.columns)}")
            dataframe, _ = dataframe.randomSplit([0.001, 0.999])

            return dataframe
        except Exception as e:
            raise FinanceException(e,sys)  

    @staticmethod
    def get_missing_report(dataframe: DataFrame) -> Dict[str, MissingReport]:
        try:
            missing_report : Dict[str:MissingReport] = dict()
            logger.info(f"Preparing missing report for each column")
            number_of_row = dataframe.count()

            for column in dataframe.columns():
                missing_row = dataframe.filter(f"{column} is null").count()
                missing_percentage = (missing_row*100)/number_of_row
                missing_report[column] = MissingReport(total_row= number_of_row,
                                                       missing_row = missing_row,
                                                       missing_percentage = missing_percentage)
                logger.info(f"Missing report prepared: {missing_report}")
                return missing_report


        except Exception as e:
            raise FinanceException(e,sys)

    