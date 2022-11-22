from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.component import DataIngestion, DataValidation
from finance_complaint.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
import sys


class TrainingPipeline:

    def __init__(self, finance_config: FinanceConfig):
        self.finance_config: FinanceConfig = finance_config

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            data_ingestion_config = self.finance_config.get_data_ingestion_config()
            data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            return data_ingestion_artifact

        except Exception as e:
            raise FinanceException(e, sys)

    def start_data_validation(self, data_ingestion_artifact: DataIngestionArtifact) -> DataValidationArtifact:
        try:
            
            data_validation_config = self.finance_config.get_data_validation_config()

            data_validation = DataValidation(data_ingestion_artifact = data_ingestion_artifact,
                                             data_validation_config = data_validation_config)
            data_validation_artifact = data_validation.initiate_data_validation()
            return data_validation_artifact
        
        except Exception as e:
            raise FinanceException(e, sys)
    

    def start(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact = data_ingestion_artifact)
            
        except Exception as e:
            raise FinanceException(e, sys)