import os,sys
from collections import namedtuple
from typing import List




from finance_complaint.exception import FinanceException
from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.entity.config_entity import DataIngestionConfig
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
from finance_complaint.entity.metadata_entity import DataIngestionMetadata
from finance_complaint.logger import logger
from datetime import datatime 

DownloadUrl = namedtuple("DownloadUrl",["url","file_path","n_retry"])


class DataIngestion:

    def __init__(self, data_ingestion_config: DataIngestionConfig, n_retry:int =5):
        try:
            logger.info(f"{'>>'*20} Starting data ingestion.{'<<'*20}")
            self.data_ingestion_config = data_ingestion_config
            self.failed_download_urls = List[DownloadUrl] =[]
            self.n_retry = n_retry
            
        except Exception as e:
            raise FinanceException(e,sys)

    def get_required_interval(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys)
    
    def download_files(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys)

    def convert_files_to_parquet(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys)

    def retry_download_data(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys)
    
    def download_data(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys)


    def write_metadata(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys)
    
    def initiate_data_ingestion(self):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys)