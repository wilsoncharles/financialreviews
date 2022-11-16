from time import strptime
from finance_complaint.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig
from finance_complaint.constant.training_pipeline_config import *
from finance_complaint.constant import TIMESTAMP


class FinanceConfig:

    def __init__(self, pipeline_name = PIPELINE_NAME , timestamp = TIMESTAMP):

        """
        Creates configuration for training pipeline

        """
        self.timestamp = timestamp
        self.pipeline_name = pipeline_name
        self.pipeline_config = self.get_pipeline_config()

    def get_pipeline_config(self) -> TrainingPipelineConfig:
        """
        Function to return training pipeline configuration

        """
        try:
            artifact_dir = PIPELINE_ARTIFACT_DIR
            pipeline_config = TrainingPipelineConfig(pipeline_name = self.pipeline_name,
                                                     artifact_dir = self.artifact_dir)

            logger.info(f"Pipeline configuration : {pipeline_config}")

            return pipeline_config
        except Exception as e:
            raise FinanceException(e,sys)

    def get_data_ingestion_config(self, from_date = DATA_INGESTION_MIN_START_DATE, to_date=None) \
            -> DataIngestionConfig:

        """
        Min start date is the hard coded value and new start date can't be less than min start date
    
        """

        min_start_date = datetime.strptime(DATA_INGESTION_MIN_START_DATE, "%Y-%m-%d")
        from_start_obj = datetime.strptime(from_date, "%Y-%m-%d")
        if from_date_obj < min_start_date:
            from_date = DATA_INGESTION_MIN_START_DATE
        if to_date is None:
            to_date = datetime.now().strptime("%Y-%m-%d")

        """
        master directory for data ingestion
        Details are stored in the metadata to avoid redundancy in downloading
        """

        data_ingestion_master_dir = os.path.join(self.pipeline_config.artifact_dir, DATA_INGESTION_DIR)

        #time based directory for each run
        data_ingestion_dir = os.path.join(data_ingestion_master_dir, self.timestamp)

        metadata_file_path = os.path.join(data_ingestion_master_dir, DATA_INGESTION_METADATA_FILE_NAME )

        data_ingestion_metadata = DataIngestionMetadata(metadata_file_path=metadata_file_path)

        if data_ingestion_metadata.is_metadata_file_present:
            metadata_info = data_ingestion_metadata.get_metadata_info()
            from_date = metadata_info.to_date
        
        
        data_ingestion_config = DataIngestionConfig(
            from_date = from_date,
            to_date = to_date,
            data_ingestion_dir = data_ingestion_dir,
            download_dir = os.path.join(data_ingestion_dir, DATA_INGESTION_DOWNLOADED_DATA_DIR),
            file_name = DATA_INGESTION_FILE_NAME,
            feature_store_dir = os.path.join(data_ingestion_master_dir, DATA_INGESTION_FEATURE_STORE_DIR),
            failed_dir = os.path.join(data_ingestion_dir, DATA_INGESTION_FAILED_DIR),
            metadata_file_path = metadata_file_path,
            data_source_url = DATA_INGESTION_DATA_SOURCE_URL
        )
        
        logger.info(f"Data ingestion config: {data_ingestion_config}")
        return data_ingestion_config