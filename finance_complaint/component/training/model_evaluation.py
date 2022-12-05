from finance_complaint.entity.artifact_entity import ModelEvaluationArtifact, DataValidationArtifact, ModelTrainerArtifact
from finance_complaint.entity.config_entity import ModelEvaluationConfig 
from finance_complaint.entity.schema import FinanceDataSchema 
from finance_complaint.logger import logger 
import sys 
from pyspark.sql import DataFrame 
from pyspark.ml.feature import StringIndexerModel 
from pyspark.ml.pipleine import PipelineModel 
from finance_complaint.config.spark_manager import spark_session 
from finance_complaint.utils import get_score 

from pyspark.sql.types import StringType, FloatType, StructType, StructField 
from finance_complaint.entity.estimator import S3FinanceEstimator
from finance_complaint.data_access.model_eval_artifact import ModelEvaluationArtifact

class ModelEvaluation:

    def __init__(self,
                 data_validation_artifact : DataValidationArtifact,
                 model_trainer_artifact : ModelTrainerArtifact,
                 model_eval_config: ModelEvaluationConfig,
                 schema = FinanceDataSchema()):

        try:
            self.model_eval_artifact_data = ModelEvaluationArtifactData()
            self.data_validation_artifact = data_validation_artifact
            self.model_eval_config = model_eval_config
            self.model_trainer_artifact = model_trainer_artifact 
            self.schema = schema 
            self.bucket_name = self.model_eval_config.bucket_name
            self.s3_model_dir_key = self.model_eval_config.model_dir
            self.s3_finance_estimator = S3FinanceEstimator(bucket_name = self.bucket_name,
                                                           s3_key = self.s3_model_dir_key)
            self.metric_report_schema = StructType([StructField("model_accepted", StringType()),
                                                    StructField("changed_accuracy", FloatType()),
                                                    StructField("trained_model_path", StringType()),
                                                    StructField("best_model_path", StringType()),
                                                    StructField("active", StringType())])
        
        except Exception as e:
            raise FinanceException(e, sys)