import os

from finance_complaint.entity.schema import FinanceDataSchema
import sys
from pyspark.ml.feature import StandardScaler, VectorAssembler, OneHotEncoder, StringIndexer, Imputer
from pyspark.ml.pipeline import Pipeline

from finance_complaint.config.spark_manager import spark_session
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.entity.artifact_entity import DataValidationArtifact, DataTransformationArtifact
from finance_complaint.entity.config_entity import DataTransformationConfig
from pyspark.sql import DataFrame
from finance_complaint.ml.feature import FrequencyImputer, DerivedFeatureGenerator
from pyspark.ml.feature import IDF, Tokenizer, HashingTF
from pyspark.sql.functions import col, rand


class DataTransformation():

    def __init__(self, data_validation_artifact: DataValidationArtifact,
                 data_transformation_config: DataTransformationConfig,
                 schema=FinanceDataSchema()
                 ):
        try:
            super().__init__()
            self.data_val_artifact = data_validation_artifact
            self.data_tf_config = data_transformation_config
            self.schema = schema
        except Exception as e:
            raise FinanceException(e, sys)

    def read_data(self) -> DataFrame:
        try:
            file_path = self.data_val_artifact.accepted_file_path
            dataframe: DataFrame = spark_session.read.parquet(file_path)
            dataframe.printSchema()
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def get_data_transformation_pipeline(self, ) -> Pipeline:
        try:

            stages = [

            ]

            # numerical column transformation

            # generating additional columns
            derived_feature = DerivedFeatureGenerator(inputCols=self.schema.derived_input_features,
                                                      outputCols=self.schema.derived_output_features)
            stages.append(derived_feature)
            # creating imputer to fill null values
            imputer = Imputer(inputCols=self.schema.numerical_columns,
                              outputCols=self.schema.im_numerical_columns)
            stages.append(imputer)

            frequency_imputer = FrequencyImputer(inputCols=self.schema.one_hot_encoding_features,
                                                 outputCols=self.schema.im_one_hot_encoding_features)
            stages.append(frequency_imputer)
            for im_one_hot_feature, string_indexer_col in zip(self.schema.im_one_hot_encoding_features,
                                                              self.schema.string_indexer_one_hot_features):
                string_indexer = StringIndexer(inputCol=im_one_hot_feature, outputCol=string_indexer_col)
                stages.append(string_indexer)

            one_hot_encoder = OneHotEncoder(inputCols=self.schema.string_indexer_one_hot_features,
                                            outputCols=self.schema.tf_one_hot_encoding_features)

            stages.append(one_hot_encoder)

            tokenizer = Tokenizer(inputCol=self.schema.tfidf_features[0], outputCol="words")
            stages.append(tokenizer)

            hashing_tf = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawFeatures", numFeatures=40)
            stages.append(hashing_tf)
            idf = IDF(inputCol=hashing_tf.getOutputCol(), outputCol=self.schema.tf_tfidf_features[0])
            stages.append(idf)

            vector_assembler = VectorAssembler(inputCols=self.schema.input_features,
                                               outputCol=self.schema.vector_assembler_output)

            stages.append(vector_assembler)

            standard_scaler = StandardScaler(inputCol=self.schema.vector_assembler_output,
                                             outputCol=self.schema.scaled_vector_input_features)
            stages.append(standard_scaler)
            pipeline = Pipeline(
                stages=stages
            )
            logger.info(f"Data transformation pipeline: [{pipeline}]")
            print(pipeline.stages)
            return pipeline

        except Exception as e:
            raise FinanceException(e, sys)