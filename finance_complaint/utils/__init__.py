import shutil

import yaml

from typing import List
from pyspark.sql import DataFrame
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
import sys,os
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def write_yaml_file(file_path: str, data: dict = None):
    """
    file_path: str
    data : dict

    """

    try:
        os.makedirs(os.path.dirname(file_path), exist_ok = True)
        with open(file_path, "w") as yaml_file:
            if data is not None:
                yaml.dump(data,yaml_file)
    except Exception as e:
        raise FinanceException(e,sys)

def read_yaml_file(file_path:str) -> dict:
    """

    Reads YAML file and return the dictionary
    """

    try:
        with open(file_path, 'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
            raise FinanceException(e,sys)
            
def get_score(dataframe: DataFrame, metric_name, label_col, prediction_col) -> float:
    try:
        evaluator = MulticlassClassificationEvaluator(
            labelCol=label_col, predictionCol=prediction_col,
            metricName=metric_name)
        score = evaluator.evaluate(dataframe)
        print(f"{metric_name} score: {score}")
        logger.info(f"{metric_name} score: {score}")
        return score
    except Exception as e:
        raise FinanceException(e, sys)