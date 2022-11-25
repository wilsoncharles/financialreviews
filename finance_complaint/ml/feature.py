from pyspark import keyword_only
from pyspark.ml import Transformer 
from pyspark.ml.param.shared import Param, Params, TypeConverters, HasOutputCols, HasInputCols 
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable 
from pyspark.ml import Estimator
from pyspark.sql import DataFrame 
from pyspark.sql.functions import desc 
from pyspark.sql.functions import col, abs
from typing import List 
from pyspark.sql.types import TimestampType, LongType 
from finance_complaint.logger import logger 
from finance_complaint.config.spark_manager import spark_session 


class FrequencyEncoder(Estimator, HasInputCols, HasOutputCols,
                        DefaultParamsReadable, DefaultParamsWritable):
    
    frequencyInfo = Param(Params._dummy(), "getfrequencyInfo", "getfrequencyInfo",
                          typeConverter = TypeConverters.toList)
    
    @keyword_only
    def __init__(self, inputCols:List[str] = None , outputcols: List[str] = None):
        super(FrequencyEncoder, self).__init__()
        kwargs = self._input_kwargs

        self.frequencyInfo = Param(self,"frequencyInfo","")
        self._setDefault(frequencyInfo="")

        self.setParams(**kwargs)

    def setfrequencyInfo(self, frequencyInfo: list):
        return self._set(frequencyInfo =frequencyInfo)

    def getfrequencyInfo(self):
        return self.getOrDefault(self.frequencyInfo)

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputcols:List[str] = None)  :
        kwargs = self._input_kwargs
        return self._set(**kwargs)   
    
    def setInputCols(self, value: List[str]):
        return self._set(inputCol = value)

    def setOutputCols(self,value:List[str]):
        return self._set(outputCols = value)

    def _fit(self, dataframe: DataFrame):
        input_columns = self.getInputCols()
        print(f"Input Columns: {input_columns}")
        output_columns = self.getOutputCols()
        print(f"Output Columns : {output_columns}")
        replace_info = []

        for column, new_column in zip(input_columns, output_columns):
            freq = (dataframe.
                    select(col(column).alias(f'g_{column}')).
                    groupby(f'g_{column}').
                    count().
                    withColumn(new_column , col('count')))
            freq = freq.drop('count')
            logger.info(f"{column} has [{freq.count()}] unique category")
            replace_info.append(freq.collect())
        
        self.setfrequencyInfo(frequencyInfo= replace_info)
        estimator = FrequencyEncoderModel(inputCols = input_columns, outputCols= output_columns)
        estimator.setfrequencyInfo(frequencyInfo = replace_info)
        return estimator
