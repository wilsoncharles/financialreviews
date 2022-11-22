import os
import argparse
from finance_complaint.exception import FinanceException
from finance_complaint.pipeline import TrainingPipeline
from finance_complaint.logger import logger
from finance_complaint.config.pipeline.training import FinanceConfig
import sys


def start_training(start=False):
    try:
        if not start:
            return None
        print("Training Running")
        TrainingPipeline(FinanceConfig()).start()
        
    except Exception as e:
        raise FinanceException(e, sys)





def main(training_status):
    try:
        start_training(start=training_status)
       
    except Exception as e:
        raise FinanceException(e, sys)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--t", default=0, type=int, help="If provided true training will be done else not")
        parser.add_argument("--p", default=0, type=int, help="If provided prediction will be done else not")

        args = parser.parse_args()

        main(training_status=args.t)
    except Exception as e:
        print(e)
        pass
        logger.exception(FinanceException(e, sys))