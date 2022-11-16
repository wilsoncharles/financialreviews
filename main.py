from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
import os, sys

if __name__ == '__main__':
    try:
        r = 1/0
    except Exception as e:
        print('getting into exception')
        logging.info("test logs")
        raise FinanceException(e,sys)

