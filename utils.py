import json
import logging
import time
import pandas as pd


def get_logger(path):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler(path)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('[%(asctime)s:%(filename)s#L%(lineno)d:%(levelname)s]: %(message)s'))

    logger.addHandler(handler)
    return logger


def load_sampling_data(sampling_log):
    with open(sampling_log, 'r') as f:
        lines = f.readlines()

    records = []
    for line in lines:
        records.append(json.loads(line))

    data = pd.DataFrame(records)

    return data
