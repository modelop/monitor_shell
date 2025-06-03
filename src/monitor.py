import json
import pandas as pd
import logging
from pathlib import Path

global LOG

# modelop.init
def init(init_param: dict):
    """
    This method gets called when the monitor is loaded by the ModelOp runtime.
    It sets the GLOBAL values that are extracted from the report.txt to obtain the DTS and version info to append to the report.

    Args:
        init_param (dict): A dictionary containing initialization parameters, including 'rawJson'.
    """
    global LOG

    LOG = logging.getLogger(__name__)
    LOG.setLevel(logging.DEBUG)

# modelop.metrics
def metrics(data: pd.DataFrame):
    """
    This method is the modelops metrics method.
    This is always called with a pandas dataframe that is arraylike and contains individual rows represented in a dataframe format
    that is representative of all of the data that comes in as the results of the first input asset on the job.
    This method will not be invoked until all data has been read from that input asset.

    Args:
        data (pd.DataFrame): A pandas DataFrame containing the input data for the metrics calculation.
    """
    LOG.info("Running the metrics function")

    finalResult = {}

    finalResult.update({
        'count': len(data)
    })

    yield finalResult

def main():
    """
    This main method is utilized to simulate what the engine will do when calling the above metrics function.
    It takes the json formatted data and converts it to a pandas DataFrame, then passes this into the metrics function for processing.
    This is a good way to develop your models to be conformant with the engine in that you can run this locally first and ensure the Python is behaving correctly before deploying on a ModelOp engine.
    """
    raw_json = Path('job_info/example_job.json').read_text()
    init_param = {'rawJson': raw_json}

    init(init_param)
    df = pd.read_csv('data/german_credit_data.csv')
    print(json.dumps(next(metrics(df)), indent=2))


if __name__ == '__main__':
    main()