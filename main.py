from dataclasses import dataclass

import typer

from db import MySQLClient
from whalefinder import cleaner, obis, validate

pipeline = typer.Typer()

#TODO add context data for pipeline


@dataclass
class pipeline_context:
    #TODO if whale in whales
    whale: str
    startdate: str = ''
    enddate: str = ''
    size: int = 10000


@pipeline.command('pipeline')
def main(whale: str, startdate: str='', enddate: str='') -> None:
    """
    Full pipeline orchestration
    """
    context = pipeline_context(whale, startdate, enddate)

    api = obis.ApiClient()
    handler = obis.ObisHandler(api, context)
    handler.batch_requests()

    validator = validate.Validator(context)
    valid_data, error_data = validator.validate_response()
    data_cleaner = cleaner.WhaleDataCleaner(valid_data, error_data, context)
    df = data_cleaner.process_and_save()

    mysql_client = MySQLClient()
    mysql_client.to_mysql(df)

# Checkpoints

@pipeline.command('obis')
def fetch_api(whale: str, startdate: str='', enddate: str='', size: int=10000) -> None:
    """
    Only retrieve data from the Obis API
    """
    context = pipeline_context(whale, startdate, enddate, size)

    api = obis.ApiClient()
    handler = obis.ObisHandler(api, context)
    handler.batch_requests()

@pipeline.command('process')
def validate_and_process(whale: str, startdate: str='', enddate: str='') -> None:
    """
    Data validations and transformations
    """
    context = pipeline_context(whale, startdate, enddate)

    validator = validate.Validator(context)
    valid_data, error_data = validator.validate_response()
    data_cleaner = cleaner.WhaleDataCleaner(valid_data, error_data, context)
    data_cleaner.process_and_save()

@pipeline.command('db')
def load_to_db(data=None) -> None:
    """
    Load sighting data to database
    """
    mysql_client = MySQLClient()
    mysql_client.to_mysql(data)


if __name__ == '__main__':
    pipeline()
