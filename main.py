import typer

from db import MySQLClient
from whalefinder import cleaner, obis, validate

pipeline = typer.Typer()

#TODO add context data for pipeline


@pipeline.command('main')
def main(whale: str, startdate: str='', enddate: str='') -> None:
    """
    Full pipeline orchestration
    """
    api = obis.ApiClient()
    handler = obis.ObisHandler(api, whale, startdate, enddate)
    handler.batch_requests()

    validator = validate.Validator(whale, startdate, enddate)
    valid_data, error_data = validator.validate_response()
    data_cleaner = cleaner.WhaleDataCleaner(whale, valid_data, error_data, startdate, enddate)
    df = data_cleaner.process_and_save()

    mysql_client = MySQLClient()
    mysql_client.to_mysql(df)

# Checkpoints

@pipeline.command('obis')
def fetch_api(whale: str, startdate: str='', enddate: str='') -> None:
    """
    Only retrieve data from the Obis API
    """
    api = obis.ApiClient()
    handler = obis.ObisHandler(api, whale, startdate, enddate)
    handler.batch_requests()

@pipeline.command('process')
def validate_and_process(whale: str, startdate: str='', enddate: str='') -> None:
    """
    Data validations and transformations
    """
    validator = validate.Validator(whale, startdate, enddate)
    valid_data, error_data = validator.validate_response()
    data_cleaner = cleaner.WhaleDataCleaner(whale, valid_data, error_data, startdate, enddate)
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
