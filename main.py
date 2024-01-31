from storage import MySQLClient
import typer
from whalefinder import cleaner, obis, validate

pipeline = typer.Typer()


@pipeline.command('main')
def main(whale: str, startdate: str='', enddate: str=''):
    """
    Pipeline orchestration
    """
    api = obis.ApiClient()
    handler = obis.ObisHandler(api, whale, startdate, enddate)
    handler.batch_requests()
    validator = validate.Validator(whale, startdate, enddate)
    valid_data, error_data = validator.validate_response()
    datacleaner = cleaner.WhaleDataCleaner(whale, valid_data, error_data, startdate, enddate)
    df = datacleaner.process_and_save()

    mysqlclient = MySQLClient()
    with mysqlclient as client:
        client.to_mysql(df)


if __name__ == '__main__':
    print('Running pipeline')
    pipeline()
    print('Pipeline finished')