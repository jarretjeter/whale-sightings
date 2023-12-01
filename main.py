from storage import *
import typer
from typing import Optional
from whalefinder import cleaner, obis, validate

pipeline = typer.Typer()


@pipeline.command('run')
def run(whale: str, startdate: Optional[str]=None, enddate: Optional[str]=None):
    """
    Pipeline orchestration
    """
    obis_api = obis.ObisAPI(whale, startdate, enddate)
    obis_api.api_requests()
    validator = validate.Validator(whale, startdate, enddate)
    valid_data, error_data = validator.validate_response()
    datacleaner = cleaner.WhaleDataCleaner(whale, valid_data, error_data, startdate, enddate)
    datacleaner.process_and_save()
    to_mysql(whale, datacleaner.filename)


if __name__ == '__main__':
    print('Running pipeline')
    pipeline()
    print('Pipeline finished')