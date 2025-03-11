from dataclasses import dataclass

import typer

from db import MySQLClient
from whales import whale_names
from whalefinder import cleaner, obis, validate


pipeline = typer.Typer()

@dataclass
class PipelineContext:
    whale: str
    startdate: str = ''
    enddate: str = ''
    size: int = 10000
    data_dir: str = './data'
    
    def __post_init__(self):
        if self.whale in whale_names:
            pass
        else:
            raise ValueError(
                f"Name '{self.whale}' not in whale_names: {[k for k in whale_names.keys()]}"
                )
        
    def get_scientific_name(self) -> str:
        self.scientificname = whale_names[self.whale]['scientificname']
        return self.scientificname


@pipeline.command('pipeline')
def main(whale: str, startdate: str='', enddate: str='') -> None:
    """
    Full pipeline orchestration
    """
    context = PipelineContext(whale, startdate, enddate)

    api = obis.ApiClient()
    handler = obis.ObisHandler(api, context)
    handler.batch_requests()

    validator = validate.Validator(context)
    valid_data, error_data = validator.validate_response()
    data_cleaner = cleaner.WhaleDataCleaner(valid_data, error_data, context)
    df = data_cleaner.process_and_save()

    mysql_client = MySQLClient()
    mysql_client.to_mysql(df)


# Pipeline Checkpoints

@pipeline.command('obis')
def fetch_api(whale: str, startdate: str='', enddate: str='', size: int=10000) -> None:
    """
    Only retrieve data from the Obis API
    """
    context = PipelineContext(whale, startdate, enddate, size)

    api = obis.ApiClient()
    handler = obis.ObisHandler(api, context)
    handler.batch_requests()

@pipeline.command('process')
def validate_and_process(whale: str, startdate: str='', enddate: str='') -> None:
    """
    Used for just data validations and transformations
    """
    context = PipelineContext(whale, startdate, enddate)

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
