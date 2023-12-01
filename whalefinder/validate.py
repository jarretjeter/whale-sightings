from datetime import date
from dateutil.parser import parse
import json
import logging
from logging import INFO
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationError
import re
import sys
from typing import Optional, Tuple

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

root_dir = Path().cwd()
file = open(f"{root_dir}/config.json", 'r')
config = json.loads(file.read())
whales = config['whales']


class Results(BaseModel):
    """
    Pydantic model to validate Obis API responses against.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='ignore')

    occurrenceID: str = Field(default='')
    eventDate: date
    verbatimEventDate: str = Field(default='')
    decimalLatitude: float
    decimalLongitude: float
    waterBody: str = Field(default='')
    species: str
    speciesid: int
    vernacularName: str = Field(default='')
    individualCount: int = Field(default=1)
    basisOfRecord: str = Field(default='')
    bibliographicCitation: str = Field(default='')


    @field_validator('eventDate', mode='before')
    @classmethod
    def check_eventDate(cls, value) -> date:
        """
        accepted format examples: 
        '1913-03-17', '1849-12-04 23:12:00', '1849-12-04T23:12:00', 
        '1849-12-04T23:12:00Z', '1971-01-01 00:00:00+00', '1910-12-24T02:00'

        unaccepted format examples: 
        1758, '1785/1913', '1800-01-01/1874-06-24', '23 Aug 1951', 'Oct 1949', '1925-11', etc.
        """
        date_formats = [
            r'^\d{4}-\d{2}-\d{2}$', 
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}Z$',
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{2}$',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}\+\d{2}$',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}$',
            ]
        
        # If split dates are the same, except the timestamp (e.g. '2014-04-28T09:42:00/2014-04-28T10:50:00')
        if '/' in value and '-' in value:
            start_date, end_date = value.split('/')
            time_format = re.compile(r'T.*')
            start_date = re.sub(time_format, '', start_date)
            end_date = re.sub(time_format, '', end_date)
            if start_date == end_date:
                value = start_date
        for fmt in date_formats:
            if re.match(fmt, value):
                return parse(value).date()
        else:
            raise ValueError(f"eventDate '{value}' does not match any accepted format.")


class Validator():
    """
    Class for retrieving files and running Pydantic model
    """
    data_dir = './data'


    def __init__(self, whale: str, startdate: Optional[str]=None, enddate: Optional[str]=None) -> None:
        """
        
        """
        if whale in whales:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whales dictionary. {whales.keys()}')
        self.startdate = startdate
        self.enddate = enddate


    def match_files(self) -> list:
        """
        Get json files that match the class instance's start and end date attributes

        Returns:
            list[Path]
        """
        whale_dir = Path(f'{self.data_dir}/{self.whale}')
        reg_pattern = r'\d{4}-\d{2}-\d{2}\--\d{4}-\d{2}-\d{2}'
        files = []
        matched_files = []
        for file in whale_dir.glob('*.json'):
            if re.search(reg_pattern, file.name):
                files.append(file)

        if matched_files:
            if self.start and self.end:
                start_year = parse(self.start).year
                end_year = parse(self.end).year

                for file in files:
                    match = re.search(r'(\d{4})-\d{2}-\d{2}\--(\d{4})-\d{2}-\d{2}', file.name)
                    if match:
                        file_start_year = int(match.group(1))
                        file_end_year = int(match.group(2))

                        if start_year <= file_start_year <= end_year and start_year <= file_end_year <= end_year:
                            matched_files.append(file)

            elif self.start and not self.end:
                start_year = parse(self.start).year

                for file in files:
                    match = re.search(r'(\d{4})-\d{2}-\d{2}\--\d{4}-\d{2}-\d{2}', file.name)
                    if match:
                        file_start_year = int(match.group(1))

                        if start_year <= file_start_year:
                            matched_files.append(file)

            elif not self.start and self.end:
                end_year = parse(self.end).year

                for file in files:
                    match = re.search(r'\d{4}-\d{2}-\d{2}\--(\d{4})-\d{2}-\d{2}', file.name)
                    if match:
                        file_end_year = int(match.group(1))

                        if file_end_year <= end_year:
                            matched_files.append(file)

            return matched_files
        else:
            return files
    

    def get_data(self) -> dict:
        """
        Load data from multiple files into a single dictionary

        Returns:
            dict
        """
        files = self.match_files()
        data = {'results': []}

        for file in files:
            with open(file) as f:
                results = json.load(f)
                if 'results' in results.keys():
                    for d in results['results']:
                        data['results'].append(d)

        return data
    

    def validate_response(self) -> Tuple[dict, dict]:
        """
        Validate data from API response

        Returns:
            Tuple containing a dict of valid data and a dict of error data
        """
        valid_data = {'validated': []}
        error_data = {'errors': []}
        num_errors = 0
        data = self.get_data()

        for item in data['results']:
            try:
                occurrence = Results(**item)
                valid_data['validated'].append(occurrence.model_dump(mode='json'))
            except ValidationError as e:
                error_details = e.errors(include_context=False, include_input=False, include_url=False)
                # extract detail location from tuple
                for detail in error_details:
                    detail['loc'] = detail['loc'][0]
                # remove extra keys from item
                filtered_item = Results.model_construct(**item).model_dump(mode='json', warnings=False)
                error_data['errors'].append({'details': error_details, 'data': filtered_item})
                num_errors += len(error_details)

        logger.info(f"Validated: {len(valid_data['validated'])}, Errors: {num_errors}")

        return valid_data, error_data
