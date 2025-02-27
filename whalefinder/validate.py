from datetime import date
from dateutil.parser import parse, ParserError
import json
from pathlib import Path
import re
import sys
from typing import Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationError

from logging_setup import setup_logging
from whales import whale_names


logger = setup_logging()


class Results(BaseModel):
    """
    Pydantic model to validate Obis API responses against.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='ignore')

    occurrenceID: str = Field(default=None)
    eventDate: date
    verbatimEventDate: str = Field(default=None)
    decimalLatitude: float
    decimalLongitude: float
    waterBody: str = Field(default=None)
    species: str
    speciesid: int
    vernacularName: str = Field(default=None)
    individualCount: int = Field(default=1)
    basisOfRecord: str = Field(default=None)
    bibliographicCitation: str = Field(default=None)


    @field_validator('eventDate', mode='before')
    @classmethod
    def check_eventdate(cls, value) -> date:
        """
        accepted format examples: 
        '1913-03-17', '1849-12-04 23:12:00', '1849-12-04T23:12:00', 
        '1849-12-04T23:12:00Z', '1971-01-01 00:00:00+00', '1910-12-24T02:00'

        unaccepted format examples:
        (formats may be parsable, but values end up being removed or added unintentionally)
        '1800-01-01/1874-06-24', '1925-11', June 1758, etc.
        """
        bad_formats = [
            r'^\d{4}-\d{1,2}$', # 1990-03
            r'^\d{1,2}-\d{4}$', # 03-1990
            r'^\d{1,4}$', # 1985
            r'^\d{1,2} [A-Za-z]+$', # 20 Nov
            r'^[A-Za-z]+ \d{1,2}$', # Oct 15
            r'^[A-Za-z]+ \d{4}$', # Oct 1970
            r'^\d{4} [A-Za-z]+$', # 1970 Oct
            r'^.*/.*$' # string with any '/' character
        ]
        # Matching bad values should be handled further down the script
        for fmt in bad_formats:
            if re.match(fmt, value):
                raise ValueError(f"eventDate '{value}' is a bad format.")
        return parse(value).date()


class Validator:
    """
    Class for retrieving files and running Pydantic model validations
    """
    data_dir = './data'

    def __init__(self, context) -> None:
        """
        whale: str
            Name of file directory to search
        startdate, enddate: str
            Date range of files to match
        """
        if context.whale and context.whale in whale_names:
            self.whale = context.whale
        else:
            raise ValueError(f'{context.whale} not in whale_names dictionary. {whale_names.keys()}')
        self.startdate = context.startdate
        self.enddate = context.enddate

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

        if files:
            if self.startdate and self.enddate:
                start_year = parse(self.startdate).year
                end_year = parse(self.enddate).year

                for file in files:  # start and end between a specific year
                    match = re.search(r'(\d{4})-\d{2}-\d{2}\--(\d{4})-\d{2}-\d{2}', file.name)
                    if match:
                        file_start_year = int(match.group(1))
                        file_end_year = int(match.group(2))

                        if start_year <= file_start_year <= end_year and start_year <= file_end_year <= end_year:
                            matched_files.append(file)

            elif self.startdate and not self.enddate:  # start from a certain file year to the last file found
                start_year = parse(self.startdate).year

                for file in files:
                    match = re.search(r'(\d{4})-\d{2}-\d{2}\--\d{4}-\d{2}-\d{2}', file.name)
                    if match:
                        file_start_year = int(match.group(1))

                        if start_year <= file_start_year:
                            matched_files.append(file)

            elif not self.startdate and self.enddate:  # starting from the 1st found file and up to a certain point
                end_year = parse(self.enddate).year

                for file in files:
                    match = re.search(r'\d{4}-\d{2}-\d{2}\--(\d{4})-\d{2}-\d{2}', file.name)
                    if match:
                        file_end_year = int(match.group(1))

                        if file_end_year <= end_year:
                            matched_files.append(file)

            else:  # return all files
                return files
            
            return matched_files
        
        else:
            logger.warning("No json files were found to validate, try fetching from the Obis API first")
            sys.exit(1)
    
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
