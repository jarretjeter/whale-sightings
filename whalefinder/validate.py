from datetime import date, datetime
from dateutil.parser import parse
import logging
from logging import INFO
import json
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationError
import re
import sys
from typing import Any, List, Optional, Tuple, Union

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)


class Results(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='ignore')

    occurrenceID: str
    eventDate: date
    verbatimEventDate: str = Field(default=None)
    year: str = Field(default=None)
    date_year: int = Field(default=None)
    month: int = Field(default=None)
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
    def check_eventDate(cls, value) -> date:
        """
        Check if an eventDate field's value is of an acceptable format.

        Accepted format examples:\n
        ('1913-03-17', '1849-12-04 23:12:00', '1849-12-04T23:12:00', '1849-12-04T23:12:00Z', '1971-01-01 00:00:00+00')

        Unaccepted format examples:\n
        (1758, '1785/1913', '1800-01-01/1874-06-24', '23 Aug 1951', 'Oct 1949', '1925-11')
        """
        date_formats = [
            r'^\d{4}-\d{2}-\d{2}$', 
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{2}$',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
            ]
        match_found = False

        for fmt in date_formats:
            if re.match(fmt, value):
                match_found = True
                break
            
        if match_found:
            return parse(value).date()
        else:
            raise ValueError('eventDate does not match any accepted format.')


data = {
    "total": 4157,
    "results": [
        {
            "basisOfRecord": "MachineObservation",
            "decimalLatitude": 71.412,
            "decimalLongitude": -152.006,
            "eventDate": "1932-10-13",
            "eventTime": "21:00:00Z",
            "footprintWKT": "POINT(-152.006 71.412)",
            "geodeticDatum": "EPSG:4326 WGS84",
            "occurrenceID": "914_2730",
            # "individualCount": "NA",
            "scientificName": "Delphinapterus leucas",
            "verbatimEventDate": "2010-10-01 13:00:00",
            "vernacularName": "Beluga",
            "waterBody": "Beaufort Sea",
            "species": "Delphinapterus leucas",
            "speciesid": 137115,
        },
        {
            "basisOfRecord": "HumanObservation",
            "decimalLatitude": 69.040001,
            "decimalLongitude": -137.603302,
            "eventDate": "1758-07-02T12:00:00Z",
            "eventTime": "22:05:00Z",
            "footprintWKT": "POINT(-137.603302 69.040001)",
            "geodeticDatum": "EPSG:4326 WGS84",
            # "occurrenceID": "825_23151",
            "individualCount": "1",
            "scientificName": "Delphinapterus leucas",
            "verbatimEventDate": "1980-07-21 14:05:00",
            "vernacularName": "Beluga",
            "waterBody": "Arctic,Beaufort Sea,Chukchi Sea,Bering Sea",
            "species": "Delphinapterus leucas",
            "speciesid": 137115,
        }
    ]
}


def validate_response(whale: str, data: dict) -> Tuple["dict[str, list]", "dict[str, list]"]:
    """
    Validate data from API response

    Args:
        whale: str
            directory of whale to save to
        data: list[dict]
            data to validate against a Pydantic model
    Returns:
        Tuple[dict[str, list], dict[str, list]]
            Tuple values of accepted data and rejected data
    """
    validated = {'validated': []}
    errors = {'errors': []}

    data_dir = './data'
    
    for item in data['results']:
        try:
            occurrence = Results(**item)
            validated['validated'].append(occurrence.model_dump(mode='json'))
        except ValidationError as e:
            invalid = e.errors(include_context=False, include_input=False, include_url=False)
            invalid.append(item)
            errors['errors'].append(invalid)

    logger.info(f"Validated: {len(validated['validated'])}, Errors: {len(errors['errors'])}")

    if validated['validated']:
        accepted_dir = Path(f"{data_dir}/{whale}/valid")
        accepted_dir.mkdir(parents=True, exist_ok=True)
        with open(f"{accepted_dir}/valid_data.json", 'w') as file:
            json.dump(validated, file, indent=4, ensure_ascii=False)
            
    if errors['errors']:
        rejects_dir = Path(f"{data_dir}/{whale}/invalid")
        rejects_dir.mkdir(parents=True, exist_ok=True)
        with open(f"{rejects_dir}/invalid_data.json", "w") as file:
            json.dump(errors, file, indent=4, ensure_ascii=False)

    return validated, errors


if __name__ == '__main__':
    validate_response(whale='test_whale', data=data)