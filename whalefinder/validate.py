from datetime import date, datetime
import json
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from typing import List, Optional, Union

date_formats = ['1913-03-17', '1785/1913', '1849-12-04 23:12:00', '1849-12-04T23:12:00', 1758, '23 Aug 1951', 'Oct 1949']


class Result(BaseModel):
    
    model_config = ConfigDict(arbitrary_types_allowed=True)

    occurrenceID: str
    eventDate: str
    verbatimEventDate: str
    decimalLatitude: float
    decimalLongitude: float
    waterBody: str
    species: str
    speciesid: int
    individualCount: int = Field(default=1)


class Response(BaseModel):
    total: int
    results: List[Result]

data = {
    "total": 4157,
    "results": [
        {
            "basisOfRecord": "MachineObservation",
            "decimalLatitude": 71.412,
            "decimalLongitude": -152.006,
            "eventDate": "2010",
            "eventTime": "21:00:00Z",
            "footprintWKT": "POINT(-152.006 71.412)",
            "geodeticDatum": "EPSG:4326 WGS84",
            "occurrenceID": "914_2730",
            "scientificName": "Delphinapterus leucas",
            # "verbatimEventDate": "2010-10-01 13:00:00",
            "vernacularName": "Beluga",
            "waterBody": "Beaufort Sea",
            "species": "Delphinapterus leucas",
            "speciesid": 137115,
        },
        {
            "basisOfRecord": "HumanObservation",
            "decimalLatitude": 69.040001,
            "decimalLongitude": -137.603302,
            "eventDate": "1980",
            "eventTime": "22:05:00Z",
            "footprintWKT": "POINT(-137.603302 69.040001)",
            "geodeticDatum": "EPSG:4326 WGS84",
            "occurrenceID": "825_23151",
            "individualCount": "NA",
            "scientificName": "Delphinapterus leucas",
            "verbatimEventDate": "1980-07-21 14:05:00",
            "vernacularName": "Beluga",
            "waterBody": "Arctic,Beaufort Sea,Chukchi Sea,Bering Sea",
            "species": "Delphinapterus leucas",
            "speciesid": 137115,
        }
    ]
}


def validate(data) -> None:
    """
    Validate data from API response
    """
    validated = []
    errors = []

    for item in data['results']:
        try:
            occurrence = Result(**item)
            validated.append(occurrence)
        except ValidationError as e:
            errors.append((item, e.errors(include_input=False)))
    print(f"Validated: {len(validated)}, Errors: {len(errors)}")
    for e in errors:
        print(e[1])
    return validated, errors

if __name__ == '__main__':
    validate(data)