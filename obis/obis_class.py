from dateutil.parser import parse
import datetime
import json
import logging
from logging import INFO
from pathlib import Path
import requests
import sys
from typing import Optional

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)



class Obis():
    """Object for working with specific OBIS api endpoints (https://api.obis.org/v3)
    
    (https://obis.org/)
    """
    whales = {'blue_whale': {'scientific_name': 'Balaenoptera musculus'}, 'sperm_whale': {'scientific_name': 'Physeter macrocephalus'}}
    data_dir = './data'

    def __init__(self, whale: str, start_date: Optional[str]=None, end_date: Optional[str]=None, size: Optional[int]=10000) -> None:
        """
        Args:
            whale: str
                Whale for the api to query
            start_date, end_date: str, default None
                start and end dates to query through, in the format of `YYYY-MM-DD`.
                Both parameters must be set to None, or a date format. 
                If None, all dates on record will be queried
            size: int, default 10,000
                Maximum number of allowed results returned in json response
                The API does not accept a size limit greater than 10,000
        """
        if whale in self.whales:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whales dictionary. {self.whales.keys()}')
        self.start = start_date
        self.end = end_date
        self.size = size
        

    def get_records(self) -> tuple:
        """Retrieve total number of records from a request to the /statistics endpoint
        """
        whale = self.whale
        start = self.start
        end = self.end
        num_records = 0
        try:
            scientific_name = self.whales[whale]['scientific_name']
            # request earliest and latest records to get a start and end date
            url = f'https://api.obis.org/v3/statistics/years?scientificname={scientific_name}&startdate={start}&enddate={end}'
            r = requests.get(url)
            print(f'statistics/years status code: {r.status_code}')
            records = r.json()
            for record in records:
                num_records = num_records + record['records']
            return records, num_records
        except requests.exceptions.RequestException as e:
            logger.info(e)


# ERROR HANDLE STATUS CODES
    def get_occurrences(self, start_date: str, end_date: str) -> None:
        """Send a get request to the OBIS api's /occurrence endpoint
        """
        whale = self.whale
        size = self.size
        try:
            scientific_name = self.whales[whale]['scientific_name']
            url = f'https://api.obis.org/v3/occurrence?scientificname={scientific_name}&startdate={start_date}&enddate={end_date}&size={size}'
            url = url.replace(' ', '%20')
            r = requests.get(url)
            print(r.status_code)
            print(start_date, end_date)
            self.response = r
            self.save_json(start_date, end_date)
        except requests.exceptions.RequestException as e:
            logger.info(e)


    def save_json(self, start_date: str, end_date: str) -> None:
        """Save a `requests.Response` to a json file
        """
        whale = self.whale
        data_dir = self.data_dir
        response = self.response
        output_dir = Path(f'{data_dir}/{whale}')
        output_dir.mkdir(parents=True, exist_ok=True)

        with open(f'{output_dir}/{start_date}--{end_date}.json', 'w') as file:
            logger.info(f'Saving json response to {file.name}')
            json.dump(response.json(), file, ensure_ascii=False, indent=4)


    def obis_requests(self) -> None:
        """Send multiple requests to OBIS api depending on total records
        of response"""
        start = self.start
        end = self.end
        max_size = self.size
        start_year = parse(start).year
        end_year = parse(end).year
        records, num_records = self.get_records()
        print(f'max: {max_size}')
        print(f'num_records: {num_records}')

        if max_size <= num_records:
            current_size = 0
            for rec in records:
                current_size += rec['records']
                if current_size <= max_size:
                    print(f"current_size: {current_size}, {rec['year']}")
                    end = str(rec['year'])
                    end = end + '-12-31'
                    continue
                else:
                    print(f"end: {end}, current size {current_size}/{max_size}, {rec['year']}")
                    self.get_occurrences(start, end)
                    current_size = rec['records']
                    print(f"Resetting, current_size: {current_size}, {rec['year']}")
                    end_year = parse(end).year
                    start = str(end_year + 1)
                    start = start + '-01-01'
                    print(f'start: {start}')
            self.get_occurrences(start, end)
        else:
            self.get_occurrences(start, end)
