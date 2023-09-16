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
    whales = {'blue_whale': {'scientificname': 'Balaenoptera musculus'}, 'sperm_whale': {'scientificname': 'Physeter macrocephalus'}}
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
        self.records, self.num_records = self.get_records()
        

    def get_records(self) -> tuple:
        """Retrieve total number of records from a request to the /statistics/years endpoint
        """
        whale = self.whale
        num_records = 0
        url = f'https://api.obis.org/v3'
        scientificname = self.whales[whale]['scientificname']
        params = {'scientificname': scientificname}
        if self.start:
            params['startdate'] = self.start
        if self.end:
            params['enddate'] = self.end
        try:
            # if a start or end date were not supplied, default values(earliest and latest records) will be retrieved from the endpoint response
            r = requests.get(f'{url}/statistics/years', params=params)
            logger.info(r.url)
            logger.info(f'/statistics/years status code: {r.status_code}')
            records = r.json()
            if not self.start:
                self.start = str(records[0]['year']) + '-01-01'
            if not self.end:
                self.end = str(records[-1]['year']) + '-12-31'
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
        url = f'https://api.obis.org/v3'
        scientificname = self.whales[whale]['scientificname']
        params = {'scientificname': scientificname, 'startdate': start_date, 'enddate': end_date, 'size': size}
        try:
            scientificname = self.whales[whale]['scientificname']
            r = requests.get(f"{url}/occurrence", params=params)
            logger.info(f"/occurrence status code: {r.status_code}")
            logger.info(r.url)
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
        records = self.records
        num_records = self.num_records
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
