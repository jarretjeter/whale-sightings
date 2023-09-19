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



class ObisAPI():
    """Object for obtaining whale data from specific OBIS api endpoints (https://api.obis.org/v3)
    
    (https://obis.org/)
    """
    whales = {'blue_whale': {'scientificname': 'Balaenoptera musculus'}, 'sperm_whale': {'scientificname': 'Physeter macrocephalus'}}
    data_dir = './data'

    def __init__(self, whale: str, startdate: Optional[str]=None, enddate: Optional[str]=None, size: Optional[int]=10000) -> None:
        """
        Args:
            whale: str
                Whale for the api to query
            startdate, enddate: str, default None
                start and end dates to query through, in the format of `YYYY-MM-DD`.
                If either are None, the earliest or latest records are retrieved.
            size: int, default 10,000
                Maximum number of allowed results returned in json response
                The API does not accept a size limit greater than 10,000
        """
        if whale in self.whales:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whales dictionary. {self.whales.keys()}')
        self.startdate = startdate
        self.enddate = enddate
        self.size = size
        self.records, self.num_records = self.get_records()
        

    def get_records(self) -> tuple:
        """Retrieve total number of records from a request to the /statistics/years endpoint

        Returns:
            tuple[list[dict], int]
        """
        whale = self.whale
        num_records = 0
        url = f'https://api.obis.org/v3'
        scientificname = self.whales[whale]['scientificname']
        params = {'scientificname': scientificname}
        if self.startdate:
            params['startdate'] = self.startdate
        if self.enddate:
            params['enddate'] = self.enddate
        try:
            # if a start or end date were not supplied, default values(earliest and latest records) will be retrieved from the endpoint response
            r = requests.get(f'{url}/statistics/years', params=params)
            logger.info(r.url)
            logger.info(f'/statistics/years status code: {r.status_code}')
            records = r.json()
            if not self.startdate:
                self.startdate = str(records[0]['year']) + '-01-01'
            if not self.enddate:
                self.enddate = str(records[-1]['year']) + '-12-31'
            for rec in records:
                num_records = num_records + rec['records']
            return records, num_records
        except requests.exceptions.RequestException as e:
            logger.info(e)


# ERROR HANDLE STATUS CODES
    def get_occurrences(self, startdate: str, enddate: str) -> None:
        """Send a get request to the OBIS api's /occurrence endpoint
        """
        whale = self.whale
        size = self.size
        url = f'https://api.obis.org/v3'
        scientificname = self.whales[whale]['scientificname']
        params = {'scientificname': scientificname, 'startdate': startdate, 'enddate': enddate, 'size': size}
        try:
            scientificname = self.whales[whale]['scientificname']
            r = requests.get(f"{url}/occurrence", params=params)
            logger.info(r.url)
            logger.info(f"/occurrence status code: {r.status_code}")
            self.response = r
            self.save_json(startdate, enddate)
        except requests.exceptions.RequestException as e:
            logger.info(e)


    def save_json(self, startdate: str, enddate: str) -> None:
        """Save a `requests.Response` to a json file
        """
        whale = self.whale
        data_dir = self.data_dir
        response = self.response
        output_dir = Path(f'{data_dir}/{whale}')
        output_dir.mkdir(parents=True, exist_ok=True)

        with open(f'{output_dir}/{startdate}--{enddate}.json', 'w') as file:
            logger.info(f'Saving json response to {file.name}')
            json.dump(response.json(), file, ensure_ascii=False, indent=4)


    def api_requests(self) -> None:
        """Send multiple requests to OBIS api depending on total records
        of response and size limit"""
        startdate = self.startdate
        enddate = self.enddate
        max_size = self.size
        startyear = parse(startdate).year
        endyear = parse(enddate).year
        records = self.records
        num_records = self.num_records
        print(f'max: {max_size}')
        print(f'num_records: {num_records}')

        # logic currently depends on if the size of the first record is less than the max size

        # if the total number of records exceeds the size attribute, a series of requests are made
        if max_size <= num_records:
            current_size = 0
            for rec in records:
                current_size += rec['records']
                # set enddate to current record if the statement is True
                if current_size <= max_size:
                    print(f"current_size: {current_size}/{max_size}, {rec['year']}")
                    enddate = str(rec['year'])
                    enddate = enddate + '-12-31'
                else:
                    print(f"end: {enddate}, current size {current_size}/{max_size}, {rec['year']}")
                    self.get_occurrences(startdate, enddate)
                    current_size = rec['records']
                    print(f"Resetting, current_size: {current_size}, {rec['year']}")
                    endyear = parse(enddate).year
                    startdate = str(endyear + 1)
                    startdate = startdate + '-01-01'
                    print(f'start: {startdate}')
            # make final request when last record is reached
            self.get_occurrences(startdate, enddate)
        # make a single request if size is not exceeded
        else:
            self.get_occurrences(startdate, enddate)
