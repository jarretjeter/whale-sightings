import json
from pathlib import Path
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

from logging_setup import setup_logging
from whales import whale_names


logger = setup_logging()


class ApiClient:
    """
    Client for the Obis API
    """
    base_url = "https://api.obis.org/v3"
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    def __init__(self) -> None:
        pass

    def send_request(self, endpoint: str, params: dict) -> requests.Response:
        """
        Send a get request to the api
        
        Args:
            endpoint: str
                API endpoint to request
            params: dict
                parameters to send with request
        Returns:
            requests.Response
        """
        try:
            response = self.session.get(f"{self.base_url}/{endpoint}", params=params)
            time.sleep(1.0)
            return response
        except requests.RequestException:
            sys.exit("Failed to connect to the API")
        

class ObisHandler:
    """Object for handling whale data from specific OBIS api endpoints
    """
    data_dir = './data'

    def __init__(self, api: ApiClient, whale: str, startdate: str='', enddate: str='', size: int=10000) -> None:
        """
        Args:
            api: ApiClient
                Object to make calls to the OBIS API
            whale: str
                Whale for the api to query
            startdate, enddate: str
                start and end dates to query through, in `YYYY-MM-DD` format.
                If values are empty, the earliest/latest records are retrieved.
            size: int, default 10,000
                Maximum number of allowed results returned in json response
                The API does not accept a size limit greater than 10,000
        """
        self.api = api
        if whale in whale_names:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whale_names dictionary. {whale_names.keys()}')
        self.startdate = startdate
        self.enddate = enddate
        self.size = size

    def get_records(self) -> Tuple[List[Dict], int]:
        """Retrieve total number of records from a request to the /statistics/years endpoint

        Returns:
            tuple[list[dict], int]
        """
        endpoint = '/statistics/years'
        scientificname = whale_names[self.whale]['scientificname']
        params = {'scientificname': scientificname, 'startdate': self.startdate, 'enddate': self.enddate}

        logger.info(f"Getting records for {self.whale}")
        records = self.api.send_request(endpoint, params)
        records = records.json()

        for record in records: record['year'] = str(record['year'])
        num_records = sum(record['records'] for record in records)

        # if start or enddate are empty, default values(earliest and latest records) will be retrieved from response
        if not self.startdate:
            self.startdate = records[0]['year']
        if not self.enddate:
            self.enddate = records[-1]['year']

        logger.info(f'Total Records: {num_records}')
        return records, num_records

    def make_dateformat(self, date_strings: tuple) -> Tuple[str, str]:
        """Converts to date format (YYYY-MM-DD) if not already
        
        Args:
            date_strings: tuple[str, str]
                tuple of string values
        Returns:
            tuple of converted strings
        """
        start = date_strings[0]
        end = date_strings[1]
        if re.match(r"\d{4}-\d{2}-\d{2}", start):
            pass
        else:
            start = start + '-01-01'
        if re.match(r"\d{4}-\d{2}-\d{2}", end):
            pass
        else:
            end = end + '-12-31'
        return start, end

    def get_occurrences(self, startdate: str, enddate: str) -> None:
        """Send a get request to the OBIS api's /occurrence endpoint

        Args:
            startdate, enddate: str
                start and end dates to query through
        Returns:
            None
        """
        endpoint = '/occurrence'
        scientificname = whale_names[self.whale]['scientificname']
        startdate, enddate = self.make_dateformat((startdate, enddate))
        params = {'scientificname': scientificname, 'startdate': startdate, 'enddate': enddate, 'size': self.size}
        
        logger.info(f"Sending /occurrence request for date period: {startdate}-{enddate}")
        response = self.api.send_request(endpoint, params)
        self.save_json(response, startdate, enddate)

    def handle_large_record(self, year: str, start: str, previous_record_year: str):
        """Send a request for a large record"""
        # request the previously iterated years
        if start and previous_record_year:
            self.get_occurrences(start, previous_record_year)
        # request the large record
        self.get_occurrences(year, year)

    def save_json(self, response: requests.Response, startdate: str, enddate: str) -> None:
        """Save a `requests.Response` to a json file
        
        Args:
            response: requests.Response
            startdate, enddate: str
                used for file naming
        Returns:
            None
        """
        output_dir = Path(f'{self.data_dir}/{self.whale}')
        output_dir.mkdir(parents=True, exist_ok=True)

        # TODO is saving json files still necessary?
        with open(f'{output_dir}/{startdate}--{enddate}.json', 'w') as file:
            logger.info(f'Saving json response to {file.name}')
            json.dump(response.json(), file, ensure_ascii=False, indent=4)

    def batch_requests(self) -> None:
        """Send requests in batches to OBIS api if total records exceed the size limit"""
        records, num_records = self.get_records()

        # make a single request if size is not exceeded
        if self.size >= num_records:
            self.get_occurrences(self.startdate, self.enddate)
            return
        
        start = self.startdate
        previous_record_year = ''
        current_size = 0

        for i, record in enumerate(records):
            year, year_records = record['year'], record['records']
            # update the start only if value was set to empty
            start = year if not start else start

            # if a single year's records exceed the size limit, save records to their own separate file
            if year_records > self.size:
                self.handle_large_record(year, start, previous_record_year)
                # new values to be set on next iteration
                current_size = 0
                start = ''
                previous_record_year = ''
                continue

            if current_size + year_records > self.size:
                self.get_occurrences(start, previous_record_year)
                current_size = 0
                start = year

            current_size += year_records
            previous_record_year = year

            # if last record is reached
            if i == len(records) - 1:
                self.get_occurrences(start, self.enddate)
