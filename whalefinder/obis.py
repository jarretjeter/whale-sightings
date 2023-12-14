import json
import logging
from logging import INFO
from pathlib import Path
import re
import requests
from requests.adapters import HTTPAdapter, Retry
import sys
import time
from typing import Dict, List, Optional, Tuple

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

ROOT_DIR = Path().cwd()
with open(f"{ROOT_DIR}/config.json", 'r') as file:
    config = json.load(file)
# Whales Dictionary
whales = config['whales']

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))


class ApiClient():
    """
    Client for the Obis API
    """
    base_url = "https://api.obis.org/v3"

    def __init__(self) -> None:
        pass

    def request_api(self, endpoint: str, params: dict) -> requests.Response:
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
            response = session.get(f"{self.base_url}/{endpoint}", params=params)
            response.raise_for_status()
            return response
        except requests.RequestException:
            sys.exit("Failed to connect to the API")
        

class ObisHandler():
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
        if whale in whales:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whales dictionary. {whales.keys()}')
        self.startdate = startdate
        self.enddate = enddate
        self.size = size
        

    def get_records(self) -> Tuple[List[Dict], int]:
        """Retrieve total number of records from a request to the /statistics/years endpoint

        Returns:
            tuple[list[dict], int]
        """
        endpoint = '/statistics/years'
        scientificname = whales[self.whale]['scientificname']
        params = {'scientificname': scientificname, 'startdate': self.startdate, 'enddate': self.enddate}

        records = self.api.request_api(endpoint, params)
        records = records.json()

        # if start or end date are empty, default values(earliest and latest records) will be retrieved from response
        if not self.startdate:
            self.startdate = str(records[0]['year'])
        if not self.enddate:
            self.enddate = str(records[-1]['year'])

        num_records = len(records)
        return records, num_records
        

    def is_dateformat(self, date_strings: tuple) -> Tuple[str, str]:
        """Checks for date formats (YYYY-MM-DD)
        
        Args:
            date_strings: tuple[str, str]
                tuple of string values
        Returns:
            tuple of converted strings
        """
        start = str(date_strings[0])
        end = str(date_strings[1])
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
        scientificname = whales[self.whale]['scientificname']
        startdate, enddate = self.is_dateformat((startdate, enddate))
        params = {'scientificname': scientificname, 'startdate': startdate, 'enddate': enddate, 'size': self.size}
        
        response = self.api.request_api(endpoint, params)
        self.save_json(response, startdate, enddate)


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

        with open(f'{output_dir}/{startdate}--{enddate}.json', 'w') as file:
            logger.info(f'Saving json response to {file.name}')
            json.dump(response.json(), file, ensure_ascii=False, indent=4)


    def api_requests(self) -> None:
        """Send multiple requests to OBIS api depending on total records
        of response and size limit"""
        records, num_records = self.get_records()
        startdate = self.startdate
        enddate = self.enddate
        max_size = self.size

        print(f'num_records: {num_records}')

        # if the total number of records exceeds the size attribute, a series of requests are made
        if max_size <= num_records:
            current_size = 0

            for i, rec in enumerate(records):
                current_size += rec['records']
                # update the startdate only if it has been set to None in elif block
                startdate = str(rec['year']) if startdate == None else startdate
                enddate = str(rec['year']) if enddate == None else enddate

                if len(records) -1 == i:
                    logger.info('Last record reached')
                    self.get_occurrences(startdate, self.enddate)
                elif current_size <= max_size:
                    logger.info(f"current_size: {current_size}/{max_size}, {rec['year']}")
                    enddate = str(rec['year'])
                # if a single year's records exceed max_size, save data in its own separate file
                elif rec['records'] > max_size:
                    logger.info(f"Records for {rec['year']} exceeds size {rec['records']}/{max_size}")
                    # save previous years
                    self.get_occurrences(startdate, enddate)
                    time.sleep(1.0)
                    # save exceeding year by itself
                    self.get_occurrences(str(rec['year']), str(rec['year']))
                    time.sleep(1.0)
                    current_size = 0
                    logger.info(f"Resetting, current_size: 0, {rec['year']}")
                    # do not set start/enddate to current record, set on next iteration
                    startdate = None
                    enddate = None
                else:
                    logger.info(f"end: {enddate}, current size {current_size}/{max_size}, {rec['year']}")
                    self.get_occurrences(startdate, enddate)
                    time.sleep(1.0)
                    current_size = rec['records']
                    logger.info(f"Resetting, current_size: {rec['records']}, {rec['year']}")
                    # After saving past records, set startdate to current record year
                    startdate = str(rec['year'])
                    enddate = str(rec['year'])
        # make a single request if size is not exceeded
        else:
            self.get_occurrences(startdate, enddate)
