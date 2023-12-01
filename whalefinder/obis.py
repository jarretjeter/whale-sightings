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

root_dir = Path().cwd()
file = open(f"{root_dir}/config.json", 'r')
config = json.loads(file.read())
# Whales Dictionary
whales = config['whales']

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))


class ObisAPI():
    """Object for obtaining whale data from specific OBIS api endpoints (https://api.obis.org/v3)
    (https://obis.org/)
    """
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
        if whale in whales:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whales dictionary. {whales.keys()}')
        self.startdate = startdate
        self.enddate = enddate
        self.size = size
        self.records, self.num_records = self.get_records()
        

    def get_records(self) -> Tuple[List[Dict], int]:
        """Retrieve total number of records from a request to the /statistics/years endpoint

        Returns:
            tuple[list[dict], int]
        """
        whale = self.whale
        num_records = 0
        url = f'https://api.obis.org/v3'
        scientificname = whales[whale]['scientificname']
        params = {'scientificname': scientificname}
        if self.startdate:
            params['startdate'] = self.startdate
        if self.enddate:
            params['enddate'] = self.enddate

        try:
            r = session.get(f"{url}/statistics/years", params=params)
            logger.info(f'Status code: {r.status_code}\n{r.url}')
            r.raise_for_status()
            records = r.json()

            # if a start or end date were not supplied, default values(earliest and latest records) will be retrieved from the endpoint response
            if not self.startdate:
                self.startdate = str(records[0]['year'])
            if not self.enddate:
                self.enddate = str(records[-1]['year'])
            for rec in records:
                num_records += rec['records']
            return records, num_records
        except requests.exceptions.RequestException as e:
            logger.info(e)


    def is_dateformat(self, date_strings: tuple) -> tuple:
        """Checks for date formats (YYYY-mm-dd)
        
        Args:
            date_strings: tuple[str, str]
                tuple of string values
        Returns:
            tuple of converted strings
        """
        start = date_strings[0]
        end = date_strings[1]
        try:
            if re.match(r"\d{4}-\d{2}-\d{2}", start):
                pass
            else:
                start = str(start) + '-01-01'
            if re.match(r"\d{4}-\d{2}-\d{2}", end):
                pass
            else:
                end = str(end) + '-12-31'
            return start, end
        except TypeError as e:
            logger.info(f"{e} \n {date_strings}, type: {type(date_strings[0]), type(date_strings[1])}")


    def get_occurrences(self, startdate: str, enddate: str) -> None:
        """Send a get request to the OBIS api's /occurrence endpoint

        Args:
            startdate, enddate: str
                start and end dates to query through
        Returns:
            None
        """
        whale = self.whale
        size = self.size
        url = f'https://api.obis.org/v3'
        scientificname = whales[whale]['scientificname']
        startdate, enddate = self.is_dateformat((startdate, enddate))
        params = {'scientificname': scientificname, 'startdate': startdate, 'enddate': enddate, 'size': size}
        
        try:
            scientificname = whales[whale]['scientificname']
            r = session.get(f"{url}/occurrence", params=params)
            logger.info(f'Status code: {r.status_code}\n{r.url}')
            r.raise_for_status()
            self.response = r
            self.save_json(startdate, enddate)
        except requests.exceptions.RequestException as e:
            logger.info(e)


    def save_json(self, startdate: str, enddate: str) -> None:
        """Save a `requests.Response` to a json file
        
        Args:
            startdate, enddate: str
                used for file naming
        Returns:
            None
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
        records = self.records
        num_records = self.num_records
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
