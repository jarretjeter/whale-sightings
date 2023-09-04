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
    """Object for working with the OBIS api (https://api.obis.org/v3)
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
                Maximum number of allowed results returned
                The API does not accept a size limit greater than 10,000
        """
        if whale in self.whales:
            self.whale = whale
        else:
            raise ValueError(f'{whale} not in whales dictionary. {self.whales.keys()}')
        self.start = start_date
        self.end = end_date
        self.size = size
        

# ERROR HANDLE STATUS CODES
    def get(self) -> None:
        """Send a get request to the OBIS api
        """
        whale = self.whale
        start = self.start
        end = self.end
        size = self.size
        try:
            if start is None and end is None:
                scientific_name = self.whales[whale]['scientific_name']
                # request earliest and latest records to get a start and end date
                url = f'https://api.obis.org/v3/statistics/years?scientificname={scientific_name}'
                r = requests.get(url)
                print(r.status_code)
                years = r.json()
                start = str(years[0]['year'])
                start = start + '-01-01'
                end = str(years[-1]['year'])
                end = end + '-12-31'
                # query occurrence endpoint with start and end date set
                url = f'https://api.obis.org/v3/occurrence?scientificname={scientific_name}&startdate={start}&enddate={end}&size={size}'
                url = url.replace(' ', '%20')
                r = requests.get(url)
                print(r.status_code)
                self.response = r
            else:
                scientific_name = self.whales[whale]['scientific_name']
                url = f'https://api.obis.org/v3/occurrence?scientificname={scientific_name}&startdate={start}&enddate={end}&size={size}'
                url = url.replace(' ', '%20')
                r = requests.get(url)
                print(r.status_code)
                self.response = r
        except requests.exceptions.RequestException as e:
            logger.info(e)



    def save_json(self) -> None:
        """Save a `requests.Response` to a json file
        """
        whale = self.whale
        start = self.start
        end = self.end
        data_dir = self.data_dir
        # Only call Obis.get() if the object doesn't currently have the 'response' attribute
        if not hasattr(self, 'response'):
            self.get()

        response = self.response
        output_dir = Path(f'{data_dir}/{whale}')

        output_dir.mkdir(parents=True, exist_ok=True)
        with open(f'{output_dir}/{start}--{end}.json', 'w') as file:
            logger.info(f'Saving json response to {file.name}')
            json.dump(response.json(), file, ensure_ascii=False, indent=4)


# if __name__ == '__main__':
#     obis = Obis('blue_whale', '1994-01-01', '1994-12-31')
#     obis.get()
#     obis.save_json()