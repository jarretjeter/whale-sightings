import datetime
import json
import logging
from logging import INFO
import pandas as pd
from pathlib import Path
import requests
import sys
import typer
from typing import Optional

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger: logging.Logger = logging

obis_app = typer.Typer(no_args_is_help=True)


def filter_keys(response: requests.Response) -> tuple:
    """
    Remove irrelevant keys from returned response
    
    Args:
        response: `requests.Response`
    Returns:
        tuple[list[dict], list]
    """
    # Relevant keys to keep
    key_list = [
    'occurrenceID',
    'verbatimEventDate',
    'eventDate',
    'eventTime',
    'year',
    'month',
    'day',
    'decimalLatitude',  
    'decimalLongitude',
    'coordinatePrecision', 
    'coordinateUncertaintyInMeters',  
    'locality',
    'waterBody',
    'bathymetry',
    'sst',
    'sss',
    'shoredistance',
    'taxonRemarks',
    'individualCount',  
    'vernacularName',
    'specificEpithet',
    'scientificName', 
    'scientificNameID',
    'order',
    'orderid',  
    'family',
    'familyid',  
    'genus',
    'genusid',
    'species', 
    'speciesid',
    'rightsHolder',
    'ownerInstitutionCode',
    'recordedBy',
    'associatedMedia', 
    'basisOfRecord',  
    'occurrenceRemarks',  
    'bibliographicCitation'
    ]
    response = response.json()
    response_list = response['results']
    filtered_response_list = [{k:v for k, v in d.items() if k in key_list} for d in response_list if isinstance(d, dict)]
    return filtered_response_list, key_list


def output_json(response: requests.Response, endpoint_name: str, whale: str, start_date: str, end_date: str, param: Optional[str]=None):
    """
    Save a `requests.Response` to a json file

    Args:
        response: `requests.Response`
            response object to read
        endpoint_name: str
            subdirectory name
        whale: str
            final subdirectory name
        start_date: str
            part of filename
        end_date: str
            part of filename
        param: (optional) str
            subdirectory name
    Returns:
        None
    """
    data_dir = './data'
    if param != None:
        output_dir = Path(f'{data_dir}/{endpoint_name}/{param}/{whale}')
    else:
        output_dir = Path(f'{data_dir}/{endpoint_name}/{whale}')

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(f'{output_dir}/{start_date}--{end_date}.json', 'w') as file:
        logger.info(f"Saving response to json file: '{file.name}'")
        json.dump(response.json(), file, ensure_ascii=False, indent=4)


def convert_dates(date_string: str) -> datetime.date:
    """
    convert eventDate column strings to datetime.date

    Args:
        date_string: str
            a string in the format of a date
    Returns:
        `datetime.date`
    """
    # split string if it contains multiple datetimes
    if '/' in date_string:
        date_list = str.split(date_string, '/')
        # only interested in the initial date sighted for now
        date = pd.to_datetime(date_list[0]).date()
    else:
        date = pd.to_datetime(date_string).date()
    return date


def create_dataframe(response: requests.Response, endpoint_name: str, whale: str, start_date: str, end_date: str, param: Optional[str]=None):
    """
    Create a `pandas.DataFrame` from Obis API response

    Args:
        response: `requests.Response`
            response object to read
        endpoint_name: str
            subdirectory name
        whale: str
            final subdirectory name
        start_date: str
            part of filename
        end_date: str
            part of filename
        param: (optional) str
            subdirectory name
    Returns:
        `pandas.DataFrame`
    """
    filtered = filter_keys(response)
    response_list = filtered[0]
    key_list = filtered[1]
    data_dir = './data'
    if param != None:
        output_dir = Path(f'{data_dir}/{endpoint_name}/{param}/{whale}')
    else:
        output_dir = Path(f'{data_dir}/{endpoint_name}/{whale}')

    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.json_normalize(response_list)
    df = df.reindex(columns=key_list)
    df['eventDate'] = df['eventDate'].apply(convert_dates)
    logger.info(f"Saving dataframe to csv: '{output_dir}/{start_date}--{end_date}.csv'")
    df.to_csv(f'{output_dir}/{start_date}--{end_date}.csv', index=False)
    return df


@obis_app.command('obis_request')
def obis_request(whale: str, start_date: str, end_date: str, endpoint: str='occurrence', param: Optional[str]=None, json: bool=True, dataframe: bool=True, size: Optional[int]=5000) -> requests.Response:
    """
    Send a get request to the obis api (https://api.obis.org/v3) to find information of encounters with a specified whale species.

    Args:
        whale: str
            Whale to filter by
        start_date: str
            Date to start search in the format of `YYYY-MM-DD`
        end_date: str
            Date to end search in the format of `YYYY-MM-DD`
        endpoint: str
            API endpoint to request
        param: (optional) str
            endpoint parameter
        json: bool, default True
            Save response to a json file
        dataframe: bool, default True
            Save response to `pandas.DataFrame`
        size: (optional) int
            Maximum number of results allowed to be returned from response
    Returns:
        `requests.Response`
    """
    whale_list = {'blue_whale': {'scientific_name': 'Balaenoptera musculus'}, 'sperm_whale': {'scientific_name': 'Physeter macrocephalus'}}
    endpoints = ['occurrence', 'statistics']
    endpoint_params = {'occurrence': {'params': []}, 'statistics': {'params': ['years']}}
    assert whale in whale_list, f'Enter a whale from whale_list: {whale_list}'
    assert endpoint in endpoints, f'Use a correct API endpoint: {endpoints}'
    assert param in endpoint_params[endpoint]['params'] or param == None, f"Use a correct endpoint parameter: {endpoint_params}"
    
    api = 'https://api.obis.org/v3'
    try:
        scientific_name = whale_list[whale]['scientific_name']
        print(scientific_name)
        if param != None:
            url = f'{api}/{endpoint}/{param}?scientificname={scientific_name}&startdate={start_date}&enddate={end_date}&size={size}'
            url = url.replace(' ', '%20')
            r = requests.get(url)
        else:
            url = f'{api}/{endpoint}?scientificname={scientific_name}&startdate={start_date}&enddate={end_date}&size={size}'
            url = url.replace(' ', '%20')
            r = requests.get(url)
        print(r.status_code)
        if json:
            output_json(response=r, endpoint_name=endpoint, whale=whale, start_date=start_date, end_date=end_date, param=param)
        if dataframe:
            create_dataframe(response=r, endpoint_name=endpoint, whale=whale, start_date=start_date, end_date=end_date, param=param)
        return r
    except requests.exceptions.RequestException as e:
            logger.info(e)




if __name__ == '__main__':
    obis_app()