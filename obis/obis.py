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


def filter_keys(json_response: dict) -> list:
    """
    Remove irrelevant keys from returned json response
    
    Args:
        json_response: dict
    Returns:
        list[dict]
    """
    # Relevant keys to keep
    key_list = [
    'associatedMedia', 
    'bibliographicCitation',  
    'ownerInstitutionCode',
    'recordedBy',
    'rightsHolder',
    'verbatimEventDate',
    'coordinatePrecision', 
    'coordinateUncertaintyInMeters',  
    'bathymetry',
    'decimalLatitude',  
    'decimalLongitude',
    'locality',
    'shoredistance',
    'sst',
    'sss',
    'waterBody',
    'basisOfRecord',  
    'occurrenceRemarks',  
    'taxonRemarks',
    'individualCount',  
    'occurrenceID',
    'order',
    'orderid',  
    'family',
    'familyid',  
    'genus',
    'genusid',
    'species', 
    'speciesid',
    'scientificName',  
    'scientificNameID',  
    'specificEpithet',
    'vernacularName'
    ]
    response_list = json_response['results']
    new_response_list = [{k:v for k, v in d.items() if k in key_list} for d in response_list if isinstance(d, dict)]
    return new_response_list


def output_json(response: requests.Response, endpoint_name: str, whale: str, param: Optional[str]=None, start_date: Optional[str]=None, end_date: Optional[str]=None):
    """
    Save a `requests.Response` to a json file

    Args:
        whale: str
            final subdirectory name
        endpoint: str
            subdirectory name
        param: (optional) str
            subdirectory name
        start_date: (optional) str
            part of filename
        end_date: (optional) str
            part of filename
    Returns:
        None
    """
    data_dir = './data'
    if param != None:
        output_dir = Path(f'{data_dir}/{endpoint_name}/{param}/{whale}')
    else:
        output_dir = Path(f'{data_dir}/{endpoint_name}/{whale}')

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(f'{output_dir}/{start_date}-{end_date}.json', 'w') as file:
        logger.info('Saving response to json file.')
        json.dump(response.json(), file, ensure_ascii=False, indent=4)


@obis_app.command('obis_request')
def obis_request(whale: str='blue_whale', endpoint: str='occurrence', param: Optional[str]=None, start_date: Optional[str]=None, end_date: Optional[str]=None, output: bool=True) -> requests.Response:
    """
    Send a get request to the obis api (https://api.obis.org/v3) to find information of encounters with a specified whale species.

    Args:
        whale: str
            Whale to filter by
        endpoint: str
            API endpoint to request
        param: (optional) str
            endpoint parameter
        start_date: (optional) str
            Date to start search in the format of `YYYY-MM-DD`
        end_date: (optional) str
            Date to end search in the format of `YYYY-MM-DD`
    Returns:
        `requests.Response`
    """
    whale_list = {'blue_whale': {'scientific name': 'Balaenoptera musculus'}}
    endpoints = ['occurrence', 'statistics']
    endpoint_params = {'occurrence': {'params': []}, 'statistics': {'params': ['years']}}
    assert whale in whale_list, f'Enter a whale from whale_list: {whale_list}'
    assert endpoint in endpoints, f'Use a correct API endpoint: {endpoints}'
    assert param in endpoint_params[endpoint]['params'] or param == None, f"Use a correct endpoint parameter: {endpoint_params}"
    
    api = 'https://api.obis.org/v3'
    try:
        scientific_name = whale_list[whale]['scientific name']
        print(scientific_name)
        if param != None:
            url = f'{api}/{endpoint}/{param}?scientificname={scientific_name}&startdate={start_date}&enddate={end_date}'
            url = url.replace(' ', '%20')
            r = requests.get(url)
        else:
            r = requests.get(f'{api}/{endpoint}?scientificname={scientific_name}&startdate={start_date}&enddate={end_date}')
        print(r.status_code)
        if output:
            output_json(response=r, endpoint_name=endpoint, whale=whale, param=param, start_date=start_date, end_date=end_date)
        return r
    except requests.exceptions.RequestException as e:
            logger.info(e)


if __name__ == '__main__':
    obis_app()