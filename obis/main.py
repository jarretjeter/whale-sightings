import datetime as dt
from manager import *
from obis_class import *


def main():
    now = str(dt.datetime.now().date())
    obis = Obis('blue_whale', '1758-01-01', now)
    obis.obis_requests()
    manager = WhaleDataManager('blue_whale', '1758-01-01', now)
    manager.create_dataframe()



if __name__ == '__main__':
    print('Running pipeline')
    main()
    print('Pipeline finished')