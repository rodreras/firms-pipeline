import pandas as pd
import requests
from datetime import datetime, timedelta 

def get_transaction_count(map_key: str) -> int:
    url = 'https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/'
    params = {'MAP_KEY': map_key}
    count = 0
    try:
        response = requests.get(url, params=params)
        data = response.json()
        df = pd.Series(data) 
        count = df['current_transactions']
    except Exception as e:
        print(e)
        raise Exception('Error in API call.')
    return count

def get_data(
    map_key: str, 
    satellite: str, 
    bbox: list[int] | Literal['world'], 
    day_range: int, 
    date: Optional[str] = None
    ) -> pd.DataFrame:

    if isinstance(bbox, list):
        bbox_str = ','.join(map(str, bbox))
    else:
        bbox_str = bbox

    url_data = f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{map_key}/{satellite}/{bbox_str}/{day_range}'

    if date:
        url_data = f'{url_data}/{date}'

    return pd.read_csv(url_data)

def rolling_dates(date: str):
    dt = datetime.strptime(date, "%Y-%m-%d").date()
    
    current = dt
    today = datetime.today().date()
    while current < today:
        span = min(5, (today - current).days)
        yield (current, span)
        current += timedelta(days=span)
    