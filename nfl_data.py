import requests
import pandas as pd
from enum import Enum
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

"""
Client tool that queries data from the Pro Football Reference
(https://www.pro-football-reference.com/play-index/play_finder.cgi)
and saves it to a local SQLite database
"""


class Position(Enum):
    """
    Enumeration to pass to `build_pfr_url`
    Handles the different query parameters for different positions
    """
    QB = {'cstat': 'pass_att',
          'order_by': 'pass_rating',
          'name': 'quarterbacks'}
    RB = {'cstat': 'rush_att',
          'order_by': 'rush_yds',
          'name': 'runningbacks'}
    REC = {'cstat': 'rec',
           'order_by': 'rec_yds',
           'name': 'receivers'}
    DEF = {'cstat': 'tackles_solo',
           'order_by': 'sacks',
           'name': 'defense'}


def build_pfr_url(year: int = 2019,
                  week: int = 1,
                  position: Position = Position.QB,
                  offset: int = 0) -> str:
    """
    Abstracts away all the key value pairs in a Pro Football Reference query,
    and allows you to only select the most relevant values
    in a query from the PFR site
    (https://www.pro-football-reference.com/play-index/)
    """
    base = 'https://www.pro-football-reference.com/play-index/pgl_finder.cgi'

    # Not mandatory parameters
    # 'game_day_of_week': '',
    # 'game_location': '',
    # 'game_result': '',
    # 'handedness': '',
    # 'is_active': '',
    # 'is_hof': '',
    # 'league_id': '',
    # 'team_id': '',
    # 'opp_id': '',
    # 'c2stat': '',
    # 'c2comp': 'gt',
    # 'c2val': '',
    # 'c3stat': '',
    # 'c3comp': 'gt',
    # 'c3val': '',
    # 'c4stat': '',
    # 'c4comp': 'gt',
    # 'c4val': '',
    query = {
        'request': 1,
        'match': 'game',
        'year_min': year,
        'year_max': year,
        'season_start': 1,
        'season_end': -1,
        'age_min': 0,
        'age_max': 99,
        'game_type': 'A',
        'game_num_min': 0,
        'game_num_max': 99,
        'week_num_min': week,
        'week_num_max': week,
        'c1stat': position.value['cstat'],
        'c1comp': 'gt',
        'c1val': 1,
        'order_by': position.value['order_by'],
        'from_link': 1,
        'offset': offset
    }

    return f'{base}/?{urlencode(query)}'


def pfr_url_to_df(session: requests.sessions.Session,
                  url: str) -> pd.DataFrame:
    """
    Takes a URL that contains a query for the
    Pro Football Reference and returns a Pandas dataframe.

    This function will take care of cleaning the data, and ensuring all the
    data types are properly set
    """

    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            '(KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
        )
    }

    # Explicitly set the types of columns
    converters = {
        'Rk': lambda x: pd.to_numeric(x, errors='ignore'),
        'Age': lambda x: pd.to_numeric(x, errors='ignore'),
        'Date': lambda x: pd.to_datetime(x, errors='ignore'),
        'G#': lambda x: pd.to_numeric(x, errors='ignore'),
        'Week': lambda x: pd.to_numeric(x, errors='ignore'),
        'Cmp': lambda x: pd.to_numeric(x, errors='ignore'),
        'Att': lambda x: pd.to_numeric(x, errors='ignore'),
        'Cmp%': lambda x: pd.to_numeric(x, errors='ignore'),
        'Yds': lambda x: pd.to_numeric(x, errors='ignore'),
        'TD': lambda x: pd.to_numeric(x, errors='ignore'),
        'Int': lambda x: pd.to_numeric(x, errors='ignore'),
        'Rate': lambda x: pd.to_numeric(x, errors='ignore'),
        'Sk': lambda x: pd.to_numeric(x, errors='ignore'),
        'Yds.1': lambda x: pd.to_numeric(x, errors='ignore'),
        'Y/A': lambda x: pd.to_numeric(x, errors='ignore'),
        'AY/A': lambda x: pd.to_numeric(x, errors='ignore'),
    }

    logging.info(f'Downloading data from {url}')
    text = session.get(url, headers=headers).text
    df = pd.read_html(text, skiprows=1, header=0, converters=converters)[0]

    # Handle multiple pages
    while 'Next Page' in text:
        # This code essentially increments the 'offset' query
        # parameter by 100
        url = increase_url_offset(url)

        # Continue downloading for however many pages there are
        text = session.get(url, headers=headers).text
        df2 = pd.read_html(text,
                           skiprows=1,
                           header=0,
                           converters=converters)[0]

        # Keep adding the new pages to our existing df dataframe
        df = pd.concat([df, df2])

    # Filter out rows where the data repeats the header row
    df = df.loc[df.Player != 'Player']

    # Let Pandas redetermine the types of the columns now
    # That each column should have homogenous data types
    df = df.infer_objects()

    # Remove bad column from source data
    df = df.drop('Unnamed: 7', axis='columns')

    return df


def increase_url_offset(url: str, increase: int = 100) -> str:
    """
    Helper function for creating a new URL that acts as if
    we clicked 'Next Page' on the PFR site. It will return
    the same URL, except the 'offset' query parameter will add
    100 to itself.
    """
    url_parts = urlparse(url)
    query_args = parse_qs(url_parts.query)
    query_args['offset'][0] = int(query_args['offset'][0]) + increase
    query = urlencode(query_args, doseq=True)
    url_parts = url_parts._replace(query=query)
    url = urlunparse(url_parts)

    return url


years = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
weeks = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]


if __name__ == '__main__':
    import sqlite3
    import os
    import logging
    from itertools import product

    logging.basicConfig(level=logging.DEBUG)

    root_dir = os.path.dirname(os.path.realpath(__file__))
    sqlite_db = os.path.join(root_dir, 'nfl_data.sqlite3')
    con = sqlite3.connect(sqlite_db)
    with con:
        with requests.Session() as s:
            # Allow 100 connections in our HTTP pool
            adapter = requests.adapters.HTTPAdapter(pool_connections=100,
                                                    pool_maxsize=100)
            s.mount('https://', adapter)
            for position in Position:
                agg_df = pd.DataFrame()
                for query in product(years, weeks, [position]):
                    year, week, position = query
                    df = pfr_url_to_df(s, build_pfr_url(year, week, position))
                    agg_df = pd.concat([agg_df, df])

                agg_df.to_sql(position.value['name'],
                              con,
                              if_exists='replace')
