import pandas as pd
import requests


class CensusApi:
    def __init__(self, api_key, timeout=15):
        self.api_key = api_key
        self.timeout = timeout

    def get_table(self, variables, year, for_predicates, in_predicates, dataset_url):
        """
        Takes in a list of variables and returns a dataframe
        """
        HOST = "https://api.census.gov/data"
        base_url = "/".join([HOST, str(year), dataset_url])
        chunks = [variables[x:x+45] for x in range(0, len(variables), 45)]
        df = pd.DataFrame()
        for chunk in chunks:
            predicates = {}
            predicates["get"] = ",".join(chunk)
            predicates["for"] = for_predicates
            if in_predicates is not None:
                predicates["in"] = in_predicates
            predicates["key"] = self.api_key
            r = requests.get(base_url, params=predicates, timeout=self.timeout)
            chunk_df = pd.DataFrame(r.json()[1:], columns=r.json()[0])
            if df.empty:
                df = chunk_df
            else:
                df.drop(columns=['state', 'county', 'tract'], inplace=True, errors='ignore')
                df = df.merge(chunk_df, left_index=True, right_index=True)
        return df

    @staticmethod
    def combine_groups(variables_dict, df):
        """
        Takes in a dictionary of variables and a dataframe and sums any variables that 
        are made up of multiple census columns.
        """
        for key, value in variables_dict.items():
            df[key] = df[value].astype(float).sum(axis=1)
            df = df.drop(value, axis=1)
        return df

    @staticmethod
    def create_in_predicates(geog, county_ids, state_id):
        """
        Takes in a geography and returns in_predicates
        """
        county_ids_str = [str(county_id)[2:] for county_id in county_ids]
        if geog in ['tract', 'block group', 'block']:
            counties_str = ','.join(county_ids_str)
            in_predicates = f'state:{str(state_id)}', f'county:{counties_str}'
        elif geog in ['county', 'place', 'congressional district']:
            in_predicates = f'state:{str(state_id)}'
        elif geog == 'state':
            in_predicates = None
        else:
            raise ValueError("geog must be: 'state', 'county', 'congressional district', 'place', 'tract', 'block group' or 'block'")
        return in_predicates

    def get_dec_data(self, variables_dict, year, geog, dataset, county_ids, state_id):
        """
        Takes in a dictionary of variables and returns decennial data in a dataframe.
        """
        in_predicates = self.create_in_predicates(geog, county_ids, state_id)
        for_predicates = f'{geog}:*'
        dataset_url = f'dec/{dataset}'
        start_vars = ['GEO_ID', 'NAME']
        variables = [i for j in variables_dict.values() for i in j]
        variables = start_vars + variables
        df = self.get_table(variables, year, for_predicates, in_predicates, dataset_url)
        df = self.combine_groups(variables_dict, df)
        df = self.create_geoid(geog, df)
        df.rename(columns={'NAME': 'name'}, inplace=True)
        df = df[['geoid', 'name'] + list(variables_dict.keys())]
        return df

    def create_geoid(self, geog, df):
        geog_slices = {
            'block': -15,
            'tract': -11,
            'block group': -12,
            'county': -5,
            'place': -7,
            'state': -2
        }
        df['geoid'] = df['GEO_ID'].str.slice(start=geog_slices[geog]).astype('int64')
        return df
