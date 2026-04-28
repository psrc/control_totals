import pandas as pd
from shapely import wkt
import sqlalchemy
import geopandas as gpd
import pyodbc

def read_from_elmer_geo(feature_class_name, cols, crs='epsg:2285'):
        """
        Returns the specified feature class as a geodataframe from ElmerGeo.

        Parameters
        ----------
        feature_class_name: the name of the featureclass in PSRC's ElmerGeo 
                        Geodatabase

        cols: list of columns to be read from the feature class

        crs: coordinate reference system
        """
        conn_str = 'mssql+pyodbc://SQLserver/ElmerGeo?driver=ODBC+Driver+17+for+SQL+Server'
        engine = sqlalchemy.create_engine(conn_str)
        con=engine.connect()
        # converts cols list to string for sql query
        cols_str = ', '.join(cols)

        df=pd.read_sql('select %s, Shape.STAsText() as geometry from %s' % 
                        (cols_str, feature_class_name), con=con)
        con.close()
        df['geometry'] = df['geometry'].apply(wkt.loads)
        gdf=gpd.GeoDataFrame(df, geometry='geometry', crs=crs)
        cols = [col for col in gdf.columns if col not in 
                ['Shape', 'GDB_GEOMATTR_DATA', 'SDE_STATE_ID']]
    
        return gdf[cols]

def read_from_elmer(table_name, columns):
        conn_str = 'mssql+pyodbc://SQLserver/Elmer?driver=ODBC+Driver+17+for+SQL+Server'
        engine = sqlalchemy.create_engine(conn_str)
        with engine.connect() as con:
                cols_str = ', '.join(columns)
                sql_query = f'select {cols_str} from {table_name}'
                df = pd.read_sql(sql=sql_query, con=con)
        return df