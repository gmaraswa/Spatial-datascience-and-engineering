#
# Assignment2
#

import psycopg2
import sys
from sqlalchemy import create_engine
import geopandas as gpd
import pandas as pd
import numpy as np


# Do not close the connection inside this file i.e. do not perform connection.close() or engine.close()


def load_shape_data(engine, input_path):
    dfAirbnb = gpd.read_file(input_path)
    dfAirbnb.to_postgis("shape_data", engine)



def geoHash(connection, output_path1):
    sql = "select geometry,community, ST_GeoHash(geometry) as geoHash from shape_data order by community"
    dfJoined = gpd.read_postgis(sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    df1.sort_values(by=['community'], inplace=True)
    df1 = df1.drop('geometry', axis=1)
    df1.to_csv(output_path1,index=False)
    cur = connection.cursor()
    cur.execute("ALTER TABLE shape_data ADD COLUMN geoHashGeom Varchar(256)")
    cur.execute("UPDATE shape_data SET geoHashGeom = ST_GeoHash(geometry)")
    cur.close()
    connection.commit()


def downloadGeometry(connection, output_path1):
    sql = "select geometry,community, ST_AsText(ST_GeomFromGeoHash(geoHashGeom)) as cp from shape_data"
    dfJoined = gpd.read_postgis(sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    df1 = df1.drop('geometry', axis=1)
    df1.sort_values(by=['community'], inplace=True)
    df1.to_csv(output_path1,header=False,index=False)

    # with open(output_path1, 'w') as f:
    #     f.write(df1['cp'].astype(str).str.cat(sep='\n'))
    #     f.write('\n')
    connection.commit()


def explore_spatial_sql(connection, output_path7, output_path8):
    geoHash(connection, output_path8)#working
    downloadGeometry(connection,output_path7)#working





def write_output(results, output_path):
    f = open(output_path, "w")
    for values in results:
        f.write(str(values[0]) + "\n")
    f.close()
