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


def getNumberOfGeometriesIn(connection, output_path1):
    join_sql = "select A.geometry,count(B.geom) as count from shape_data A join point_data B on " \
               "ST_Contains(A.geometry,B.geom) group by 1 order by count "
    dfJoined = gpd.read_postgis(join_sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    with open(output_path1, 'w') as f:
        f.write(df1['count'].astype(str).str.cat(sep='\n'))
        f.write('\n')

def getAdjacent(connection, output_path1):
    join_sql = "select A.geometry,count(B.Geometry) as count from shape_data A join shape_data B on " \
               "ST_Distance(A.geometry,B.geometry)=0  group by 1 order by count "
    dfJoined = gpd.read_postgis(join_sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    with open(output_path1, 'w') as f:
        f.write(df1['count'].astype(str).str.cat(sep='\n'))
        f.write('\n')

def getHausdorffDistance(connection, output_path1):
    join_sql = "select geometry,ST_HausdorffDistance( ST_SetSRID(ST_MakePoint(" \
               "-87.60914087617012, 41.84469250346108), 4326),A.geometry) as distance from shape_data A order by distance"
    dfJoined = gpd.read_postgis(join_sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    with open(output_path1, 'w') as f:
        f.write(df1['distance'].astype(str).str.cat(sep='\n'))
        f.write('\n')

def getMaxDistance(connection, output_path1):
    join_sql = "select geometry, ST_MaxDistance(A.geometry, ST_SetSRID(ST_MakePoint(" \
               "-87.60914087617012, 41.84469250346108), 4326)) as distance from shape_data A order by distance"
    dfJoined = gpd.read_postgis(join_sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    with open(output_path1, 'w') as f:
        f.write(df1['distance'].astype(str).str.cat(sep='\n'))
        f.write('\n')

def getConvexHull(connection, output_path1):
    join_sql = "select geometry, st_length(st_boundary(ST_ConvexHull(A.geometry))) as boundry from shape_data A order by boundry"
    dfJoined = gpd.read_postgis(join_sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    with open(output_path1, 'w') as f:
        f.write(df1['boundry'].astype(str).str.cat(sep='\n'))
        f.write('\n')

def outerBoundryArea(connection, output_path):
    sql = "select geometry, st_Area(A.geometry) " \
          "as area from shape_data A where ST_GeometryType(A.geometry)='ST_Polygon' " \
          " order by area"
    dfJoined = gpd.read_postgis(sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    with open(output_path, 'w') as f:
        f.write(df1['area'].astype(str).str.cat(sep='\n'))
        f.write('\n')


def closestPointInMinimumBoundingRectangle(connection, output_path1):
    sql = "select geometry,ST_AsText(ST_ClosestPoint( ST_Envelope(A.geometry),ST_SetSRID(ST_MakePoint(" \
               "-87.60914087617012, 41.84469250346108), 4326))) as cp from shape_data A " \
          " order by cp"
    dfJoined = gpd.read_postgis(sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    with open(output_path1, 'w') as f:
        f.write(df1['cp'].astype(str).str.cat(sep='\n'))
        f.write('\n')


def areaOfSharedPortion(connection, output_path1):
    sql = "select geometry,ST_Area(ST_Intersection(A.geometry,ST_MakePolygon(ST_MakeLine(ARRAY[ST_SetSRID(ST_MakePoint(-87.69227959522789, 41.85766547551493),4326)," \
          "ST_SetSRID(ST_MakePoint(-87.69227959522789, 41.88908028505862),4326),ST_SetSRID(ST_MakePoint(-87.63450859376373, 41.88908028505862),4326),ST_SetSRID(ST_MakePoint(-87.63450859376373, " \
          "41.85766547551493),4326),ST_SetSRID(ST_MakePoint(-87.69227959522789, 41.85766547551493),4326)])) )) as cp from shape_data A " \
          " order by cp"
    dfJoined = gpd.read_postgis(sql, connection, geom_col="geometry")
    df1 = pd.DataFrame(dfJoined)
    with open(output_path1, 'w') as f:
        f.write(df1['cp'].astype(str).str.cat(sep='\n'))
        f.write('\n')

def createPointTable(connection, input_path):
    f = open(input_path)
    cur = connection.cursor()
    cur.execute("CREATE TABLE point_data(  latitude double precision,  longitude double precision);")
    cur.copy_from(f, 'point_data', sep=",")
    cur.execute("ALTER TABLE point_data ADD COLUMN geom geometry(Point, 4326);")
    cur.execute("UPDATE point_data SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);")
    cur.close()
    connection.commit()


def explore_spatial_sql(connection, input_path, output_path1, output_path2, output_path3, output_path4, output_path5,
                        output_path6, output_path7, output_path8):
    createPointTable(connection, input_path)
    getNumberOfGeometriesIn(connection, output_path1)
    getAdjacent(connection, output_path2)
    getHausdorffDistance(connection,output_path3) #error
    getMaxDistance(connection,output_path4) #error
    getConvexHull(connection,output_path5)
    outerBoundryArea(connection,output_path6)
    closestPointInMinimumBoundingRectangle(connection,output_path7)# error
    areaOfSharedPortion(connection,output_path8)#working




def write_output(results, output_path):
    f = open(output_path, "w")
    for values in results:
        f.write(str(values[0]) + "\n")
    f.close()
