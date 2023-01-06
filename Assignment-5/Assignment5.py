
import geopandas as gpd
import pandas as pd
import numpy as np
import math

## find the hotspot taxi zones in terms of taxi_pickups and write in the output_path
def computePointsInsideZones(zones, pickups):
	countPoints = gpd.sjoin(zones, pickups, how='left', predicate='contains').groupby('LocationID').agg(
		countPoints=('index_right', 'count'), sqcountPoints=('index_right', lambda x: len(x) ** 2))
	return countPoints


def computeIntersectingZones(taxi_zones):
	w = gpd.sjoin(taxi_zones, taxi_zones, how='inner', predicate='intersects')
	w = w[w['LocationID_left'] != w['LocationID_right']]
	return w


def computeMean(countPoints,n):
	X = countPoints['countPoints'].sum() / n
	return X


def computeStd(countPoints,X,n):
	countPoints['sqcountPoints'] = np.power(countPoints['countPoints'], 2)
	return np.sqrt((countPoints['sqcountPoints'].sum()/n)-( np.power(X,2)))


def computeWij(w, countPoints):
	wx = pd.merge(w, countPoints, left_on="LocationID_right", right_on="LocationID")
	wx = wx[["LocationID_left", "LocationID_right", "countPoints"]]
	wx_agg = wx.groupby('LocationID_left').agg({'countPoints': ['sum', 'count']})
	wx_agg.columns = ['sum', 'count']
	wx_agg = wx_agg.reset_index()
	return wx_agg


def computeG(wx_agg,X,S,n):
	G = wx_agg
	G['num'] = G['sum'] - X * G['count']
	G['den'] = (n * (G['count']) - (G['count']) * (G['count'])) / (n - 1.0)
	G['den'] = S * np.sqrt(G['den'])
	G.loc[G['den'] == 0, 'den'] = 0.0000000001
	G['ans'] = G['num'] / G['den']
	G = G.sort_values('ans', ascending=False)
	G = G[['LocationID_left', 'ans']]
	G.columns = ['LocationID', 'g_score']

	G= G.iloc[:50]
	return G



def find_hotspot(taxi_zones, taxi_pickups, output_path):
	taxi_pickups=loadDataToDataframe(taxi_pickups)
	taxi_zones = taxi_zones.to_crs("EPSG:4326")
	n=len(taxi_zones)
	#compute pickUpPoints inside geometry
	countPoints=computePointsInsideZones(taxi_zones,taxi_pickups)
	#computeIntersection
	w=computeIntersectingZones(taxi_zones)
	#compute Mean
	X=computeMean(countPoints,n)
	#compute Standard Deviation
	S=computeStd(countPoints,X,n)
	#compute Wij
	wx=computeWij(w,countPoints)
	#compute Gfactor
	G=computeG(wx,X,S,n)
	#printOutput
	G.to_csv(output_path, sep="\t",index=False)


def loadDataToDataframe(taxi_pickups):
	return gpd.GeoDataFrame(taxi_pickups, geometry=gpd.points_from_xy(taxi_pickups.pickup_longitude,
																			  taxi_pickups.pickup_latitude)
									,crs="EPSG:4326")


	





