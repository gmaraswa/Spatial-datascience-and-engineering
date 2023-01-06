
from geotorch.datasets.grid import Processed
from torch.utils.data import DataLoader
import itertools
import numpy as np
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from geotorch.preprocessing import SparkRegistration, load_geo_data, load_data, load_geotiff_image, write_geotiff_image
from geotorch.preprocessing.enums import GeoFileType
from geotorch.preprocessing.enums import AggregationType
from geotorch.preprocessing.enums import GeoRelationship
from geotorch.preprocessing.raster import RasterProcessing as rp
from geotorch.preprocessing.grid import SpacePartition
from geotorch.preprocessing.grid import STManager as stm
from geotorch.preprocessing import Adapter
## import other necessary modules


## create the st_tensor and write it to output_path
def createSparkSession():
	spark = SparkSession.builder.master("local[*]").appName("Sedona App").config("spark.serializer",
																				 KryoSerializer.getName).config(
		"spark.kryo.registrator", SedonaKryoRegistrator.getName).config("spark.jars.packages",
																		"org.apache.sedona:sedona-python-adapter-3.0_2.12:1.2.0-incubating,org.datasyslab:geotools-wrapper:geotools-24.0").getOrCreate()
	SedonaRegistrator.registerAll(spark)
	sc = spark.sparkContext
	sc.setSystemProperty("sedona.global.charset", "utf8")
	SparkRegistration.set_spark_session(spark)

def create_zones_grid_df(zones_path):
	zones = load_geo_data(zones_path, GeoFileType.SHAPE_FILE)
	zones.CRSTransform("epsg:2263", "epsg:4326")

	zones_df = Adapter.rdd_to_spatial_df(zones)
	grid_df = SpacePartition.generate_grid_cells(zones_df, "geometry", 32, 32)
	return  grid_df

def taxi_pickup_dtatframe(pickup_path):
	taxi_df = load_data(pickup_path, data_format="csv", header="true")
	taxi_df = taxi_df.select("Trip_Pickup_DateTime", "Start_Lon", "Start_Lat")
	return taxi_df

def add_temporal_data(taxi_df):
	taxi_df = stm.get_unix_timestamp(taxi_df, "Trip_Pickup_DateTime", new_column_alias="converted_unix_time")
	taxi_df = stm.add_temporal_steps(taxi_df, timestamp_column="converted_unix_time", step_duration=1800,
									 temporal_steps_alias="timesteps_id")
	total_temporal_setps = stm.get_temporal_steps_count(taxi_df, temporal_steps_column="timesteps_id")
	taxi_df = stm.add_spatial_points(taxi_df, lat_column="Start_Lat", lon_column="Start_Lon",
									 new_column_alias="point_loc")
	return total_temporal_setps,taxi_df

def create_st_tensor(path_to_shape_file, path_to_csv_file, output_path):
	createSparkSession()
	grid_df=create_zones_grid_df(path_to_shape_file)
	taxi_pickup_df=taxi_pickup_dtatframe(path_to_csv_file)
	taxi_pickup_df = stm.trim_on_datetime(taxi_pickup_df, target_column="Trip_Pickup_DateTime", upper_date="2009-01-30 23:59:59",
								   lower_date="2009-01-02 00:00:00")
	total_temporal_setps,taxi_pickup_df=add_temporal_data(taxi_pickup_df)
	column_list = ["point_loc"]
	agg_types_list = [AggregationType.COUNT]
	alias_list = ["point_cnt"]
	st_df = stm.aggregate_st_dfs(grid_df, taxi_pickup_df, "geometry", "point_loc", "cell_id", "timesteps_id",
								 GeoRelationship.CONTAINS, column_list, agg_types_list, alias_list)
	st_tensor = stm.get_st_grid_array(st_df, "timesteps_id", "cell_id", alias_list,
									  temporal_length=total_temporal_setps, height=32, width=32, missing_data=0)
	st_tensor = np.swapaxes(st_tensor, 1, 3)
	st_tensor = np.swapaxes(st_tensor, 2, 3)
	np.save(output_path, st_tensor)

## returns the periodical representation of st_tensor
def load_tensor_periodical(path_to_tensor, len_closeness, len_period, len_trend, T_closeness, T_period, T_trend, batch_size, batch_index):
	processed_obj = Processed(path_to_tensor, len_closeness=len_closeness, len_period=len_period, len_trend=len_trend, T_closeness=T_closeness
							  , T_period=T_period,
							  T_trend=T_trend)
	dataloader = DataLoader(processed_obj, batch_size=batch_size)
	for i, sample in enumerate(dataloader):
		if i==batch_index:
			return sample["x_closeness"], sample["x_period"], sample["x_trend"], sample["y_data"]
	return None

## returns the sequential representation of st_tensor
def load_tensor_sequential(path_to_tensor, len_history, len_predict, batch_size, batch_index):
	processed_obj = Processed(path_to_tensor)
	processed_obj.merge_closeness_period_trend(history_length=len_history, predict_length=len_predict)
	dataloader = DataLoader(processed_obj, batch_size=batch_size)
	for i, sample1 in enumerate(dataloader):
		if i == batch_index:
			return sample1["x_data"], sample1["y_data"]
	return None




