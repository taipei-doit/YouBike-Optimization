"""
preprocess.py - Data Preprocessing Module

This module contains a class and functions for preprocessing and cleaning data before analysis
or modeling. It includes various data cleaning and transformation techniques to prepare data for
further processing or analysis.

Classes:
- Preprocess: A class that handles data preprocessing tasks.

"""

import configparser
import datetime
import glob
import itertools
import json
import os
import pickle
import re
import warnings
from ast import literal_eval
from typing import Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import pykml.parser
import rasterio as rio
import rasterstats
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.merge import merge
from rasterio.warp import Resampling, calculate_default_transform, reproject
from shapely.geometry import LineString, Point, Polygon, shape
from shapely.geometry.multipolygon import MultiPolygon
from skimage import measure
from tqdm import tqdm

pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

class Preprocess:
    """
    Preprocess - Data Preprocessing Class

    This class provides methods for preprocessing and cleaning data before analysis or modeling.
    It includes various data cleaning and transformation techniques to prepare data for further
    processing or analysis.

    Functions:
    - overlay_within_ppl: Calculate the intersection area within two Polygon series.

    - transaction: Preprocess transaction data, including stop data and transaction data.

    - population: Preprocess population data, including age demographics and population counts.

    - traffic_mrt: Preprocess traffic data for MRT stations, including entrance and
      exit points, station information, and passenger counts.

    - traffic_bus: Preprocess traffic data for bus stops, including geospatial overlay.

    - traffic_bus_route: Preprocess bus route data.

    - road_side_walk: Preprocess sidewalk data, including geospatial overlay.

    - road_marked_side_walk: Preprocess marked sidewalk data, including geospatial overlay.

    - road_bike_route: Preprocess bike route data, including geospatial overlay and
      length calculation.

    - road_tree: Preprocess tree data, including geospatial overlay.

    - road_light: Preprocess light data, including geospatial overlay.

    - road_road_area: Preprocess road network data.

    - road_road_length: Preprocess road length data.

    - terrain_dtm: Preprocess terrain data about DTM.

    - terrain_ndvi: Preprocess terrain data about NDVI.

    - development: Preprocess development data.

    - land_landuse: Preprocess land use data.

    - land_building: Preprocess land building data.

    - poi: Preprocess point of interest (poi) data.

    - save_df: Save preprocessed data to a CSV file.

    """

    def __init__(self, date_list):
        # date
        self.date_list = date_list
        self.hyphen_date_list = [x[0:4] + '-' + x[4:6] + '-' + x[6:8] for x in self.date_list]

        # config
        config = configparser.ConfigParser()
        config.read('input/params.ini', encoding='utf-8')
        ini = config['INI']
        self.target = literal_eval(ini['target'])

        # file path
        file_path = config['FILEPATH']
        # pylint: disable=C0103
        self.transaction_file_path = literal_eval(file_path['transaction'])
        self.population_file_path = literal_eval(file_path['population'])
        self.traffic_file_path = literal_eval(file_path['traffic'])
        self.road_file_path = literal_eval(file_path['road'])
        self.terrain_file_path = literal_eval(file_path['terrain'])
        self.land_file_path = literal_eval(file_path['land'])
        self.poi_file_path = literal_eval(file_path['poi'])
        self.done_file_path = 'output_ON' if self.target == 'on' else 'output_OFF'
        # pylint: enable=C0103

        # grid information
        self.grid_df = gpd.read_file(
            f'{self.population_file_path}/FET_2023_grid_97.geojson'
        ).set_crs('epsg:3826')
        self.grid_df['Area'] = self.grid_df.geometry.area

        # preprocess constants params
        preprocess = config['PREPROCESS']
        # pylint: disable=C0103
        self.NDVI_THRESHOLD = literal_eval(preprocess['NDVI_THRESHOLD'])
        self.POI_FILTER_NUM = literal_eval(preprocess['POI_FILTER_NUM'])
        self.CONTOUR_INTERVAL = literal_eval(preprocess['CONTOUR_INTERVAL'])
        self.SLOPE_BINS = literal_eval(preprocess['SLOPE_BINS'])
        self.SLOPE_BINS_LABEL = literal_eval(preprocess['SLOPE_BINS_LABEL'])
        self.ROAD_CATEGORY = literal_eval(preprocess['ROAD_CATEGORY'])
        self.BUILDING_CATEGORY = literal_eval(preprocess['BUILDING_CATEGORY'])
        self.LANDUSE_CATEGORY = literal_eval(preprocess['LANDUSE_CATEGORY'])
        self.LANDUSE_CATEGORY_CODE = literal_eval(preprocess['LANDUSE_CATEGORY_CODE'])
        # pylint: enable=C0103

    # get the intersection area within two Polygon series
    def overlay_within_ppl(self, df1, df2, key1, key2, method):

        # df1: 'polygon df' or 'point df' or 'line df'
        # df2: 'grid df'
        # method: 'Polygon' or 'Point' or 'Line'

        overlay_df = gpd.overlay(
            df1, df2, how='union'
        ).explode().reset_index()
        overlay_df = overlay_df[[key1, key2, 'geometry']]
        overlay_df = overlay_df[overlay_df[key1].notnull()]
        overlay_df = overlay_df[overlay_df[key2].notnull()]
        overlay_df[key2] = overlay_df[key2].astype(int)
        overlay_df.index = range(len((overlay_df)))

        if method == 'Polygon':
            overlay_df['area'] = overlay_df.geometry.area
        elif method == 'Line':
            overlay_df['length'] = overlay_df.geometry.length
        elif method == 'Point':
            pass

        return overlay_df

    # transaction
    def transaction(self):
        # read stop data
        stop_df = pd.read_csv(
            f'{self.transaction_file_path}/86ec099baa2d36c22ab3a87350b718de_export.csv'
        )
        stop_df = stop_df[['sno', 'lat', 'lng']]
        stop_df['sno'] = stop_df['sno'].astype(str)
        stop_df['sno'] = 'U' + stop_df['sno'].str[3:]
        stop_df['geometry'] = stop_df.apply(lambda x: Point((x.lng, x.lat)), axis=1)
        stop_df = gpd.GeoDataFrame(stop_df, crs=4326)
        stop_df = stop_df.to_crs(3826)

        # read transaction data
        df = pd.DataFrame()
        for date in self.date_list:
            tem_file = f'{self.transaction_file_path}/202303_txn_identified_transfer/{date}.pkl'
            with open(tem_file,'rb') as tem:
                tem_file = pickle.load(tem)
                df = pd.concat([df, tem_file])
        df.index = range(0, len(df))

        # convert on_time(datetime), off_time(datetime) to on_hour(int), off_hour(int)
        # hardcore the location of date and hour
        df['on_date'] = df['on_time'].astype(str).str[:10].str.split('-').str.join('')
        df['off_date'] = df['off_time'].astype(str).str[:10].str.split('-').str.join('')
        df['on_hour'] = df['on_time'].astype(str).str[11:13].astype(int)
        df['off_hour'] = df['off_time'].astype(str).str[11:13].astype(int)

        # complete mapping table
        on_stop_id = list(set(df['on_stop_id']))
        on_date = list(set(df['on_date']))
        on_hour = list(set(df['on_hour']))
        on_columns = ['on_stop_id', 'on_date', 'on_hour']
        off_columns = ['off_stop_id', 'off_date','off_hour']
        product_elements = list(
            itertools.product(on_stop_id, on_date, on_hour)
        )
        on_mapping_df = pd.DataFrame(
            product_elements, columns=on_columns
        )
        off_mapping_df = pd.DataFrame(
            product_elements, columns=off_columns
        )

        # on (借車)
        on_df = df.groupby(
            on_columns).size().reset_index(name='counts')
        on_df = on_df.merge(
            on_mapping_df, how='right', on=on_columns
        )
        on_df.fillna(0, inplace=True)

        # off (還車)
        off_df = df.groupby(
            off_columns).size().reset_index(name='counts')
        off_df = off_df.merge(
            off_mapping_df, how='right', on=off_columns
        )
        off_df.fillna(0, inplace=True)

        # stop_id's information include lat, lng
        on_df = on_df.merge(
            stop_df, how='left', left_on='on_stop_id', right_on='sno'
        ).drop(
            ['sno'], axis=1
        )
        off_df = off_df.merge(
            stop_df, how='left', left_on='off_stop_id', right_on='sno'
        ).drop(
            ['sno'], axis=1
        )

        # get 'on' geometry, 'off' geometry and Grid geometry
        on_point = gpd.GeoDataFrame(on_df[['on_stop_id', 'geometry']])
        off_point = gpd.GeoDataFrame(off_df[['off_stop_id', 'geometry']])
        grid_poly = self.grid_df[['gridid', 'geometry']]

        # drop duplicate
        on_point.drop_duplicates(inplace=True)
        off_point.drop_duplicates(inplace=True)
        on_point.index = range(len(on_point))
        off_point.index = range(len(off_point))

        # overlay
        overlay_on_df = self.overlay_within_ppl(
            on_point, grid_poly, 'on_stop_id', 'gridid', method='Point'
        )
        overlay_off_df = self.overlay_within_ppl(
            off_point, grid_poly, 'off_stop_id', 'gridid', method='Point'
        )

        # mapping
        on_df = on_df.merge(
            overlay_on_df.drop(['geometry'], axis=1), how='left', on='on_stop_id'
        )
        off_df = off_df.merge(
            overlay_off_df.drop(['geometry'], axis=1), how='left', on='off_stop_id'
        )

        # kick out NA, reset index and astype
        on_df = on_df[on_df.gridid.notnull()]
        off_df = off_df[off_df.gridid.notnull()]
        on_df.index = range(len(on_df))
        off_df.index = range(len(off_df))
        on_df[['counts', 'gridid']] = on_df[['counts', 'gridid']].astype(int)
        off_df[['counts', 'gridid']] = off_df[['counts', 'gridid']].astype(int)
        return on_df, off_df

    # population (人口信令)
    def population(self):
        # read data
        population_df = pd.read_csv(
            f'{self.population_file_path}/台北市停留人口_資料集_1.csv'
        )
        work_df = pd.read_csv(
            f'{self.population_file_path}/台北市停留人口_資料集_2_工作人口.csv'
        )
        live_df = pd.read_csv(
            f'{self.population_file_path}/台北市停留人口_資料集_2_居住人口.csv'
        )
        tour_df = pd.read_csv(
            f'{self.population_file_path}/台北市停留人口_資料集_2_遊客人口.csv'
        )
        population_df['日期'] = population_df['日期'].str.split('-').str.join('')
        work_df['日期'] = work_df['日期'].str.split('-').str.join('')
        live_df['日期'] = live_df['日期'].str.split('-').str.join('')
        tour_df['日期'] = tour_df['日期'].str.split('-').str.join('')

        # by age and total
        df = population_df.groupby(
            ['日期', '時間', '網格編號', '年齡別']).sum('放大後人數').reset_index()
        df = pd.pivot_table(
            df, values='放大後人數', index=['日期', '時間', '網格編號'], columns='年齡別'
        ).reset_index().fillna(0)

        df.columns = [
            '日期', '時間', '網格編號', 'Age_15_17_Counts', 'Age_18_21_Counts',
            'Age_22_29_Counts', 'Age_30_39_Counts', 'Age_40_49_Counts',
            'Age_50_59_Counts', 'Age_60_64_Counts', 'Age_Over65_Counts'
        ]
        total_cols = [
            'Age_15_17_Counts', 'Age_18_21_Counts','Age_22_29_Counts',
            'Age_30_39_Counts', 'Age_40_49_Counts', 'Age_50_59_Counts',
            'Age_60_64_Counts', 'Age_Over65_Counts'
        ]
        df['Age_Total_Counts'] = df[total_cols].sum(axis=1)

        merge_cols = ['日期', '時間', '網格編號']
        # by work
        df = df.merge(
            work_df, how='left', on=merge_cols
        )
        df.rename(
            columns={'放大後人數': 'WorkPopulationCounts'}, inplace=True
        )

        # by live
        df = df.merge(
            live_df, how='left', on=merge_cols
        )
        df.rename(
            columns={'放大後人數': 'LivePopulationCounts'}, inplace=True
        )

        # by tour
        df = df.merge(
            tour_df, how='left', on=merge_cols
        )
        df.rename(
            columns={'放大後人數': 'TourPopulationCounts'}, inplace=True
        )
        return df

    # traffic for MRT
    def traffic_mrt(self):
        # read data
        mrt_df = pd.read_csv(
            f'{self.traffic_file_path}/臺北捷運車站出入口座標.csv', encoding='big5'
        )
        population = pd.read_csv(
            f'{self.traffic_file_path}/臺北捷運每日分時各站OD流量統計資料_202303.csv'
        )

        ## information of MRT Station and Exit
        mrt_df = mrt_df[['出入口名稱', '經度', '緯度']]
        mrt_df['geometry'] = mrt_df.apply(
            lambda x: Point((x.經度, x.緯度)), axis=1
        )
        mrt_df = gpd.GeoDataFrame(mrt_df, crs=4326)
        mrt_df = mrt_df.to_crs(3826)

        # get exit information
        exit_point = mrt_df[['出入口名稱', 'geometry']]

        # get station information
        station_point = mrt_df[['出入口名稱', '經度', '緯度']]
        station_point['Station'] = station_point['出入口名稱'].str.split('站出口').str[0]
        station_point = station_point[['經度', '緯度', 'Station']].groupby(
            'Station').agg(lng_mean=('經度', 'mean'), lat_mean=('緯度', 'mean')).reset_index()
        tpe_main_station = station_point[station_point['Station'].str.contains('台北車站')]
        tpe_main_station = pd.DataFrame(
            data={
                'Station': ['台北車站'],
                'lng_mean': [tpe_main_station.lng_mean.mean()],
                'lat_mean': [tpe_main_station.lat_mean.mean()]
            }
        )
        station_point = pd.concat(
            [station_point[~station_point['Station'].str.contains('台北車站')], tpe_main_station],
            ignore_index=True
        )
        station_point['geometry'] = station_point.apply(
            lambda x: Point((x.lng_mean, x.lat_mean)), axis=1
        )
        station_point = gpd.GeoDataFrame(station_point, crs=4326)
        station_point = station_point.to_crs(3826)
        station_point = station_point[['Station', 'geometry']]

        grid_poly = self.grid_df[['gridid', 'geometry']]

        # overlay exits
        overlay_exit_df = self.overlay_within_ppl(
            exit_point, grid_poly, '出入口名稱', 'gridid', method='Point'
        )

        # groupby grid and count the number of the exits
        mrt_exit_counts_df = (overlay_exit_df.drop('geometry', axis=1)
                           .groupby('gridid')
                           .size()
                           .reset_index(name='MRTExitCounts'))

        # overlay stations
        overlay_station_df = self.overlay_within_ppl(
            station_point, grid_poly, 'Station', 'gridid', method='Point'
        )

        ## population of MRT Stop (各站點進出人次)
        population = population[population['日期'].isin(self.hyphen_date_list)]
        population_in = population[['日期', '時段', '進站', '人次']]
        population_out = population[['日期', '時段', '出站', '人次']]
        population_in.index = range(0, len(population_in))
        population_out.index = range(0, len(population_out))

        # calculate number of people by day, hour, station
        population_in = population_in.groupby(
            ['日期', '時段', '進站']).sum('人次').reset_index()
        population_out = population_out.groupby(
            ['日期', '時段', '出站']).sum('人次').reset_index()

        # get GRID ID
        population_in = population_in.merge(
            overlay_station_df, how='inner', left_on='進站', right_on='Station'
        )
        population_out = population_out.merge(
            overlay_station_df, how='inner', left_on='出站', right_on='Station'
        )

        # restructure date type
        population_in['日期'] = population_in['日期'].str.split('-').str.join('')
        population_out['日期'] = population_out['日期'].str.split('-').str.join('')

        # Calculate MRT population based on grid
        mrt_population_cols = ['gridid', '日期', '時段']
        mrt_population_in_sum_df = population_in[mrt_population_cols+['人次']].groupby(
            mrt_population_cols).sum().reset_index()
        mrt_population_out_sum_df = population_out[mrt_population_cols+['人次']].groupby(
            mrt_population_cols).sum().reset_index()

        return mrt_exit_counts_df, mrt_population_in_sum_df, mrt_population_out_sum_df

    # traffic for BUS
    def traffic_bus(self):
        # read bus data
        bus_point = gpd.read_file(
            f'{self.traffic_file_path}/busstop/busstop.shp'
        ).set_crs('epsg:4326').to_crs('epsg:3826')
        bus_point = bus_point[['BSM_BUSSTO', 'geometry']]

        grid_poly = self.grid_df[['gridid', 'geometry']]

        # overlay
        overlay_df = self.overlay_within_ppl(
            bus_point, grid_poly, 'BSM_BUSSTO', 'gridid', 'Point'
        )

        overlay_df = (overlay_df.groupby('gridid')
                      .size()
                      .reset_index(name='BusStopCounts')
                      .astype({'BusStopCounts':'int'}))

        return overlay_df

    # road for bus route
    # bus routes
    def traffic_bus_routes(self)->pd.DataFrame:
        """
        Calculate the number of bus routes in each grid.

        Returns:
            pandas.DataFrame: A DataFrame containing the number of bus routes and grid IDs.
        """
        # Import data
        bus_routes = pd.read_csv(f'{self.traffic_file_path}/bus_station_detailed.csv')

        # Convert DataFrame to GeoDataFrame
        bus_routes_gdf = (gpd.GeoDataFrame(
            bus_routes[['RouteNum']],
            geometry=gpd.points_from_xy(bus_routes['lon'], bus_routes['lat']),
            crs = 'epsg:4326'
        ).to_crs('epsg:3826'))

        # Aggregate the bus stops into the grids
        bus_agg_df = ((gpd.sjoin(
            self.grid_df[['gridid', 'geometry']],
            bus_routes_gdf,
            predicate='intersects',
            how='inner',))
            .groupby(['gridid']).agg({'RouteNum': 'sum'})
            .rename(columns={'RouteNum': 'BusRouteCounts'})
            .reset_index()
            .astype({'BusRouteCounts':'int'}))

        # fill 0 for the grids without bus stops
        bus_routes_df = (pd.merge(self.grid_df[['gridid']], bus_agg_df, how='left',on='gridid')
                         .fillna(0))

        return bus_routes_df

    # road for side walk
    def road_side_walk(self):
        # read SideWalk data
        with open(f'{self.road_file_path}/TP_SIDEWORK.json', encoding='utf-8') as f:
            sidewalk_df = json.load(f)
            sidewalk_df = pd.json_normalize(sidewalk_df['features'])
            sidewalk_df = gpd.GeoDataFrame(sidewalk_df)
            sidewalk_df['geometry'] = [
                Polygon(x[0][0])
                for x in sidewalk_df['geometry.coordinates']
        ]

        # get sidewalk geometry and Grid geometry
        sw_poly = sidewalk_df[['properties.ObjectID', 'geometry']]
        grid_poly = self.grid_df[['gridid', 'geometry']]

        # overlay
        overlay_df = self.overlay_within_ppl(
            sw_poly, grid_poly, 'properties.ObjectID', 'gridid', method='Polygon'
        )

        # groupby grid and rename column
        overlay_df = (overlay_df[['gridid', 'area']].groupby('gridid')
                      .sum()
                      .reset_index()
                      .rename(columns={'area':'SideWalkArea'}))
        return overlay_df

    # road for Marked SideWalk data
    def road_marked_side_walk(self):
        # read Marked SideWalk data
        marked_side_walk_df = gpd.read_file(
            f'{self.road_file_path}/(交工處)標線型人行道圖資_202304171730/grapline_21_15.shp'
        )

        # get sidewalk geometry and Grid geometry
        msw_poly = marked_side_walk_df[['KEYID', 'geometry']]
        grid_poly = self.grid_df[['gridid', 'geometry']]

        # overlay
        overlay_df = self.overlay_within_ppl(
            msw_poly, grid_poly, 'KEYID', 'gridid', method='Polygon'
        )

        # groupby grid and rename column
        overlay_df = (overlay_df[['gridid', 'area']].groupby('gridid')
                      .sum()
                      .reset_index()
                      .rename(columns={'area': 'MarkedSideWalkArea'}))

        return overlay_df

    # road for Bike
    def road_bike_route(self):
        # read data
        tem_file = f'{self.road_file_path}/台北市_自行車道-市區自行車道1120505.kml'
        with open(tem_file, 'r', encoding='utf-8') as f:
            root = pykml.parser.fromstring(f.read())
        name = [
            x.name.text
            for x in root.findall('.//{http://www.opengis.net/kml/2.2}Placemark')
        ]
        coor_ = [
            tuple(str(x.LineString.coordinates).split(',0 ')[0:-1])
            for x in root.findall('.//{http://www.opengis.net/kml/2.2}Placemark')
        ]
        coor = []
        for x in coor_:
            coor.append([literal_eval(y) for y in x])

        bike_lines = pd.DataFrame(
            data={
                'name': name,
                'geometry': coor
            }
        )
        bike_lines = bike_lines[bike_lines.geometry.map(len)>1]
        bike_lines['geometry'] = bike_lines.geometry.apply(LineString)
        bike_lines = gpd.GeoDataFrame(
            bike_lines, crs=4326
        )
        bike_lines = bike_lines.to_crs(3826)

        # get road of bike DataFrame and Grid DataFrame
        gdf_poly = self.grid_df[['gridid', 'geometry']]

        # get intersection LineString between all road of bike and all Grid
        overlay_df = gpd.overlay(
            bike_lines, gdf_poly, how='union'
        ).explode().reset_index(drop=True)

        # get Taipei df
        tpe_overlay_df = overlay_df[overlay_df['gridid'].notnull()]
        tpe_overlay_df.index = range(len(tpe_overlay_df))
        tpe_overlay_df['gridid'] = tpe_overlay_df['gridid'].astype(int)

        # calaculate Length of road of bike
        tpe_overlay_df['length'] = tpe_overlay_df.geometry.length

        # groupby grid and rename the column
        tpe_overlay_df = (tpe_overlay_df[['gridid', 'length']].groupby('gridid')
                          .sum()
                          .reset_index()
                          .rename(columns={'length':'BikeRouteLength'}))
        return tpe_overlay_df

    # road for tree
    def road_tree(self):
        # read data
        with open(f'{self.road_file_path}/TaipeiTree.json', encoding='utf-8') as f:
            tree_df = json.load(f)
        tree_df = pd.json_normalize(tree_df)
        tree_df['LatLng'] = list(zip(tree_df.X, tree_df.Y))
        tree_df = gpd.GeoDataFrame(tree_df)
        tree_df['geometry'] = tree_df.LatLng.apply(Point)

        # get Tree geometry and Grid geometry
        tree_point = tree_df[['TreeID', 'geometry']]
        grid_poly = self.grid_df[['gridid', 'geometry']]

        # overlay
        overlay_df = self.overlay_within_ppl(
            tree_point, grid_poly, 'TreeID', 'gridid', method='Point'
        )

        # groupby grid and rename the column
        overlay_df = (overlay_df.groupby('gridid')
                      .size()
                      .reset_index(name='TreeCounts')
                      .astype({'TreeCounts':'int'}))

        return overlay_df

    # road for light
    def road_light(self):
        # read data
        with open(f'{self.road_file_path}/TaipeiLight.json', encoding='utf-8') as f:
            light_df = json.load(f)
        light_df = pd.json_normalize(light_df)
        light_df = light_df[light_df.X != '']
        light_df = light_df[light_df.Y != '']
        light_df.index = range(len(light_df))
        light_df['LatLng'] = list(
            zip(light_df.X, light_df.Y)
        )
        light_df = gpd.GeoDataFrame(light_df)
        light_df['geometry'] = light_df.LatLng.apply(Point)

        # get Tree geometry and Grid geometry
        light_point = light_df[['LIGHTID', 'geometry']]
        grid_poly = self.grid_df[['gridid', 'geometry']]

        # overlay
        overlay_df = self.overlay_within_ppl(
            light_point, grid_poly, 'LIGHTID', 'gridid', method='Point'
        )

        overlay_df = (overlay_df.groupby('gridid')
                      .size()
                      .reset_index(name='LightCounts')
                      .astype({'LightCounts':'int'}))
        return overlay_df

    # road for road net
    # road area
    def road_road_area(self)->pd.DataFrame:
        """
        Calculate the road area in each grid.

        Returns:
            pandas.DataFrame: A DataFrame containing the calculated road area and grid IDs.
        """
        # read data
        road_area = gpd.read_file(os.path.join(self.road_file_path, 'Road/Road.shp'))

        # Overlay grid and road_polygon
        overlay_df = gpd.overlay(self.grid_df[['gridid', 'geometry', 'Area']],
                                     road_area,
                                     how='intersection')
        overlay_df['area'] = overlay_df.area

        # Sum the road area of each grid
        overlay_df = overlay_df.groupby('gridid')['area'].sum()

        road_area_df = (self.grid_df[['gridid', 'Area']].merge(overlay_df,
                                                               how='left',
                                                               on='gridid').fillna(0))

        # Calculate the road area ratio for each grid
        road_area_df['ratio'] = (road_area_df['area'] / road_area_df['Area'])*100

        road_area_df = (road_area_df.drop(['Area', 'area'], axis=1)
                        .rename(columns={'ratio':'RoadAreaRatio'})
                        .round(6))

        return road_area_df

    # road for road length
    # road length
    def road_road_length(self)->pd.DataFrame:
        """
        Calculate the road length in each grid.

        Returns:
            pandas.DataFrame: A DataFrame containing the calculated road length and grid IDs.
        """
        # Import data
        road_line = gpd.read_file(os.path.join(self.road_file_path, 'MidRoad/MidRoad.shp'),
                                  crs='epsg:3826')

        road_lengths = {}
        for category, road_type in self.ROAD_CATEGORY.items():
            selected_roads = road_line[road_line['ROADTYPE'].isin(road_type)]
            road_overlay = gpd.overlay(selected_roads[['ROADTYPE', 'geometry']],
                                       self.grid_df[['gridid', 'geometry']],
                                       how='intersection')
            road_overlay['length'] = road_overlay.length
            road_lengths[category] = road_overlay.groupby('gridid')['length'].sum()

        road_length_df = self.grid_df[['gridid']].copy()

        for idx, length_by_grid in enumerate(road_lengths.values()):
            road_length_df = road_length_df.merge(length_by_grid,
                                                  how='left',
                                                  on='gridid',
                                                  suffixes=(None, f'_{idx}')).fillna(0)

        road_length_df = road_length_df.rename(
            columns={
                road_length_df.columns[1]: list(self.ROAD_CATEGORY)[0],
                road_length_df.columns[2]: list(self.ROAD_CATEGORY)[1],
                road_length_df.columns[3]: list(self.ROAD_CATEGORY)[2],
                road_length_df.columns[4]: list(self.ROAD_CATEGORY)[3],
            })

        road_length_df['TotalRoadLength'] = road_length_df.iloc[:, 1:].sum(axis=1)
        road_length_df = road_length_df.round(6)

        return road_length_df

    # terrain
    def terrain_dtm(self)->pd.DataFrame:
        """Calculate the DTM in each grid."""

        def merge_dtms()->Tuple[np.ndarray, rio.Affine]:
            """
            Merge all of 20M DTMs in Taipei

            Returns:
                Tuple[numpy.ndarray, Affines]:
                    A tuple containing the merged DTM data as a NumPy array and the transformation
                    information as an Affine object.
            """
            # Import dtm tiles
            dtm_input_files = list(
                glob.glob(f'{self.terrain_file_path}/dtm_images/分幅_臺北市20MDEM/*.grd'))

            # set the crs for the dtm files
            new_crs = rio.crs.CRS({'init': 'epsg:3826'})

            # Merge the DTM data
            with MemoryFile() as memfile:
                datasets = []
                nodata_value = None

                for file in dtm_input_files:
                    with rio.open(file) as src:
                        bounds = src.bounds
                        res = src.res

                        # create a new affine
                        new_transform = rio.Affine(res[0], 0, bounds[0], 0, -res[1], bounds[1])

                        # Update the new meta
                        new_meta = src.meta.copy()
                        new_meta.update({
                            'count': 1,    # 1 band
                            'driver': 'XYZ',
                            'crs': new_crs,
                            'transform': new_transform,
                            'nodata': None,
                        })
                        data = src.read()
                        dataset = memfile.open(**new_meta)

                        # Flipping the array of data (up down)
                        dataset.write(np.array([np.flipud(data[0])]))
                    datasets.append(dataset)

                # return the mosaic data
                mosaic_data, mosaic_transform = merge(datasets, nodata=nodata_value)

            mosaic_data = mosaic_data[0]
            return mosaic_data, mosaic_transform

        mosaic_data, mosaic_transform = merge_dtms()

        # make nodata as np.nan
        mosaic_mask = (mosaic_data == 0) # pylint: disable=C0325
        mosaic_data[mosaic_mask] = np.nan

        # set interval and create contours
        max_val = int(self.CONTOUR_INTERVAL *
                      round(np.amax(mosaic_data[~mosaic_mask]) / self.CONTOUR_INTERVAL))

        contour_line_collection = []
        for elev in range(0, max_val, self.CONTOUR_INTERVAL):    # get each contour
            contours = measure.find_contours(mosaic_data, elev)

            for contour in contours:
                contour_geo = [rio.transform.xy(mosaic_transform, x, y) for x, y in contour]
                geom = shape({'type': 'LineString', 'coordinates': contour_geo})
                feature = {'geometry': geom}
                feature['ELEV'] = {'ELEV': elev}
                new_df = pd.DataFrame(feature)
                contour_line_collection.append(new_df)

        total_contours_df = pd.concat(contour_line_collection, ignore_index=True)
        total_contours_gdf = gpd.GeoDataFrame(total_contours_df,
                                              geometry='geometry',
                                              crs='epsg:3826')

        # Get the intersected points between grid and contour
        copy_grid_df = self.grid_df.copy()
        copy_grid_df.geometry = copy_grid_df.geometry.exterior

        intersection_points = (
            gpd.overlay(total_contours_gdf,
                        copy_grid_df[['gridid', 'geometry']],
                        how='intersection',
                        keep_geom_type=False)    # keep the non-linestring geometry object
        )

        # multipoint to single point
        single_point = intersection_points[intersection_points.geom_type == 'MultiPoint'].explode(
            index_parts=True)
        # count the intersected point for each grid
        single_point_grouped = pd.DataFrame(single_point.groupby(['gridid'])['ELEV'].count())
        # Calculate the slope
        single_point_grouped['Slope'] = single_point_grouped.apply(
            lambda count: ((count * 3.14 * self.CONTOUR_INTERVAL) /
                           (8 * 250)) * 100    # 250 means the grid width/hieght
        )

        # convert the continous slope into discrete classes
        # convert the category dtype into int
        single_point_grouped['SlopeClass'] = (pd.cut(single_point_grouped['Slope'],
                                                right=True,
                                                bins=self.SLOPE_BINS,
                                                labels=self.SLOPE_BINS_LABEL).astype(int))

        fill_values = {'Slope':0, 'SlopeClass':1}
        all_grid_slope = (pd.merge(self.grid_df[['gridid']],
                                  single_point_grouped.reset_index(),
                                  on='gridid',
                                  how='left')
                                  .fillna(value=fill_values)
                                  .drop(['ELEV'], axis='columns'))


        def dtm_zonal_stats(mosaic_data:np.ndarray, mosaic_transform:rio.Affine) -> pd.DataFrame:
            """
            Calculate zonal statistics by grids to obtain the mean DTM value for each grid.

            Args:
                mosaic_data (numpy.ndarray): The DTM data as a NumPy array.
                mosaic_transform (Affine): The transformed affine for the DTM data (epsg:3826).

            Returns:
                pd.DataFrame: A DataFrame containing the grid IDs and the mean DTM values
                              for each grid.

            """

            # zonal statistics for getting mean NDVI of each grid
            dtm_zonal = rasterstats.zonal_stats(
                self.grid_df[['geometry']],    # geometry object/ epsg:3826
                mosaic_data,    # ndarray
                affine=mosaic_transform,    # the transformed affine (epsg:3826)
                stats='mean',    # Get the mean NDVI by grid
                nodata=0,
            )

            # Assign gridid for the output of zonal statistics
            dtm_zonal_df = pd.concat(
                [self.grid_df[['gridid']],
                 pd.DataFrame(dtm_zonal, columns=['mean']).rename(columns={'mean':'ElevMean'})],
                axis='columns',
            )
            return dtm_zonal_df

        dtm_zonal_df = dtm_zonal_stats(mosaic_data, mosaic_transform)

        all_grid_slope = pd.merge(all_grid_slope, dtm_zonal_df, on='gridid', how='inner')

        return all_grid_slope

    # NDVI
    def terrain_ndvi(self)->pd.DataFrame:
        """
        Calculate the NDVI(Normalized Difference Vegetation Index) in each grid.

        Returns:ㄉㄢ
            pandas.DataFrame: A DataFrame containing the mean NDVI values and tree coverage
                              for each grid.
        """
        # Import satellite data (Download from https://apps.sentinel-hub.com/eo-browser/)
        ndvi_b04_fp = os.path.join(
            self.terrain_file_path,
            r'ndvi_images/2023-03-06-00_00_2023-03-06-23_59_Sentinel-2_L2A_B04_(Raw).tiff',
        )
        ndvi_b08_fp = os.path.join(
            self.terrain_file_path,
            r'ndvi_images/2023-03-06-00_00_2023-03-06-23_59_Sentinel-2_L2A_B08_(Raw).tiff',
        )
        with rio.open(ndvi_b04_fp) as src:
            ndvi_b04 = src.read(1)
            bounds = src.bounds
            meta = src.meta.copy()
        with rio.open(ndvi_b08_fp) as src:
            ndvi_b08 = src.read(1)

        # NDVI formula: (NIR - R) / (NIR + R)
        np.seterr(invalid='ignore')
        # Calculate NDVI and Ensure that the denominator is not 0
        ndvi_values = np.where((ndvi_b08 + ndvi_b04) == 0, 0,
                               (ndvi_b08 - ndvi_b04) / (ndvi_b08 + ndvi_b04))
        ndvi_values = ndvi_values.astype(np.float32)

        def ndvi_zonal_stats(meta:dict, bounds:tuple, ndvi_values:np.ndarray) -> pd.DataFrame:
            """
            zonal statistics by grids, get the mean NDVI value for each grid.

            Args:
                meta (dict): Metadata information for the NDVI data.
                bounds (tuple): Geographic bounds of the NDVI data.
                ndvi_values (numpy.ndarray): NDVI values.

            Returns:
                pandas.DataFrame: A DataFrame containing the mean NDVI values for each grid.
            """

            # Reproject transform from epsg:4326 to epsg:3826
            dst_crs = CRS.from_epsg(3826)
            reprojected_transform, reprojected_width, reprojected_height = (
                calculate_default_transform(
                meta['crs'], dst_crs, meta['width'], meta['height'], *bounds))

            # Reproject values from epsg:4326 to epsg:3826
            reprojected_shape = (reprojected_height, reprojected_width)
            reprojected_ndvi_values = np.empty(reprojected_shape, dtype = ndvi_values.dtype)
            reproject(
                source=ndvi_values,
                destination= reprojected_ndvi_values,
                src_transform= meta['transform'],
                src_crs= meta['crs'],
                dst_transform= reprojected_transform,
                dst_crs= dst_crs,
                resampling= Resampling.nearest # Use appropriate resampling method
            )

            # Zonal statistics for getting mean NDVI of each grid
            ndvi_zonal = rasterstats.zonal_stats(
                self.grid_df[['geometry']],    # geometry object/ epsg:3826
                reprojected_ndvi_values,    # ndarray
                affine=reprojected_transform,    # the transformed affine (epsg:3826)
                stats='mean',    # Get the mean NDVI by grid
                nodata=0,
            )

            # Assign gridid for the output of zonal statistics
            ndvi_zonal_df = pd.concat(
                [self.grid_df[['gridid']],
                 pd.DataFrame(ndvi_zonal, columns=['mean']).rename(columns={'mean':'NdvMean'})],
                axis='columns',
            )
            return ndvi_zonal_df

        def ndvi_coverage(ndvi_values:np.ndarray) -> pd.DataFrame:
            """
            Calculate the coverage of tree pixels in each grid based on NDVI values.

            Args:
                ndvi_values (numpy.ndarray): NDVI values.

            Returns:
                pandas.DataFrame: A DataFrame containing the calculated tree coverage for each grid.
            """
            # Defined tree and non-tree pixels code
            tree_value = 2
            non_tree_value = 1

            # Classify tree and non-tree by NDVI_THRESHOLD
            classified_data = np.where(ndvi_values >= self.NDVI_THRESHOLD, tree_value,
                                       non_tree_value)
            classified_data = classified_data.astype(np.int32)

            # Polygonize the raster data with tree value
            shapes = list(rio.features.shapes(classified_data, transform=meta['transform']))
            polygons = [shape(geom) for geom, value in shapes if value == tree_value]

            # Create a new geodataframe for vectorized polygon
            tree_vector = (gpd.GeoDataFrame({
                'fid': [tree_value]},
                geometry=[MultiPolygon(polygons)
            ]).set_crs('epsg:4326').to_crs('epsg:3826'))

            # Intersects between grid and tree multipolygon
            ndvi_coverage_df = (gpd.overlay(tree_vector[['geometry']],
                                            self.grid_df[['gridid', 'geometry']],
                                            how='intersection'))

            # Calculate coverage
            ndvi_coverage_df['area'] = ndvi_coverage_df.area
            ndvi_coverage_df = ndvi_coverage_df.groupby('gridid')['area'].sum()
            ndvi_coverage_df = (pd.merge(self.grid_df[['gridid', 'Area']],
                                         ndvi_coverage_df,
                                         how='left',
                                         on='gridid').fillna(0))
            ndvi_coverage_df['NdviCoverage'] = (ndvi_coverage_df['area'] /
                                            ndvi_coverage_df['Area']) * 100

            # Drop redundance columns
            ndvi_coverage_df.drop(['area', 'Area'], axis=1, inplace=True)

            return ndvi_coverage_df

        # Get the zonal and coverage
        ndvi_zonal_df = ndvi_zonal_stats(meta, bounds, ndvi_values)
        ndvi_coverage_df = ndvi_coverage(ndvi_values)

        # Concat the coverage and zonal together
        ndvi_df = pd.merge(ndvi_zonal_df, ndvi_coverage_df, how='inner', on='gridid')
        ndvi_df = ndvi_df.round(6)

        return ndvi_df

    def _sindex_intersection(
        self,
        input_data: gpd.GeoDataFrame,
        tree_data: gpd.GeoDataFrame,
        input_columns: list = None,
        tree_columns: list = None,
        area: bool = True,
        geometry: bool = True,
    ) -> pd.DataFrame:
        """
        Perform spatial intersection using a spatial index between two GeoDataFrames.

        Using spatial index to intersect between two polygon data,
        tree_data means the data with spatial index
        input_data means the input of the function sindex.query()

        Args:
            input_data:
            tree_data:
            input_columns: the column you wanted to keep from the input data
            tree_columns: the column you wanted to keep from the tree data
            area: If true, return the area of each intersected polygon as a new column.
            geometry: If true, return the geometry info as a new column in the dataframe.
        Returns:
            pandas.DataFrame: A DataFrame containing specified columns from input_data
            and tree_data, along with optional area and geometry columns for intersected
            polygons.
        """
        # the columns for the optput dataframe
        columns = input_columns + tree_columns
        if area:
            columns.append('area')
        if geometry:
            columns.append('geometry')
        sindex_output_df = pd.DataFrame(columns=columns)

        # create the spatial index for the tree data
        sindex = tree_data.sindex
        # Using spatial index to intersects
        idx = sindex.query(input_data['geometry'], predicate='intersects')

        row_list = []
        for input_idx, tree_idx in zip(idx[0], idx[1]):
            selected_input = input_data.iloc[input_idx]
            selected_tree = tree_data.iloc[tree_idx]
            intersects = selected_tree.geometry.intersection(selected_input.geometry)

            # Eliminate the invalid geometry types and insert row data information
            row = {}
            if intersects.geom_type in ['Polygon', 'MultiPolygon']:
                if input_columns:
                    for col in input_columns:
                        row[col] = selected_input[col]
                if tree_columns:
                    for col in tree_columns:
                        row[col] = selected_tree[col]
                if area:
                    row['area'] = intersects.area
                if geometry:
                    row['geometry'] = intersects

                row_list.append(row)

        sindex_output_df = pd.concat([sindex_output_df, pd.DataFrame(row_list)],
                                     axis=0,
                                     ignore_index=True)
        return sindex_output_df


    def _landuse_calculate(self)->pd.DataFrame:
        """
        Calculate land use statistics for each grid.

        Returns:
            pd.DataFrame: A DataFrame containing land use category codes, areas, and grid IDs.
        """
        # Import landuse data
        landuse = gpd.read_file(f'{self.land_file_path}/landuse_108.gpkg', layer='landuse_108')

        # mapping landuse code to landuse category code
        landuse['category'] = landuse['code'].map(
            lambda code: self.LANDUSE_CATEGORY.get(str(code), 'unknown'))

        input_columns = ['code', 'category']
        tree_columns = ['gridid']

        landuse_df = self._sindex_intersection(landuse,
                                               self.grid_df,
                                               input_columns,
                                               tree_columns,
                                               area=True,
                                               geometry=True)

        return landuse_df

    # land(土地使用現況)
    def land_landuse(self)->pd.DataFrame:
        """
        Calculate land use ratios for each grid.

        Returns:
            pd.DataFrame: A DataFrame containing land use ratios for each grid.

        """
        landuse_df = self._landuse_calculate()

        landuse_output_df = landuse_df.pivot_table(index='gridid',
                                                   columns='category',
                                                   values='area',
                                                   aggfunc='sum').fillna(0)

        landuse_output_df = pd.merge(self.grid_df[['gridid', 'Area']],
                                     landuse_output_df,
                                     how='left',
                                     on='gridid')

        for code, name in self.LANDUSE_CATEGORY_CODE.items():
            landuse_output_df[f'{name}Ratio'] = (landuse_output_df[str(code)] /
                                                 landuse_output_df['Area'])
            landuse_output_df.drop(str(code), axis=1, inplace=True)

        landuse_output_df.drop('Area', axis=1, inplace=True)
        landuse_output_df = landuse_output_df.round(6)

        return landuse_output_df

    # development(容積)
    def land_building(self)->pd.DataFrame:
        """
        Calculate building volume and category ratios for each grid.

        Returns:
            pd.DataFrame: A DataFrame containing building volume, category ratios,
            and other relevant data for each grid.
        """

        # Import building data
        building = gpd.read_file(os.path.join(self.land_file_path, 'building.gpkg'),
                                 layer='building')

        # Fix building geometry
        building.geometry = building.geometry.buffer(0)

        # Get the landuse Ratio
        landuse_df = self._landuse_calculate()
        landuse_df = gpd.GeoDataFrame(landuse_df, geometry='geometry', crs='epsg:3826')

        # Get each building's floor data
        input_columns = ['1_floor']
        tree_columns = ['gridid', 'code']
        development_df = self._sindex_intersection(building,
                                                  landuse_df,
                                                  input_columns,
                                                  tree_columns,
                                                  area=True)

        development_df['floor_area'] = development_df['1_floor'] * development_df['area']

        development_df_pt = (development_df.pivot_table(index='gridid',
                                                       columns='code',
                                                       values='floor_area',
                                                       aggfunc='sum')
                                            .fillna(0))

        # Sum of total building area
        development_df_pt['All'] = development_df_pt.sum(axis=1)

        # Filter selected columns
        selected_category = list(self.BUILDING_CATEGORY)
        building_category = selected_category.copy()
        building_category.append('All')

        # Filtered table
        development_df_pt = development_df_pt[building_category]
        development_df_pt['Others'] = (
            development_df_pt['All'] -
            development_df_pt.loc[:, selected_category].sum(axis=1)
            # Others = Total - selected categories
        )

        # Rename columns
        development_df_pt.rename(columns=self.BUILDING_CATEGORY, inplace=True)

        # # Merge back to the origin grid
        development_df_pt = (pd.merge(self.grid_df[['gridid', 'Area']], development_df_pt,
                                                                   how='left',
                                                                   on='gridid')
                                                            .fillna(0))

        # Add ratio columns
        for value in self.BUILDING_CATEGORY.values():
            development_df_pt[f'{value}Ratio'] = (development_df_pt[value] /
                                                   development_df_pt['Area'])

        development_df_pt['OthersRatio'] = (development_df_pt['Others'] /
                                             development_df_pt['Area'])
        development_df_pt['AllRatio'] = development_df_pt['All'] / development_df_pt['Area']

        development_df_pt = (development_df_pt.round(6)
                             .drop(['Area'], axis=1))

        return development_df_pt


    # poi
    # POI
    def poi(self)->pd.DataFrame:
        """
        Calculate the number of the point of interest(POI) in each grid.

        Returns:
            pandas.DataFrame: A DataFrame containing the calculated number of POIs of various types
                              and grid IDs.(17 types)
                              (art gallery/bar/book store/cafe/clothing store/convenience store
                              department store/drugstore/laundary/lodging/museum/night_club/
                              restaurant/shopping mall/store/supermarket/tourist attraction)
        """

        # Import data
        poi_file_path_list = list(glob.glob(f'{self.poi_file_path}/*.csv'))

        def poi_preprocessing(poi_file_path:str)->pd.DataFrame:
            """
            Perform preprocessing for single type of POI data

            Args:
                poi_file_path (str): The file path to the POI data CSV file.
            Returns:
                pandas.DataFrame: A DataFrame containing aggregated POI data for each grid.

            """
            # Get the POI name by the original file name
            layer_name = ''.join(
                re.search(r'poi_(.*?)\.csv', poi_file_path).group(1).capitalize().split('_'))

            # import poi data
            poi_df = pd.read_csv(poi_file_path)

            # Convert DataFrame into GeoDataFrame
            poi_gdf = (
                gpd.GeoDataFrame(
                    poi_df[['rating_num', 'rating']],
                    geometry=gpd.points_from_xy(poi_df.lng.astype(float), poi_df.lat.astype(float)),
                    crs='epsg:4326'
                ).to_crs('epsg:3826')    # destinate crs
            )

            # POIs intersect with grid and compute the aggregation
            poi_agg_df = (
                gpd.sjoin(
                    self.grid_df[['gridid', 'geometry']],
                    poi_gdf,
                    predicate='intersects',
                    how='inner')
                .loc[lambda x: x['rating_num'] > self.
                    POI_FILTER_NUM]  # filter the POI rating less than the threshold POI_FILTER_NUM
                .groupby(['gridid'])
                .agg({
                    'rating_num': ['count', 'sum'],    # create multiIndex columns
                    'rating': 'sum'})
                .droplevel(0, axis=1)    # flatten the multiIndex columns built by agg() function
                .reset_index()    # let 'gridid' as a new column instead of the index
            )

            # Update the columns name
            poi_agg_df.columns = [
                'gridid',
                f'{layer_name}POICounts',
                f'{layer_name}POIRatingCountsSum',
                f'{layer_name}POIRatingStarSum',
            ]

            # Merge with original grid, if no data, fill 0.
            poi_merge_df = (pd.merge(self.grid_df[['gridid']],poi_agg_df,on='gridid',how='left')
                    .fillna(0)
                    .astype({f'{layer_name}POICounts':'int',
                             f'{layer_name}POIRatingCountsSum':'int'}))

            return poi_merge_df

        poi_df = pd.DataFrame()

        # Merge all types of POI
        for file_path in poi_file_path_list:
            poi_merge_df = poi_preprocessing(file_path)
            if poi_df.empty:
                poi_df = poi_merge_df
            else:
                poi_df = pd.merge(poi_df, poi_merge_df, on='gridid')

        return poi_df

    # save preprocessed data
    def save_df(self, df):
        df.to_csv(
            f'{self.done_file_path}/DF.csv', index=False
        )
        print('Save the dataframe after preprocessing!')

    # pipeline of preprocess
    def run(self):
        """
        Run preprocessing.
        """
        # create Base df with complete gridID, Date, Hour
        gridid_ = list(
            set(self.grid_df['gridid'])
        )
        hour_ = list(range(24))
        product_elements = list(
            itertools.product(gridid_, self.date_list, hour_)
        )
        keys = ['GridID', 'Date', 'Hour']
        final_df = pd.DataFrame(
            product_elements, columns=keys
        )
        final_df['Weekday'] = pd.to_datetime(
            final_df['Date'], format='%Y%m%d'
        ).dt.weekday
        final_df['IsWeekend'] = final_df['Weekday'].apply(
            lambda x: 1 if x > 4 else 0
        )

        ## transaction
        def transaction_merge(final_df)->pd.DataFrame:
            """
            Merge transaction data with the final dataset.
            Inclouding OnCounts, OffCounts, NetCounts.
            """
            tran_on_df, tran_off_df = self.transaction()
            tran_on_keys = ['gridid', 'on_date', 'on_hour']
            tran_off_keys = ['gridid', 'off_date', 'off_hour']
            tran_on_df = tran_on_df[tran_on_keys+['counts']].groupby(
                tran_on_keys).sum().reset_index()
            tran_off_df = tran_off_df[tran_off_keys+['counts']].groupby(
                tran_off_keys).sum().reset_index()

            # OnCounts
            final_df = final_df.merge(
                tran_on_df, how='left', left_on=keys, right_on=tran_on_keys
            )
            final_df = final_df[['GridID', 'Date', 'Hour', 'IsWeekend', 'counts']]
            final_df.rename(
                columns={'counts': 'OnCounts'}, inplace=True
            )
            tem_df_cols = list(final_df.columns)

            # OffCounts
            final_df = final_df.merge(
                tran_off_df, how='left', left_on=keys, right_on=tran_off_keys
            )
            final_df = final_df[tem_df_cols+['counts']]
            final_df.rename(
                columns={'counts': 'OffCounts'}, inplace=True
            )

            # NetCounts
            final_df.fillna(0, inplace=True)
            final_df['NetCounts'] = final_df['OffCounts'] - final_df['OnCounts']

            return final_df

        ## Population
        def population_merge(final_df)->pd.DataFrame:
            """
            Merge population data with the final dataset.
            Inclouding Age_15_17_Counts, Age_18_21_Counts, ..., Age_Over65_Counts, Age_Total_Counts,
            WorkPopulationCounts, LivePopulationCounts, TourPopulationCounts
            """
            population_df = self.population()
            popu_off_keys = ['網格編號', '日期', '時間']
            final_df = (final_df
                        .merge(population_df, how='left', left_on=keys, right_on=popu_off_keys)
                        .drop(popu_off_keys, axis=1)
                        .fillna(0))
            return final_df

        ## Traffic MRT
        def traffic_mrt_merge(final_df)->pd.DataFrame:
            """
            Merge MRT data with the final dataset.
            Including MRT exits count, MRT population in sum, MRT population out sun
            """
            mrt_exit_counts_df, mrt_population_in_df, mrt_population_out_df = self.traffic_mrt()

            mrt_population_cols = ['gridid', '日期', '時段']

            # MRTExitCounts
            final_df = (final_df
                        .merge(mrt_exit_counts_df, how='left', left_on='GridID', right_on='gridid')
                        .drop('gridid',axis=1)
                        .rename(columns={'counts':'MRTExitCounts'})
                        .fillna(0)
                        .astype({'MRTExitCounts':'int32'}))

            # MRTPopulationInCounts
            final_df = (final_df
                        .merge(mrt_population_in_df, how='left', left_on=keys,
                               right_on=mrt_population_cols)
                        .drop(mrt_population_cols, axis=1)
                        .rename(columns={'人次':'MRTPopulationInCounts'})
                        .fillna(0)
                        .astype({'MRTPopulationInCounts':'int32'}))

            # MRTPopulationOutCounts
            final_df = (final_df
                        .merge(mrt_population_out_df, how='left', left_on=keys,
                               right_on=mrt_population_cols)
                        .drop(mrt_population_cols, axis=1)
                        .rename(columns={'人次':'MRTPopulationOutCounts'})
                        .fillna(0)
                        .astype({'MRTPopulationOutCounts':'int32'}))

            return final_df

        ## The list of the preprocessing tasks
        tasks = [
            ('Transaction', transaction_merge),
            ('Population', population_merge),
            ('traffic MRT', traffic_mrt_merge),
            ('Traffic Bus', self.traffic_bus),
            ('Traffic Bus Routes', self.traffic_bus_routes),
            ('Road Sidewalk', self.road_side_walk),
            ('Road Marked Sidewalk', self.road_marked_side_walk),
            ('Road Bike Route', self.road_bike_route),
            ('Road Tree', self.road_tree),
            ('Road Light', self.road_light),
            ('Road Road Area', self.road_road_area),
            ('Road Road Length', self.road_road_length),
            ('Terrain DTM', self.terrain_dtm),
            ('Terrain NDVI', self.terrain_ndvi),
            ('Land Landuse', self.land_landuse),
            ('Land Building', self.land_building),
            ('POI', self.poi)
        ]


        def merge_with_final_df(final_df, task_func):
            result = task_func()
            final_df = (final_df.merge(result, how='left', left_on='GridID', right_on='gridid')
                    .drop('gridid',axis=1))
            if final_df.isna().any().any():
                final_df = final_df.fillna(0)
                # TODO: 待改進效率
            return final_df

        with tqdm(total=len(tasks), desc='Data Preprocessing') as pbar:
            start_time = datetime.datetime.now()

            for task_name, task_func in tasks:
                start_task_time = datetime.datetime.now()
                print(f'Preprocessing for {task_name} data ...')

                if task_name in ['Transaction', 'Population', 'traffic MRT']:
                    final_df = task_func(final_df)
                else:
                    final_df = merge_with_final_df(final_df, task_func)

                end_task_time = datetime.datetime.now()
                elapsed_time = end_task_time - start_task_time
                pbar.set_description(f'Processing {task_name}')
                pbar.update(1)
                print(f'{task_name} took {elapsed_time} to complete.')

            end_time = datetime.datetime.now()

            total_elapsed_time = end_time - start_time
            print(f'Total time spent: {total_elapsed_time}')
            pbar.update(1)
            pbar.disable = True

        ## save
        self.save_df(final_df)

if __name__ == '__main__':
    p = Preprocess(
        date_list=['20230305', '20230311', '20230317', '20230322']
    )

    START_TIME = str(datetime.datetime.now())
    print(f'Data Preprocessing Start at {START_TIME}!\n')

    p.run()

    END_TIME = str(datetime.datetime.now())
    print(f'\nData Preprocessing Finished at {END_TIME}!')
