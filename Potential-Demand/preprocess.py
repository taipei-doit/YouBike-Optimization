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
import itertools
import json
import os
import pickle
import warnings
from ast import literal_eval

import geopandas as gpd
import pandas as pd
import pykml.parser
from shapely.geometry import LineString, Point, Polygon
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
        self.done_file_path = 'output_ON' if self.target == 'on' else 'output_OFF'
        # pylint: enable=C0103

        # grid information
        self.grid_df = gpd.read_file(
            f'{self.population_file_path}/FET_2023_grid_97.geojson'
        ).set_crs('epsg:3826')
        self.grid_df['Area'] = self.grid_df.geometry.area

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
