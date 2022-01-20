# -*- coding: utf-8 -*-
"""
Main analysis script

@author: akhilesh.koul
"""


#edaimport json
import pandas as pd
import numpy as np
import glob
import seaborn as sns
sns.set_theme()
import json
import matplotlib.pyplot as plt
from tqdm import tqdm
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import sys

import seaborn as sns
sns.set_theme()
import requests
from shapely.geometry import Point
from shapely.ops import cascaded_union
from shapely.ops import unary_union
from matplotlib import cm

class CrashStatAnalysis:
    
    def __init__(self,csv_path,shp_file_path=None):
        """
        Init file for CrashStatAnalysis. Pre-processed files are loaded and merged here 

        Parameters
        ----------
        csv_path : STR
            Path where pre-processed data is stored.
        shp_file_path : STR, optional
            Path where shape file is stored.

        Returns
        -------
        None.

        """
        
        self.csv_path=csv_path
        self.shp_file_path=shp_file_path
        csv_files=glob.glob(self.csv_path)
        print(csv_files)

        self.encoder_label=pd.read_pickle(self.csv_path +'\\'+ 'encoder_label.pkl')
        
        
        print('Loading accident_df')
        accident_df=pd.read_pickle(self.csv_path +'\\'+ 'accident_df_cleaned.pkl')
        # ['ACCIDENT_NO', 'ACCIDENT_DATETIME', 'ACCIDENT_TYPE', 'Accident Type Desc', 'ACCIDENT_TYPE_DESC', 'DAY_OF_WEEK', 'WEEKDAY_NAME', 'DAY_OF_WEEK_DESC', 'DCA_CODE', 'DCA Description', 'DCA_CODE_DESC', 'DIRECTORY', 'EDITION', 'PAGE', 'GRID_REFERENCE_X', 'GRID_REFERENCE_Y', 'LIGHT_CONDITION', 'Light Condition Desc', 'LIGHT_CONDITION_DESC', 'NODE_ID', 'NO_OF_VEHICLES', 'NO_PERSONS', 'NO_PERSONS_INJ_2', 'NO_PERSONS_INJ_3', 'NO_PERSONS_KILLED', 'NO_PERSONS_NOT_INJ', 'POLICE_ATTEND', 'POLICE_ATTEND_DESC', 'ROAD_GEOMETRY', 'Road Geometry Desc', 'ROAD_GEOMETRY_DESC', 'SEVERITY', 'SEVERITY_DESC', 'SPEED_ZONE', 'SPEED_ZONE_DESC']
        accident_df['DATETIME_HOUR']=accident_df['ACCIDENT_DATETIME'].dt.hour
        accident_df['DATETIME_MONTH']=accident_df['ACCIDENT_DATETIME'].dt.month
        accident_df['DATETIME_QUATER']=accident_df['ACCIDENT_DATETIME'].dt.quarter
        accident_df['DATETIME_YEAR']=accident_df['ACCIDENT_DATETIME'].dt.year
        accident_df['DATETIME_WEEK']=accident_df['ACCIDENT_DATETIME'].dt.isocalendar().week
    
        print('Loading accident_location_df')
        accident_location_df=pd.read_pickle( self.csv_path +'\\'+ 'accident_location_df_cleaned.pkl',)
        #['ACCIDENT_NO', 'NODE_ID', 'ROAD_ROUTE_1', 'ROAD_NAME', 'ROAD_TYPE', 'ROAD_NAME_INT', 'ROAD_TYPE_INT', 'DISTANCE_LOCATION', 'DIRECTION_LOCATION','NEAREST_KM_POST', 'OFF_ROAD_LOCATION']
        
        
        print('Loading accident_event_df')
        accident_event_df=pd.read_pickle( self.csv_path +'\\'+ 'accident_event_df_cleaned.pkl') 
        # ['ACCIDENT_NO', 'EVENT_SEQ_NO', 'EVENT_TYPE', 'Event Type Desc', 'VEHICLE_1_ID', 'VEHICLE_1_COLL_PT', 'Vehicle 1 Coll Pt Desc', 'VEHICLE_2_ID', 'VEHICLE_2_COLL_PT', 'Vehicle 2 Coll Pt Desc', 'PERSON_ID', 'OBJECT_TYPE', 'Object Type Desc']
        
        print('Loading atomos_cond_df')
        atomos_cond_df=pd.read_pickle(self.csv_path +'\\'+ 'atomos_cond_df_cleaned.pkl') 
        #['ACCIDENT_NO', 'ATMOSPH_COND', 'Atmosph Cond Desc', 'ATMOSPH_COND_SEQ']
        
        print('Loading node_df')
        node_df=pd.read_pickle( self.csv_path +'\\'+ 'node_df_cleaned.pkl')
        # ['ACCIDENT_NO', 'NODE_ID', 'NODE_TYPE', 'VICGRID94_X', 'VICGRID94_Y', 'LGA_NAME', 'LGA_NAME_ALL', 'REGION_NAME', 'DEG_URBAN_NAME', 'Lat', 'Long', 'POSTCODE_NO']
        
        print('Loading road_cond_df')
        road_cond_df=pd.read_pickle( self.csv_path +'\\'+ 'road_cond_df_cleaned.pkl') 
        #['ACCIDENT_NO', 'SURFACE_COND', 'Surface Cond Desc', 'SURFACE_COND_SEQ']
        
        # accident_chainage_df=pd.read_pickle(self.csv_path +'\\'+ 'accident_chainage_df_cleaned.pkl')
        # ['NODE_ID', 'ROUTE_NO', 'CHAINAGE_SEQ', 'ROUTE_LINK_NO', 'CHAINAGE']

        # node_cmplx_df=pd.read_pickle( self.csv_path +'\\'+ 'node_cmplx_df_cleaned.pkl') 
        
        print('Loading person_df')
        person_df=pd.read_pickle( self.csv_path +'\\'+ 'person_df_cleaned.pkl') 
        #['ACCIDENT_NO', 'PERSON_ID', 'VEHICLE_ID', 'SEX', 'AGE', 'AGE_GROUP', 'INJ_LEVEL', 'Inj Level Desc', 'SEATING_POSITION', 'HELMET_BELT_WORN', 'ROAD_USER_TYPE', 'Road User Type Desc', 'LICENCE_STATE', 'PEDEST_MOVEMENT', 'POSTCODE', 'TAKEN_HOSPITAL', 'EJECTED_CODE']
        
        # subdca_df=pd.read_pickle( self.csv_path +'\\'+ 'subdca_df_cleaned.pkl') 
        print('Loading vehicle_df')
        vehicle_df=pd.read_pickle(self.csv_path +'\\'+ 'vehicle_df_cleaned.pkl')
        # ['ACCIDENT_NO', 'VEHICLE_ID', 'VEHICLE_YEAR_MANUF', 'VEHICLE_DCA_CODE',  'INITIAL_DIRECTION', 'INITIAL_DIRECTION_DESC', 'ROAD_SURFACE_TYPE', 'Road Surface Type Desc', 'ROAD_SURFACE_TYPE_DESC', 'REG_STATE', 'REG_STATE_DESC', 'VEHICLE_BODY_STYLE', 'VEHICLE_MAKE', 'VEHICLE_MODEL', 'VEHICLE_POWER', 'VEHICLE_TYPE', 'Vehicle Type Desc', 'VEHICLE_TYPE_DESC',  'VEHICLE_WEIGHT', 'CONSTRUCTION_TYPE', 'CONSTRUCTION_TYPE_DESC', 'FUEL_TYPE', 'FUEL_TYPE_DESC', 'NO_OF_WHEELS', 'NO_OF_CYLINDERS', 'SEATING_CAPACITY', 'TARE_WEIGHT', 'TOTAL_NO_OCCUPANTS', 'CARRY_CAPACITY', 'CUBIC_CAPACITY',  'FINAL_DIRECTION', 'DRIVER_INTENT', 'VEHICLE_MOVEMENT', 'VEHICLE_MOVEMENT_DESC', 'TRAILER_TYPE', 'TRAILER_TYPE_DESC', 'VEHICLE_COLOUR_1', 'VEHICLE_COLOUR_1_DESC', 'VEHICLE_COLOUR_2', 'VEHICLE_COLOUR_2_DESC', 'CAUGHT_FIRE', 'CAUGHT_FIRE_DESC',  'INITIAL_IMPACT', 'INITIAL_IMPACT_DESC', 'LAMPS', 'LAMPS_DESC', 'LEVEL_OF_DAMAGE', 'LEVEL_OF_DAMAGE_DESC', 'OWNER_POSTCODE', 'TOWED_AWAY_FLAG',  'TOWED_AWAY_FLAG_DESC', 'TRAFFIC_CONTROL', 'Traffic Control Desc',  'TRAFFIC_CONTROL_DESC'] 
        
        print('Merging accident_info_df')
        #accident_df
        accident_info_df=accident_df.copy()
        
        #accident_location_df
        accident_info_df=accident_info_df.merge(accident_location_df,how='left',on='ACCIDENT_NO')
        
        #accident_event_df
        # accident_event_df_group=accident_event_df.groupby('ACCIDENT_NO').agg(lambda col: ','.join(col))
        accident_event_df_group=accident_event_df.groupby('ACCIDENT_NO').first()
        accident_event_df_group['ACCIDENT_NO']=accident_event_df_group.index
        accident_event_df_group.reset_index(drop=True,inplace=True)
        accident_info_df=accident_info_df.merge(accident_event_df_group,how='left',on='ACCIDENT_NO')
        
        #atomos_cond_df
        atomos_cond_df_group=atomos_cond_df.groupby('ACCIDENT_NO').agg(lambda col: ','.join(col))
        # atomos_cond_df_group=atomos_cond_df.groupby('ACCIDENT_NO').last()
        atomos_cond_df_group['ACCIDENT_NO']=atomos_cond_df_group.index
        atomos_cond_df_group.reset_index(drop=True,inplace=True)
        accident_info_df=accident_info_df.merge(atomos_cond_df_group,how='left',on='ACCIDENT_NO')
        
        # node_df
        accident_info_df=accident_info_df.merge(node_df,how='left',on='ACCIDENT_NO')
        #['ACCIDENT_NO', 'NODE_ID', 'NODE_TYPE', 'VICGRID94_X', 'VICGRID94_Y', 'LGA_NAME', 'LGA_NAME_ALL', 'REGION_NAME', 'DEG_URBAN_NAME', 'Lat', 'Long', 'POSTCODE_NO']
        
        #road_cond_df
        road_cond_df_group=road_cond_df.groupby('ACCIDENT_NO').agg(lambda col: ','.join(col))
        # road_cond_df_group=road_cond_df.groupby('ACCIDENT_NO').last()
        road_cond_df_group['ACCIDENT_NO']=road_cond_df_group.index
        road_cond_df_group.reset_index(drop=True,inplace=True)
        accident_info_df=accident_info_df.merge(road_cond_df_group,how='left',on='ACCIDENT_NO')
        self.accident_info_df=accident_info_df
        #['ACCIDENT_NO', 'ACCIDENT_DATETIME', 'ACCIDENT_TYPE', 'Accident Type Desc', 'ACCIDENT_TYPE_DESC', 'DAY_OF_WEEK', 'WEEKDAY_NAME', 'DAY_OF_WEEK_DESC', 'DCA_CODE', 'DCA Description', 'DCA_CODE_DESC', 'DIRECTORY', 'EDITION', 'PAGE', 'GRID_REFERENCE_X', 'GRID_REFERENCE_Y', 'LIGHT_CONDITION', 'Light Condition Desc', 'LIGHT_CONDITION_DESC', 'NODE_ID_x', 'NO_OF_VEHICLES', 'NO_PERSONS', 'NO_PERSONS_INJ_2', 'NO_PERSONS_INJ_3', 'NO_PERSONS_KILLED', 'NO_PERSONS_NOT_INJ', 'POLICE_ATTEND', 'POLICE_ATTEND_DESC', 'ROAD_GEOMETRY', 'Road Geometry Desc', 'ROAD_GEOMETRY_DESC', 'SEVERITY', 'SEVERITY_DESC', 'SPEED_ZONE', 'SPEED_ZONE_DESC', 'DATETIME_HOUR', 'DATETIME_MONTH', 'DATETIME_QUATER', 'DATETIME_YEAR', 'NODE_ID_y', 'ROAD_ROUTE_1', 'ROAD_ROUTE_1_DESC', 'ROAD_NAME', 'ROAD_TYPE', 'ROAD_NAME_INT', 'ROAD_TYPE_INT', 'DISTANCE_LOCATION', 'DIRECTION_LOCATION', 'NEAREST_KM_POST', 'OFF_ROAD_LOCATION', 'EVENT_SEQ_NO', 'EVENT_TYPE', 'Event Type Desc', 'EVENT_TYPE_DESC', 'VEHICLE_1_ID', 'VEHICLE_1_COLL_PT', 'Vehicle 1 Coll Pt Desc', 'VEHICLE_1_COLL_PT_DESC', 'VEHICLE_2_ID', 'VEHICLE_2_COLL_PT', 'Vehicle 2 Coll Pt Desc', 'VEHICLE_2_COLL_PT_DESC', 'PERSON_ID', 'OBJECT_TYPE', 'Object Type Desc', 'OBJECT_TYPE_DESC', 'ATMOSPH_COND', 'Atmosph Cond Desc', 'ATMOSPH_COND_DESC', 'ATMOSPH_COND_SEQ', 'NODE_ID', 'NODE_TYPE', 'VICGRID94_X', 'VICGRID94_Y', 'LGA_NAME', 'LGA_NAME_ALL', 'REGION_NAME', 'DEG_URBAN_NAME', 'Lat', 'Long', 'POSTCODE_NO', 'SURFACE_COND', 'Surface Cond Desc', 'SURFACE_COND_DESC', 'SURFACE_COND_SEQ']
        
        print('Merging person_info_df')
        person_info_df=person_df.copy()
        person_info_df=person_info_df.merge(accident_df,how='left',on='ACCIDENT_NO')
        self.person_info_df=person_info_df
        # print(list(self.person_info_df))
        # ['ACCIDENT_NO', 'PERSON_ID', 'VEHICLE_ID', 'SEX', 'SEX_DESC', 'AGE', 'AGE_GROUP', 'INJ_LEVEL', 'Inj Level Desc', 'INJ_LEVEL_DESC', 'SEATING_POSITION', 'SEATING_POSITION_DESC', 'HELMET_BELT_WORN', 'HELMET_BELT_WORN_DESC', 'ROAD_USER_TYPE', 'Road User Type Desc', 'ROAD_USER_TYPE_DESC', 'LICENCE_STATE', 'LICENCE_STATE_DESC', 'PEDEST_MOVEMENT', 'PEDEST_MOVEMENT_DESC', 'POSTCODE', 'TAKEN_HOSPITAL', 'EJECTED_CODE', 'EJECTED_CODE_DESC', 'ACCIDENT_DATETIME', 'ACCIDENT_TYPE', 'Accident Type Desc', 'ACCIDENT_TYPE_DESC', 'DAY_OF_WEEK', 'WEEKDAY_NAME', 'DAY_OF_WEEK_DESC', 'DCA_CODE', 'DCA Description', 'DCA_CODE_DESC', 'DIRECTORY', 'EDITION', 'PAGE', 'GRID_REFERENCE_X', 'GRID_REFERENCE_Y', 'LIGHT_CONDITION', 'Light Condition Desc', 'LIGHT_CONDITION_DESC', 'NODE_ID', 'NO_OF_VEHICLES', 'NO_PERSONS', 'NO_PERSONS_INJ_2', 'NO_PERSONS_INJ_3', 'NO_PERSONS_KILLED', 'NO_PERSONS_NOT_INJ', 'POLICE_ATTEND', 'POLICE_ATTEND_DESC', 'ROAD_GEOMETRY', 'Road Geometry Desc', 'ROAD_GEOMETRY_DESC', 'SEVERITY', 'SEVERITY_DESC', 'SPEED_ZONE', 'SPEED_ZONE_DESC', 'DATETIME_HOUR', 'DATETIME_MONTH', 'DATETIME_QUATER', 'DATETIME_YEAR']
       
        print('Merging vehicle_info_df')
        vehicle_info_df=vehicle_df.copy()
        vehicle_info_df=vehicle_info_df.merge(accident_df,how='left',on='ACCIDENT_NO')
        self.vehicle_info_df=vehicle_info_df
        # print(list(self.vehicle_info_df))
        # ['ACCIDENT_NO', 'VEHICLE_ID', 'VEHICLE_YEAR_MANUF', 'VEHICLE_DCA_CODE', 'INITIAL_DIRECTION', 'INITIAL_DIRECTION_DESC', 'ROAD_SURFACE_TYPE', 'Road Surface Type Desc', 'ROAD_SURFACE_TYPE_DESC', 'REG_STATE', 'REG_STATE_DESC', 'VEHICLE_BODY_STYLE', 'VEHICLE_MAKE', 'VEHICLE_MODEL', 'VEHICLE_POWER', 'VEHICLE_TYPE', 'Vehicle Type Desc', 'VEHICLE_TYPE_DESC', 'VEHICLE_WEIGHT', 'CONSTRUCTION_TYPE', 'CONSTRUCTION_TYPE_DESC', 'FUEL_TYPE', 'FUEL_TYPE_DESC', 'NO_OF_WHEELS', 'NO_OF_CYLINDERS', 'SEATING_CAPACITY', 'TARE_WEIGHT', 'TOTAL_NO_OCCUPANTS', 'CARRY_CAPACITY', 'CUBIC_CAPACITY', 'FINAL_DIRECTION', 'DRIVER_INTENT', 'VEHICLE_MOVEMENT', 'VEHICLE_MOVEMENT_DESC', 'TRAILER_TYPE', 'TRAILER_TYPE_DESC', 'VEHICLE_COLOUR_1', 'VEHICLE_COLOUR_1_DESC', 'VEHICLE_COLOUR_2', 'VEHICLE_COLOUR_2_DESC', 'CAUGHT_FIRE', 'CAUGHT_FIRE_DESC', 'INITIAL_IMPACT', 'INITIAL_IMPACT_DESC', 'LAMPS', 'LAMPS_DESC', 'LEVEL_OF_DAMAGE', 'LEVEL_OF_DAMAGE_DESC', 'OWNER_POSTCODE', 'TOWED_AWAY_FLAG', 'TOWED_AWAY_FLAG_DESC', 'TRAFFIC_CONTROL', 'Traffic Control Desc', 'TRAFFIC_CONTROL_DESC', 'ACCIDENT_DATETIME', 'ACCIDENT_TYPE', 'Accident Type Desc', 'ACCIDENT_TYPE_DESC', 'DAY_OF_WEEK', 'WEEKDAY_NAME', 'DAY_OF_WEEK_DESC', 'DCA_CODE', 'DCA Description', 'DCA_CODE_DESC', 'DIRECTORY', 'EDITION', 'PAGE', 'GRID_REFERENCE_X', 'GRID_REFERENCE_Y', 'LIGHT_CONDITION', 'Light Condition Desc', 'LIGHT_CONDITION_DESC', 'NODE_ID', 'NO_OF_VEHICLES', 'NO_PERSONS', 'NO_PERSONS_INJ_2', 'NO_PERSONS_INJ_3', 'NO_PERSONS_KILLED', 'NO_PERSONS_NOT_INJ', 'POLICE_ATTEND', 'POLICE_ATTEND_DESC', 'ROAD_GEOMETRY', 'Road Geometry Desc', 'ROAD_GEOMETRY_DESC', 'SEVERITY', 'SEVERITY_DESC', 'SPEED_ZONE', 'SPEED_ZONE_DESC', 'DATETIME_HOUR', 'DATETIME_MONTH', 'DATETIME_QUATER', 'DATETIME_YEAR']

        

    def eda_count_plot(self,df,field_name,date_range=None,hue=None,filter_list=None,order=False):
        """
        Function to perform EDA and plot count with hue information 

        Parameters
        ----------
        df : PANDAS DATAFRAME
            Pandas DataFrame for which analysis is to be done.
        field_name : STR
            Field name to obtain count for.
        date_range : NUMPY ARRAY, optional
            numpy array with data range e.g. ['2019-01-01','2019-12-01']
        hue : STR, optional
            Field name to plot in addition to the field name.
        filter_list : LIST, optional
            List with key,value pair if filter needed to be done. e.g. ([['SEVERITY','Fatal accident'],['ROAD_GEOMETRY','Cross intersection']])
         order : BOOL, optional
            Plot count in decensing order or not. The default is False.
            

        Returns
        -------
        None.

        """
        
        if date_range != None:
            df = df[(df['ACCIDENT_DATETIME']> date_range[0])& (df['ACCIDENT_DATETIME']< date_range[1])]
        
        if filter_list!=None:
            for i in range(len(filter_list)):
                
                if filter_list[i][0] in list(self.encoder_label['FIELD_NAME']):
                    df=df[(df[filter_list[i][0]+'_DESC']==filter_list[i][1])]
                else:
                    df=df[(df[filter_list[i][0]]==filter_list[i][1])]
                    
        df_subset=pd.DataFrame()
        df_subset[field_name]=df[field_name]
       
        if hue != None :
            if hue in list(self.encoder_label['FIELD_NAME']):
                hue_desc=hue+'_DESC'
                df_subset[hue]=df[hue_desc]
            else:
                df_subset[hue]=df[hue]
        if field_name in list(self.encoder_label['FIELD_NAME']):
            field_name_desc=field_name+'_DESC'
            df_subset[field_name_desc]=df[field_name_desc]
     
            # fig, ax = plt.subplots(figsize=(15,8))
            fig, ax = plt.subplots()
    
            if order == False:
                sns.countplot(x=df_subset[field_name_desc],                          hue=hue, data=df_subset,ax=ax)
                
            
            elif order == True:
                 sns.countplot(x=df_subset[field_name_desc],
                            order=df_subset[field_name_desc].value_counts(ascending=False).index, hue=hue,data=df_subset,ax=ax)
                 
        else:
            
            
            # fig, ax = plt.subplots(figsize=(15,8))
            fig, ax = plt.subplots()
            if order == False:
                sns.countplot(x=df_subset[field_name],                          hue=hue, data=df_subset,ax=ax)
            
            elif order == True:
                 sns.countplot(x=df_subset[field_name],
                            order=df_subset[field_name].value_counts(ascending=False).index, hue=hue,data=df_subset,ax=ax)
        
        plt.xticks(rotation=90)
        if hue != None :
            plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
        
        total = float(len(df_subset))
        for p in ax.patches:
            percentage = '{:.1f}%'.format(100 * p.get_height()/total)
            x = p.get_x() + p.get_width()
            y = p.get_height()
            ax.annotate(percentage, (x, y),ha='right',rotation=90)
         
        plt.show()
        
    def subquery(self,df,query_flow,date_range=None,top=None,filter_list=None):
        """
        Function to perform subquery and obtain stat.

        Parameters
        ----------
        df : PANDAS DATAFRAME
            Pandas DataFrame for which analysis is to be done.
        query_flow : LIST
            List of query to performed in the written order.
        date_range : NUMPY ARRAY, optional
            numpy array with data range e.g. ['2019-01-01','2019-12-01']
        top : INT, optional
            Used to filter the top value in grouped dataframe
        filter_list : LIST, optional
            List with key,value pair if filter needed to be done. e.g. ([['SEVERITY','Fatal accident'],['ROAD_GEOMETRY','Cross intersection']])
        Returns
        -------
        grouped_df : PANDAS DATAFRAME
            Stat based on the query.

        """
        
        if date_range != None:
            df = df[(df['ACCIDENT_DATETIME']> date_range[0])& (df['ACCIDENT_DATETIME']< date_range[1])]
        
        
        query_flow_updated=[]
        
        for i in range(len(query_flow)):
            if query_flow[i] in list(self.encoder_label['FIELD_NAME']):
                query_flow_updated.append(query_flow[i]+'_DESC')
            else:
                query_flow_updated.append(query_flow[i])
                
        
        if filter_list!=None:
            for i in range(len(filter_list)):
                if filter_list[i][0] in list(self.encoder_label['FIELD_NAME']):
                    df=df[(df[filter_list[i][0]+'_DESC']==filter_list[i][1])]
                else:
                    df=df[(df[filter_list[i][0]]==filter_list[i][1])]
        
        
        if top !=None:
            grouped_df = df.groupby(query_flow_updated).size().nlargest(top)
            grouped_df.sort_index(inplace=True)
        else :
            grouped_df = df.groupby(query_flow_updated).size()
        return  grouped_df  
    
    def plot_map(self,field_name=None):
        """
        Function to plot geospatial data using Lat/Long

        Parameters
        ----------
        field_name : STR, optional
            Field name for which geospatial is to obtained.


        Returns
        -------
        None.

        """
        
        if field_name in list(self.encoder_label['FIELD_NAME']):
            field_name = field_name + '_DESC'
            # print(field_name)
            
  
        vic_lga = gpd.read_file(self.shp_file_path)


        
        
        geo_df=gpd.GeoDataFrame()
        geometry=[Point(xy) for xy in zip(self.accident_info_df['Long'],self.accident_info_df['Lat'])]
        geo_df['geometry']=geometry
        geo_df[field_name]=self.accident_info_df[field_name]
        geo_df[geo_df['geometry'].is_valid]
    
        # print(geo_df)
        
        
        fig, ax = plt.subplots(figsize=(12, 8))
        vic_lga.boundary.plot(ax=ax,color='gray',edgecolor='black')
        geo_df.plot(ax=ax,column=field_name,legend=True,markersize=5,alpha=0.7)
        ax.set_axis_off()
        plt.title('VIC Local Government Areas - Geoscape Administrative Boundaries\n'+field_name)
        plt.show()
            
    
    
    
    def xgb_classifier_train(self,X_col, y_col):
        pass;
        return 1
        
    
            

if __name__ == '__main__':
    
    #initilization
    CrashStatAnalysis=CrashStatAnalysis(csv_path='data\\ACCIDENT_CLEANED',shp_file_path='data/SHP_FILE/nov21_vic_lga_polygon_shp/vic_lga.shp')
    
    
    
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SEVERITY',order=False,date_range=['2009-01-01','2020-01-01'])
   
   
   
    
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SEVERITY',hue='ROAD_GEOMETRY',order=False,date_range=['2009-01-01','2020-01-01'])
    
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SEVERITY',hue='ATMOSPH_COND',order=False,date_range=['2009-01-01','2020-01-01'])
    
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SEVERITY',hue='ATMOSPH_COND',filter_list=([['SEVERITY','Fatal accident']]),order=False,date_range=['2009-01-01','2020-01-01'])
    
    
    #eda count
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SEVERITY')
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SEVERITY',filter_list=([['SEVERITY','Fatal accident'],['ROAD_GEOMETRY','Cross intersection']]))
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SEVERITY',filter_list=([['DIRECTORY','MEL'],['ROAD_GEOMETRY','Cross intersection']]))
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SEVERITY',date_range=['2019-01-01','2019-12-01'])
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SEVERITY',date_range=['2019-01-01','2019-12-01'],hue='DAY_OF_WEEK')


    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='EVENT_TYPE')
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='OBJECT_TYPE')
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='OBJECT_TYPE',hue='EVENT_TYPE')



    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='ROAD_ROUTE_1')
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='ATMOSPH_COND')



    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='NODE_TYPE')
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.accident_info_df,field_name='SURFACE_COND')


    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.person_info_df,field_name='HELMET_BELT_WORN',date_range=['2019-01-01','2019-12-01'])
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.person_info_df,field_name='HELMET_BELT_WORN',hue='SEVERITY',date_range=['2019-01-01','2019-12-01'])
        
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.vehicle_info_df,field_name='ROAD_SURFACE_TYPE',date_range=['2019-01-01','2019-12-01'])
    CrashStatAnalysis.eda_count_plot(df=CrashStatAnalysis.vehicle_info_df,field_name='ROAD_SURFACE_TYPE',hue='SEVERITY',date_range=['2019-01-01','2019-12-01'])
    

     
    #subquery
    subquery_data=CrashStatAnalysis.subquery(df=CrashStatAnalysis.accident_info_df, query_flow=['SEVERITY','EVENT_TYPE'],top=10,filter_list=([['SEVERITY','Fatal accident'],['ROAD_GEOMETRY','Cross intersection']]))
    print(subquery_data)


    subquery_data=CrashStatAnalysis.subquery(df=CrashStatAnalysis.accident_info_df,query_flow=['SEVERITY','SPEED_ZONE'], date_range=['2019-01-01','2019-12-01'],top=10)
    print(subquery_data)

    subquery_data=CrashStatAnalysis.subquery(df=CrashStatAnalysis.accident_info_df,                 query_flow=['SEVERITY','SPEED_ZONE'])
    print(subquery_data)
    
    subquery_data=CrashStatAnalysis.subquery(df=CrashStatAnalysis.accident_info_df,                 query_flow=['SEVERITY','SPEED_ZONE'],date_range=['2009-01-01','2020-01-01'],top=10)
    print(subquery_data)

    subquery_data=CrashStatAnalysis.subquery(df=CrashStatAnalysis.vehicle_info_df,                   query_flow=['SEVERITY','ROAD_SURFACE_TYPE','SPEED_ZONE'],date_range=['2019-01-01','2019-12-01'], top=10)
    print(subquery_data)
    
    
    subquery_data=CrashStatAnalysis.subquery(df=CrashStatAnalysis.person_info_df,                   query_flow=['INJ_LEVEL','SEX','AGE_GROUP'],date_range=['2019-01-01','2019-12-01'], top=10)
    print(subquery_data)
    
    subquery_data=CrashStatAnalysis.subquery(df=CrashStatAnalysis.accident_info_df, query_flow=['SEVERITY','EVENT_TYPE'],top=10,filter_list=([['SEVERITY','Fatal accident'],['ROAD_GEOMETRY','Cross intersection']]),date_range=['2009-01-01','2020-01-01'])
    print(subquery_data)
    
   

    #plot_map
    CrashStatAnalysis.plot_map(field_name='SEVERITY')
    CrashStatAnalysis.plot_map(field_name='NO_PERSONS_KILLED')
    CrashStatAnalysis.plot_map(field_name='POLICE_ATTEND')
    CrashStatAnalysis.plot_map(field_name='LIGHT_CONDITION')
    CrashStatAnalysis.plot_map(field_name='DATETIME_HOUR')
    CrashStatAnalysis.plot_map(field_name='DATETIME_MONTH')
    CrashStatAnalysis.plot_map(field_name='ATMOSPH_COND')
    CrashStatAnalysis.plot_map(field_name='SURFACE_COND')
  
   
    
 
 








