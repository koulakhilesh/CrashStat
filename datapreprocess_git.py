# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 13:28:12 2022

@author: akhilesh.koul
"""
import glob
import pandas as pd
import numpy as np
import json
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm

class CrashStatPreData:
    
    def __init__(self,csv_path,save_path):
        """
        init function for crash stat data preparation

        Parameters
        ----------
        csv_path : STR
            Path where orginal file are stored.
        save_path : STR
            Path where pre-processed data is stored.

        Returns
        -------
        None.

        """
        
        self.csv_path=csv_path
        # print(self.csv_path)
        csv_files=glob.glob(self.csv_path+'\*')
        print(csv_files)
        self.encoder_label=pd.DataFrame()
        self.encoder_label['FIELD_NAME']=""
        self.encoder_label['CODE_DESCRIPTION']=""
        self.save_path=save_path
        print('Data Clean Folder = '+ self.save_path)
    
    
    def label_decoder(self, field_name,code):
        """
        Function to use label_decoder for field_name and their code

        Parameters
        ----------
        field_name : STR
            field_name such as "SPEED_ZONE".
        code : STR
            Code for field_name such as SPEED_ZONE_DESC as per the metadata.

        Returns
        -------
        decode : STR
            Code for the field_name such as '40 km/h' for '40'.

        """
        subset=self.encoder_label[self.encoder_label['FIELD_NAME'] == field_name]
        code_dict = json.loads(subset['CODE_DESCRIPTION'].values.item())
        for key,value in code_dict.items():
            # print(key,value)
            if code == key:
                decode=value
        return decode  

    def label_encoder(self,field_name,desc):
        """
        Function to use label_encoder for field_name and their description


        Parameters
        ----------
        field_name : STR
            field_name such as "SPEED_ZONE".
        desc : STR
            Description for field_name such as SPEED_ZONE_DESC as per the metadata.

        Returns
        -------
        code : STR
            Code for the field_name such as '40' for '40 km/h'.

        """
        subset=self.encoder_label[self.encoder_label['FIELD_NAME'] == field_name]
        code_dict = json.loads(subset['CODE_DESCRIPTION'].values.item())
        for key,value in code_dict.items():
            # print(key,value)
            if desc == value:
                code=key
        return code      


    def cleanAccidentFiles(self):
        """
        Main file to clean and preprocess crash stat files

        Returns
        -------
        None.

        """
        
        #1
        print('')
        # self.csv_path='data\ACCIDENT'
        single_csv='ACCIDENT.csv'
        print('Cleaning '+single_csv)
        accident_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        accident_df_cleaned=pd.DataFrame()
        # tmp=accident_df[0:5]
        # print(tmp)
        accident_df=accident_df.convert_dtypes()
        
        #col_list= ['ACCIDENT_NO','ACCIDENTDATE','ACCIDENTTIME',
        #'ACCIDENT_TYPE','Accident Type Desc'
        #'DAY_OF_WEEK','Day Week Description',
        # 'DCA_CODE', 'DCA Description',
        #'DIRECTORY', 'EDITION', 'PAGE','GRID_REFERENCE_X', 'GRID_REFERENCE_Y',
        #'LIGHT_CONDITION', 'Light Condition Desc', 'NODE_ID', 'NO_OF_VEHICLES',
        # 'NO_PERSONS', 'NO_PERSONS_INJ_2', 'NO_PERSONS_INJ_3', 'NO_PERSONS_KILLED',
        # 'NO_PERSONS_NOT_INJ', 'POLICE_ATTEND', 'ROAD_GEOMETRY', 'Road Geometry Desc',
        # 'SEVERITY', 'SPEED_ZONE']
        
        #clean the column wise
        accident_df_cleaned['ACCIDENT_NO']=accident_df['ACCIDENT_NO']
        accident_df_cleaned['ACCIDENT_DATETIME']=pd.to_datetime(accident_df['ACCIDENTDATE']) + pd.to_timedelta(accident_df['ACCIDENTTIME'])
        
        
        field_name="ACCIDENT_TYPE"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        accident_df_cleaned[field_name].describe()
        accident_df_cleaned[field_name].value_counts()
        # ax = sns.countplot(x=field_name, data=accident_df)
        # plt.show()
        
        field_name="Accident Type Desc"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        accident_df_cleaned[field_name].describe()
        accident_df_cleaned[field_name].value_counts()
        
        grouped_df = accident_df_cleaned.groupby(["ACCIDENT_TYPE", "Accident Type Desc"]).size()
        code=grouped_df.index.get_level_values('ACCIDENT_TYPE').tolist()
        description=grouped_df.index.get_level_values('Accident Type Desc').tolist()
        field_name="ACCIDENT_TYPE"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_df_cleaned[field_name])]
       
        # for cod in code :
        #     print(self.label_decoder(field_name,cod))
            
    
        field_name="DAY_OF_WEEK" 
        accident_df_cleaned[field_name]=accident_df_cleaned['ACCIDENT_DATETIME'].dt.weekday.astype(str)
        accident_df_cleaned[field_name].describe()
        accident_df_cleaned[field_name].value_counts()
        # ax = sns.countplot(x=field_name, data=accident_df_cleaned)
        # plt.show()
        
        
        field_name="WEEKDAY_NAME" 
        accident_df_cleaned[field_name]=accident_df_cleaned['ACCIDENT_DATETIME'].dt.day_name().astype(str)
        accident_df_cleaned[field_name].describe()
        accident_df_cleaned[field_name].value_counts()
        
        grouped_df = accident_df_cleaned.groupby(["DAY_OF_WEEK", "WEEKDAY_NAME"]).size()
        code=grouped_df.index.get_level_values('DAY_OF_WEEK').tolist()
        description=grouped_df.index.get_level_values('WEEKDAY_NAME').tolist()
        field_name="DAY_OF_WEEK" 
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_df_cleaned[field_name])]
       
        # for cod in code :
        #     print(self.label_decoder(field_name,cod))
     
        
        field_name="DCA_CODE"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        accident_df_cleaned[field_name].describe()
        accident_df_cleaned[field_name].value_counts()
        # ax = sns.countplot(x=field_name, data=accident_df_cleaned)
        # plt.show()
        
        field_name="DCA Description"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str).str.strip()
        # accident_df_cleaned[field_name].describe()
        # accident_df_cleaned[field_name].value_counts()
        
        grouped_df = accident_df_cleaned.groupby(["DCA_CODE", "DCA Description"]).size()
        code=grouped_df.index.get_level_values('DCA_CODE').tolist()
        description=grouped_df.index.get_level_values('DCA Description').tolist()
        
        field_name="DCA_CODE"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_df_cleaned[field_name])]
       
        # for cod in code :
        #     print(self.label_decoder(field_name,cod))
        
    
        
        
        field_name="DIRECTORY"
        accident_df_cleaned[field_name]= accident_df[field_name].astype(str)
        accident_df_cleaned[field_name] = accident_df_cleaned[field_name].replace(' ', '<NA>')
        # grouped_df = accident_df_cleaned.groupby([field_name]).size()
        accident_df_cleaned[field_name].describe()
        accident_df_cleaned[field_name].value_counts()
        
        
        
       
        field_name="EDITION"
        accident_df_cleaned[field_name]= accident_df[field_name]
        accident_df_cleaned[field_name] = accident_df_cleaned[field_name].replace(' ', '<NA>')
        accident_df_cleaned[field_name] = accident_df_cleaned[field_name].replace('nan', '<NA>')
       
    
        accident_df_cleaned.loc[accident_df_cleaned[field_name]=="ED35",field_name]=35
        mask = pd.to_numeric(accident_df_cleaned[field_name], errors='coerce').notnull()
        accident_df_cleaned[field_name].loc[mask]=((accident_df_cleaned[field_name].loc[mask]).astype(int).round()).astype(str)
         # grouped_df = accident_df_cleaned.groupby([field_name]).size()
      
        
        field_name="PAGE"
        accident_df_cleaned[field_name]= accident_df[field_name].astype(str)
        accident_df_cleaned[field_name] = accident_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df = accident_df_cleaned.groupby([field_name]).size()
        
        
        field_name="GRID_REFERENCE_X"
        accident_df_cleaned[field_name]= accident_df[field_name].astype(str)
        accident_df_cleaned[field_name] = accident_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df = accident_df_cleaned.groupby([field_name]).size()
       
        
        field_name="GRID_REFERENCE_Y"
        accident_df_cleaned[field_name]= accident_df[field_name].astype(str)
        accident_df_cleaned[field_name] = accident_df_cleaned[field_name].replace(' ', '<NA>')
        accident_df_cleaned[field_name] = accident_df_cleaned[field_name].replace('nan', '<NA>')
        mask = pd.to_numeric(accident_df_cleaned[field_name], errors='coerce').notnull()
        accident_df_cleaned[field_name].loc[mask]=((pd.to_numeric(accident_df_cleaned[field_name].loc[mask], errors='coerce')).astype(int).round()).astype(str)
        accident_df_cleaned[field_name]=accident_df_cleaned[field_name].astype(str)
        grouped_df = accident_df_cleaned.groupby([field_name]).size()
        
        
        field_name="LIGHT_CONDITION"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        accident_df_cleaned.groupby([field_name]).size()
        
        field_name="Light Condition Desc"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        accident_df_cleaned.groupby([field_name]).size()
        
        grouped_df = accident_df_cleaned.groupby(["LIGHT_CONDITION", "Light Condition Desc"]).size()
        code=grouped_df.index.get_level_values('LIGHT_CONDITION').tolist()
        description=grouped_df.index.get_level_values('Light Condition Desc').tolist()
        
        field_name="LIGHT_CONDITION"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_df_cleaned[field_name])]
       
        # for cod in code :
        #     print(self.label_decoder(field_name,cod))
        
        
    
        field_name="NODE_ID"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
        
        
        field_name="NO_OF_VEHICLES"
        accident_df_cleaned[field_name]= (accident_df[field_name])
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
 
    
        field_name="NO_PERSONS"
        accident_df_cleaned[field_name]= (accident_df[field_name])
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
 
      
        field_name="NO_PERSONS_INJ_2"
        accident_df_cleaned[field_name]= (accident_df[field_name])
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
 
   
        field_name="NO_PERSONS_INJ_3"
        accident_df_cleaned[field_name]= (accident_df[field_name])
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
      
        
        field_name="NO_PERSONS_KILLED"
        accident_df_cleaned[field_name]= (accident_df[field_name])
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
      
        
        field_name="NO_PERSONS_NOT_INJ"
        accident_df_cleaned[field_name]= (accident_df[field_name])
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
      
       
        
        
        field_name="POLICE_ATTEND"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        description=['Yes','No','Not known']
        # print(code,description)
        
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        # for cod in code :
        #     print(self.label_decoder(field_name,cod))
        accident_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_df_cleaned[field_name])]
       
       
         
               
        field_name="ROAD_GEOMETRY"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
        
        field_name="Road Geometry Desc"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
        
        grouped_df = accident_df_cleaned.groupby(["ROAD_GEOMETRY", "Road Geometry Desc"]).size()
        code=grouped_df.index.get_level_values('ROAD_GEOMETRY').tolist()
        description=grouped_df.index.get_level_values('Road Geometry Desc').tolist()
        
        field_name="ROAD_GEOMETRY"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_df_cleaned[field_name])]
       
        # for cod in code :
        #     print(self.label_decoder(field_name,cod))
        
      
        
        
        
        field_name="SEVERITY"
        accident_df_cleaned[field_name]= (accident_df[field_name]).astype(str)
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        description=['Fatal accident','Serious injury accident','Other injury accident','Non injury accident']
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_df_cleaned[field_name])]
       
       
        
        field_name="SPEED_ZONE"
        accident_df_cleaned[field_name]= (accident_df[field_name])
        grouped_df=accident_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        description=[ '30 km/hr',
                     '40 km/hr',
                     '50 km/hr',
                     '60 km/hr',
                     '70 km/hr',
                     '75 km/hr',
                     '80 km/hr',
                     '90 km/hr',
                     '100 km/hr',
                     '110 km/hr',
                     'Other speed limit',
                     'Camping grounds, off road',
                     'Not known']
        
        accident_df_cleaned[field_name]= (accident_df_cleaned[field_name]).astype(str)
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_df_cleaned[field_name])]
       
        
        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
       
        # accident_df_cleaned.to_csv('data/ACCIDENT_CLEANED/accident_df_cleaned.csv')
        # accident_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/accident_df_cleaned.pkl")
        accident_df_cleaned.to_pickle(self.save_path+"\\"+"accident_df_cleaned.pkl")
        
        
        
        
        #2
        # self.csv_path='data\ACCIDENT'
        single_csv='ACCIDENT_CHAINAGE.csv'
        print('Cleaning '+single_csv)
        accident_chainage_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        accident_chainage_df_cleaned=pd.DataFrame()
        # tmp=accident_chainage_df[0:5]
        # print(tmp)
        accident_chainage_df=accident_chainage_df.convert_dtypes()
        
        # col_lists=['Node Id', 'Route No', 'Chainage Seq', 'Route Link No', 'Chainage']
    


        old_field_name="Node Id"
        field_name= "NODE_ID"
        accident_chainage_df_cleaned[field_name]= (accident_chainage_df[old_field_name]).astype(str)
        accident_chainage_df_cleaned[field_name]=accident_chainage_df_cleaned[field_name].str.replace(',','')
        grouped_df=accident_chainage_df_cleaned.groupby([field_name]).size()
        
        
        
        
        old_field_name="Route No"
        field_name= "ROUTE_NO"
        accident_chainage_df_cleaned[field_name]= (accident_chainage_df[old_field_name]).astype(str)
        grouped_df=accident_chainage_df_cleaned.groupby([field_name]).size()
        
     
        
        old_field_name="Chainage Seq"
        field_name= "CHAINAGE_SEQ"
        accident_chainage_df_cleaned[field_name]= (accident_chainage_df[old_field_name]).astype(str)
        grouped_df=accident_chainage_df_cleaned.groupby([field_name]).size()
    
    
        old_field_name="Route Link No"
        field_name= "ROUTE_LINK_NO"
        accident_chainage_df_cleaned[field_name]= (accident_chainage_df[old_field_name]).astype(str)
        grouped_df=accident_chainage_df_cleaned.groupby([field_name]).size()
    
       
        old_field_name="Chainage"
        field_name= "CHAINAGE"
        accident_chainage_df_cleaned[field_name]= (accident_chainage_df[old_field_name]).astype(str)
        grouped_df=accident_chainage_df_cleaned.groupby([field_name]).size()
    
        # accident_chainage_df_cleaned.to_csv('data/ACCIDENT_CLEANED/accident_chainage_df_cleaned.csv')
        
        # accident_chainage_df_cleaned.to_pickle(self.save_path+"\\"+"accident_chainage_df_cleaned.pkl")
        accident_chainage_df_cleaned.to_pickle(self.save_path+"\\"+"accident_chainage_df_cleaned.pkl")
        
        #3
        # self.csv_path='data\ACCIDENT'
        single_csv='ACCIDENT_EVENT.csv'
        print('Cleaning '+single_csv)
        accident_event_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        accident_event_df_cleaned=pd.DataFrame()
        # tmp=accident_event_df[0:5]
        # print(tmp)
        accident_event_df=accident_event_df.convert_dtypes()
       
       # col_lists=['ACCIDENT_NO','EVENT_SEQ_NO',
        # 'EVENT_TYPE', 'Event Type Desc',
        #'VEHICLE_1_ID',
         #'VEHICLE_1_COLL_PT',
         #'Vehicle 1 Coll Pt Desc',
         #'VEHICLE_2_ID',
         #'VEHICLE_2_COLL_PT',
         #'Vehicle 2 Coll Pt Desc',
         
        #'PERSON_ID',
        # 'OBJECT_TYPE',
        # 'Object Type Desc']
                   
        field_name="ACCIDENT_NO"           
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        
        field_name="EVENT_SEQ_NO"           
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
    
    
        
        field_name="EVENT_TYPE"
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].replace('<NA>', '9')
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
    
        
        field_name="Event Type Desc"
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        grouped_df=accident_event_df_cleaned.groupby(['EVENT_TYPE','Event Type Desc']).size()
        accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].replace('Unknown', 'Not known ')
       
        code=grouped_df.index.get_level_values('EVENT_TYPE').tolist()
        description=grouped_df.index.get_level_values('Event Type Desc').tolist()
        
        field_name="EVENT_TYPE"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_event_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_event_df_cleaned[field_name])]
       # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
      
    
        field_name="VEHICLE_1_ID"           
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
    
    
    
        field_name="VEHICLE_1_COLL_PT"
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        # accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].replace('<NA>', '9')
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
    
        field_name="Vehicle 1 Coll Pt Desc"
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        # accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].replace(' ', 'Not known or Not Applicable')
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
    
        grouped_df=accident_event_df_cleaned.groupby(['VEHICLE_1_COLL_PT','Vehicle 1 Coll Pt Desc']).size()
    
        code=grouped_df.index.get_level_values('VEHICLE_1_COLL_PT').tolist()
        description=grouped_df.index.get_level_values('Vehicle 1 Coll Pt Desc').tolist()
        
        field_name="VEHICLE_1_COLL_PT"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_event_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_event_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
  
        field_name="VEHICLE_2_ID"           
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
       
    
        field_name="VEHICLE_2_COLL_PT"
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
    
        field_name="Vehicle 2 Coll Pt Desc"
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
    
        grouped_df=accident_event_df_cleaned.groupby(['VEHICLE_2_COLL_PT','Vehicle 2 Coll Pt Desc']).size()
    
        code=grouped_df.index.get_level_values('VEHICLE_2_COLL_PT').tolist()
        description=grouped_df.index.get_level_values('Vehicle 2 Coll Pt Desc').tolist()
        
        field_name="VEHICLE_2_COLL_PT"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_event_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_event_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
        
        field_name="PERSON_ID"
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].replace('  ', '<NA>')
        accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].replace('2 ', '02')
        accident_event_df_cleaned[field_name] = accident_event_df_cleaned[field_name].str.strip()
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
      
        
        
        field_name="OBJECT_TYPE"
        accident_event_df_cleaned[field_name]=(accident_event_df[field_name]).map('{0:0=2d}'.format).astype(str)
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
     
        field_name="Object Type Desc"
        accident_event_df_cleaned[field_name]=accident_event_df[field_name].astype(str)
        grouped_df=accident_event_df_cleaned.groupby([field_name]).size()
     
        grouped_df=accident_event_df_cleaned.groupby(['OBJECT_TYPE','Object Type Desc']).size()
    
        code=grouped_df.index.get_level_values('OBJECT_TYPE').tolist()
        description=grouped_df.index.get_level_values('Object Type Desc').tolist()
        
        field_name="OBJECT_TYPE"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_event_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_event_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
        # accident_event_df_cleaned.to_csv('data/ACCIDENT_CLEANED/accident_event_df_cleaned.csv')
        # accident_event_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/accident_event_df_cleaned.pkl")
        accident_event_df_cleaned.to_pickle(self.save_path+"\\"+"accident_event_df_cleaned.pkl")
        
       
        
        #4
        # self.csv_path='data\ACCIDENT'
        single_csv='ACCIDENT_LOCATION.csv'
        print('Cleaning '+single_csv)
        accident_location_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        accident_location_df_cleaned=pd.DataFrame()
        accident_location_df=accident_location_df.convert_dtypes()
        # tmp=accident_location_df[0:5]
        # print(tmp)
       

        # col_lists=['ACCIDENT_NO', 'NODE_ID','ROAD_ROUTE_1',
        #  'ROAD_NAME', 'ROAD_TYPE',  'ROAD_NAME_INT','ROAD_TYPE_INT',
        #  'DISTANCE_LOCATION','DIRECTION_LOCATION',
        #  'NEAREST_KM_POST',
        #  'OFF_ROAD_LOCATION']
  
         
        
        
        field_name="ACCIDENT_NO"           
        accident_location_df_cleaned[field_name]=accident_location_df[field_name].astype(str)
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
    
    
        field_name="NODE_ID"           
        accident_location_df_cleaned[field_name]=accident_location_df[field_name].astype(str)
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
    
        field_name="ROAD_ROUTE_1"           
        accident_location_df_cleaned[field_name]=accident_location_df[field_name]
        
        tmp=accident_location_df_cleaned[field_name].copy().astype(str)
    
        mask=(accident_location_df_cleaned[field_name] >=2000) & (accident_location_df_cleaned[field_name] <3000)
        tmp.loc[mask]='2000-2999'
        
        mask=(accident_location_df_cleaned[field_name] >=3000) & (accident_location_df_cleaned[field_name] <4000)
        tmp.loc[mask]='3000-3999'
        
        mask=(accident_location_df_cleaned[field_name] >=4000) & (accident_location_df_cleaned[field_name] <5000) 
        tmp.loc[mask]='4000-4999'
        
        mask=(accident_location_df_cleaned[field_name] >=5000) & (accident_location_df_cleaned[field_name] <6000)
        tmp.loc[mask]='5000-5999'
       
        mask=(accident_location_df_cleaned[field_name] >=7000) & (accident_location_df_cleaned[field_name] <8000) 
        tmp.loc[mask]='7000-7999'
        
        mask=(accident_location_df_cleaned[field_name] >=8000) & (accident_location_df_cleaned[field_name] <9000) 
        tmp.loc[mask]='8000-8999'
        
        
        mask=(accident_location_df_cleaned[field_name] == 9999) 
        tmp.loc[mask]='9999'
        
        mask=(accident_location_df_cleaned[field_name] == -1) 
        tmp.loc[mask]='<NA>'
        
        mask=(accident_location_df_cleaned[field_name] == 0) 
        tmp.loc[mask]='<NA>'
        grouped_df=tmp.value_counts()
        
        accident_location_df_cleaned[field_name]=tmp
        
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
        description=[ 'Freeways or Highways',
                     'Forest Rds',
                     'Tourist Rds', 
                     'Main Rds',
                     'Ramps (mainly Freeway ramps)',
                     'No Information',
                     'Unclassified Roads e.g. Council / "Local" roads',
                     'No Information']

        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        accident_location_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(accident_location_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
    
     
        
        
        field_name="ROAD_NAME"           
        accident_location_df_cleaned[field_name] = accident_location_df[field_name].astype(str)
        accident_location_df_cleaned[field_name] = accident_location_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
        
        field_name="ROAD_TYPE"           
        accident_location_df_cleaned[field_name] = accident_location_df[field_name].astype(str)
        accident_location_df_cleaned[field_name] = accident_location_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
    
    
        field_name="ROAD_NAME_INT"           
        accident_location_df_cleaned[field_name] = accident_location_df[field_name].astype(str)
        accident_location_df_cleaned[field_name] = accident_location_df_cleaned[field_name].replace(' ', '<NA>')
        # accident_location_df_cleaned[field_name] = accident_location_df_cleaned[field_name].replace('nan', '<NA>')
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
        
    
        field_name="ROAD_TYPE_INT"           
        accident_location_df_cleaned[field_name] = accident_location_df[field_name].astype(str)
        accident_location_df_cleaned[field_name] = accident_location_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
        
        
        field_name="DISTANCE_LOCATION"           
        accident_location_df_cleaned[field_name]=accident_location_df[field_name]
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
    
        
   
        field_name="DIRECTION_LOCATION"        
        accident_location_df_cleaned[field_name]=accident_location_df[field_name].astype(str)
        accident_location_df_cleaned[field_name] = accident_location_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
       
        
        
        field_name="NEAREST_KM_POST"           
        accident_location_df_cleaned[field_name]=accident_location_df[field_name].astype(str)
        accident_location_df_cleaned[field_name] = accident_location_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()
        
     
        
        field_name="OFF_ROAD_LOCATION"   
        accident_location_df_cleaned[field_name]=accident_location_df[field_name].astype(str)
        accident_location_df_cleaned[field_name] = accident_location_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=accident_location_df_cleaned.groupby([field_name]).size()        
      
               
        # accident_location_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/accident_location_df_cleaned.pkl")
        accident_location_df_cleaned.to_pickle(self.save_path+"\\"+"accident_location_df_cleaned.pkl")
        
       
        
        #5
        # self.csv_path='data\ACCIDENT'
        single_csv='ATMOSPHERIC_COND.csv'
        print('Cleaning '+single_csv)
        atomos_cond_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        atomos_cond_df_cleaned=pd.DataFrame()
        atomos_cond_df=atomos_cond_df.convert_dtypes()
        # tmp=atomos_cond_df[0:5]
        # print(tmp)
        # print(list(tmp))
        # col_list=['ACCIDENT_NO', 'ATMOSPH_COND', 'ATMOSPH_COND_SEQ', 'Atmosph Cond Desc']
        
       
        field_name="ACCIDENT_NO"           
        atomos_cond_df_cleaned[field_name]=atomos_cond_df[field_name].astype(str)
        grouped_df=atomos_cond_df_cleaned.groupby([field_name]).size()
    
           
        
        field_name="ATMOSPH_COND"
        atomos_cond_df_cleaned[field_name]=atomos_cond_df[field_name].astype(str)
        grouped_df=atomos_cond_df_cleaned.groupby([field_name]).size()
    
        field_name="Atmosph Cond Desc"
        atomos_cond_df_cleaned[field_name]=atomos_cond_df[field_name].astype(str)
        grouped_df=atomos_cond_df_cleaned.groupby([field_name]).size()
        
        grouped_df=atomos_cond_df_cleaned.groupby(['ATMOSPH_COND','Atmosph Cond Desc']).size()
        code=grouped_df.index.get_level_values('ATMOSPH_COND').tolist()
        description=grouped_df.index.get_level_values('Atmosph Cond Desc').tolist()
        
        field_name="ATMOSPH_COND"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        atomos_cond_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(atomos_cond_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
        
        
    
        field_name="ATMOSPH_COND_SEQ"
        atomos_cond_df_cleaned[field_name]=atomos_cond_df[field_name].astype(str)
        grouped_df=atomos_cond_df_cleaned.groupby([field_name]).size()
        
        # atomos_cond_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/atomos_cond_df_cleaned.pkl")
        atomos_cond_df_cleaned.to_pickle(self.save_path+"\\"+"atomos_cond_df_cleaned.pkl")
     
       
        #6
        # self.csv_path='data\ACCIDENT'
        single_csv='NODE.csv'
        print('Cleaning '+single_csv)
        node_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        node_df_cleaned=pd.DataFrame()
        node_df=node_df.convert_dtypes()
        # tmp=node_df[0:5]
        # print(tmp)
        # print(list(tmp))
        # col_list=['ACCIDENT_NO', 'NODE_ID', 'NODE_TYPE', 'VICGRID94_X', 'VICGRID94_Y', 
        #           'LGA_NAME', 'LGA_NAME_ALL', 'REGION_NAME', 'DEG_URBAN_NAME', 
        #           'Lat', 'Long', 'POSTCODE_NO']
        
 

        
        field_name="ACCIDENT_NO"           
        node_df_cleaned[field_name]=node_df[field_name].astype(str)
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
        
        field_name="NODE_ID"
        node_df_cleaned[field_name]=node_df[field_name].astype(str)
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
        
        field_name="NODE_TYPE"
        node_df_cleaned[field_name]=node_df[field_name].astype(str)
        node_df_cleaned[field_name] = node_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
      
        
        field_name="VICGRID94_X"
        node_df_cleaned[field_name]=node_df[field_name]
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
        
        field_name="VICGRID94_Y"
        node_df_cleaned[field_name]=node_df[field_name]
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
        
    
        field_name="LGA_NAME"
        node_df_cleaned[field_name]=node_df[field_name].astype(str)
        node_df_cleaned[field_name] = node_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
        
        field_name='LGA_NAME_ALL'
        node_df_cleaned[field_name]=node_df[field_name].astype(str)
        node_df_cleaned[field_name] = node_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
        
        field_name='REGION_NAME'
        node_df_cleaned[field_name]=node_df[field_name].astype(str)
        node_df_cleaned[field_name] = node_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
        field_name='DEG_URBAN_NAME'
        node_df_cleaned[field_name]=node_df[field_name].astype(str)
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
    
        
        field_name='Lat'
        node_df_cleaned[field_name]=node_df[field_name]
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
     
        field_name='Long'
        node_df_cleaned[field_name]=node_df[field_name]
        grouped_df=node_df_cleaned.groupby([field_name]).size()
       
        
        field_name='POSTCODE_NO'
        node_df_cleaned[field_name]=node_df[field_name].astype(str)
        grouped_df=node_df_cleaned.groupby([field_name]).size()
        
        # node_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/node_df_cleaned.pkl")
        node_df_cleaned.to_pickle(self.save_path+"\\"+"node_df_cleaned.pkl")
        
        
        #7
        # self.csv_path='data\ACCIDENT'
        single_csv='NODE_ID_COMPLEX_INT_ID.csv'
        print('Cleaning '+single_csv)
        node_cmplx_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        node_cmplx_df_cleaned=pd.DataFrame()
        node_cmplx_df=node_cmplx_df.convert_dtypes()
        # tmp=node_cmplx_df[0:5]
        # print(tmp)
        # print(list(tmp))
        # col_list=['ACCIDENT_NO', 'NODE_ID', 'COMPLEX_INT_NO']

        field_name="ACCIDENT_NO"           
        node_cmplx_df_cleaned[field_name]=node_cmplx_df[field_name].astype(str)
        grouped_df=node_cmplx_df_cleaned.groupby([field_name]).size()
        
        field_name="NODE_ID"           
        node_cmplx_df_cleaned[field_name]=node_cmplx_df[field_name].astype(str)
        grouped_df=node_cmplx_df_cleaned.groupby([field_name]).size()
        
        field_name="COMPLEX_INT_NO"           
        node_cmplx_df_cleaned[field_name]=node_cmplx_df[field_name].astype(str)
        grouped_df=node_cmplx_df_cleaned.groupby([field_name]).size()
        
        # node_cmplx_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/node_cmplx_df_cleaned.pkl")
        node_cmplx_df_cleaned.to_pickle(self.save_path+"\\"+"node_cmplx_df_cleaned.pkl")
        

        
        #8
        # self.csv_path='data\ACCIDENT'
        single_csv='PERSON.csv'
        print('Cleaning '+single_csv)
        person_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        person_df_cleaned=pd.DataFrame()
        person_df=person_df.convert_dtypes()
        # tmp=person_df[0:5]
        # print(tmp)
        # print(list(tmp))
        # col_list=['ACCIDENT_NO', 
        #            'PERSON_ID', 'VEHICLE_ID',
        #            'SEX', 'AGE', 'Age Group', 
        #            'INJ_LEVEL', 'Inj Level Desc', 
        #            'SEATING_POSITION', 'HELMET_BELT_WORN',
        #            'ROAD_USER_TYPE', 'Road User Type Desc', 
        #            'LICENCE_STATE', 'PEDEST_MOVEMENT', 
        #            'POSTCODE', 'TAKEN_HOSPITAL', 'EJECTED_CODE']
    

         
   
    
        field_name="ACCIDENT_NO"           
        person_df_cleaned[field_name]=person_df[field_name].astype(str)
        grouped_df=person_df_cleaned.groupby([field_name]).size()


        field_name="PERSON_ID"
        person_df_cleaned[field_name]=person_df[field_name].astype(str).str.strip()
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        

        field_name="VEHICLE_ID"
        person_df_cleaned[field_name]=person_df[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        
        
        
        field_name="SEX"
        person_df_cleaned[field_name]=person_df[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        description=[ 'No Information', 'Female', 'Male', 'Not known']
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        person_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(person_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
        
        field_name="AGE"
        person_df_cleaned[field_name]=person_df[field_name]
        grouped_df=person_df_cleaned.groupby([field_name]).size()
       
    
        
        old_field_name='Age Group'
        field_name='AGE_GROUP'
        person_df_cleaned[field_name]=person_df[old_field_name].astype(str)
        grouped_df=person_df_cleaned.groupby([field_name]).size()
       
        
        #from here
        field_name="INJ_LEVEL"
        person_df_cleaned[field_name]=person_df[field_name]
        person_df_cleaned[field_name]=pd.to_numeric(person_df_cleaned[field_name],errors='coerce')
        mask = pd.to_numeric(person_df_cleaned[field_name]).notnull()
        person_df_cleaned[field_name].loc[mask]=((person_df_cleaned[field_name].loc[mask]).astype(int).round()).astype(str)
        person_df_cleaned[field_name]=person_df_cleaned[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace('nan', '<NA>')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
       
       
       
        field_name="Inj Level Desc"
        person_df_cleaned[field_name]=person_df[field_name].astype(str)
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        
        grouped_df=person_df_cleaned.groupby(['INJ_LEVEL','Inj Level Desc']).size()
        
        code=grouped_df.index.get_level_values('INJ_LEVEL').tolist()
        description=grouped_df.index.get_level_values('Inj Level Desc').tolist()
        
        field_name="INJ_LEVEL"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        person_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(person_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
        
    
        field_name='SEATING_POSITION'
        person_df_cleaned[field_name] =person_df[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace('  ', 'NA')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()       
        description=[ 'No Information', 'Centre-front','Centre-rear','Driver or rider','Left-front','Left-rear', 'Not applicable', 'Not known','Other-rear','Pillion passenger', 'Motorcycle sidecar passenger','Right-rear']
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                       
        person_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(person_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
       
      
        field_name='HELMET_BELT_WORN'
        person_df_cleaned[field_name] =person_df[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list() 
        
        description=[ 'Seatbelt worn','Seatbelt not worn','Child restraint worn', 'Child restraint not worn', 'Seatbelt/restraint not fitted', 'Crash helmet worn', 'Crash helmet not worn', 'Not appropriate','Not known','No Information']

        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                       
        person_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(person_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
        
        field_name='ROAD_USER_TYPE'
        person_df_cleaned[field_name]=pd.to_numeric(person_df[field_name],errors='coerce')
        mask = pd.to_numeric(person_df_cleaned[field_name]).notnull()
        person_df_cleaned[field_name].loc[mask]=((person_df_cleaned[field_name].loc[mask]).astype(int).round()).astype(str)
        person_df_cleaned[field_name]=person_df_cleaned[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace('nan', '9')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        
        field_name='Road User Type Desc'
        person_df_cleaned[field_name] =person_df[field_name].astype(str)
        # person_df_cleaned[field_name] = person_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        
        grouped_df=person_df_cleaned.groupby(['ROAD_USER_TYPE','Road User Type Desc']).size()
        code=grouped_df.index.get_level_values('ROAD_USER_TYPE').tolist()
        description=grouped_df.index.get_level_values('Road User Type Desc').tolist()
        # print(code,description)
        field_name='ROAD_USER_TYPE'
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                       
        person_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(person_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
        
    
        field_name='LICENCE_STATE'
        person_df_cleaned[field_name] =person_df[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.tolist()
        description=['Not available', 'Australian Capital Territory', 'Commonwealth', 'Northern Territory',
                     'New South Wales', 'Overseas', 'Queensland','South Australia','Tasmania',
                     'Victoria','Western Australia','Not known']
        
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        person_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(person_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
          
       
        
        field_name='PEDEST_MOVEMENT'
        person_df_cleaned[field_name]=person_df[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
        
        description=['Not applicable','Crossing carriageway', 'Working/playing/lying or standing on carriageway',
                     'Walking on carriageway with traffic', 'Walking on carriageway against traffic',
                     'Pushing or working on vehicle', 'Walking to/from or boarding tram', 'Walking to/from or boarding other vehicle',
                     'Not on carriageway (e.g. footpath)', 'Not known','No Informartion']


        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        person_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(person_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
          
        field_name='POSTCODE'
        person_df_cleaned[field_name]=person_df[field_name].astype(str)
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        

       
        field_name='TAKEN_HOSPITAL'
        person_df_cleaned[field_name]=person_df[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        
        
        field_name='EJECTED_CODE'
        person_df_cleaned[field_name]=person_df[field_name].astype(str)
        person_df_cleaned[field_name] = person_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=person_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
    
        description=['Not applicable',
                     'Total ejected',
                     'Partially ejected',
                     'Partial ejection involving extraction',
                     'Not known',
                     'No Information']

       
   
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        person_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(person_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
      
        # person_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/person_df_cleaned.pkl")
        person_df_cleaned.to_pickle(self.save_path+"\\"+"person_df_cleaned.pkl")
        

   
          
        #9
        # self.csv_path='data\ACCIDENT'
        single_csv='ROAD_SURFACE_COND.csv'
        print('Cleaning '+single_csv)
        road_cond_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        road_cond_df_cleaned=pd.DataFrame()
        road_cond_df=road_cond_df.convert_dtypes()
        # tmp=road_cond_df[0:5]
        # print(tmp)
        # print(list(tmp))
        # col_list=['ACCIDENT_NO', 'SURFACE_COND', 'Surface Cond Desc', 'SURFACE_COND_SEQ']
        
    
        field_name="ACCIDENT_NO"           
        road_cond_df_cleaned[field_name]=road_cond_df[field_name].astype(str)
        grouped_df=road_cond_df_cleaned.groupby([field_name]).size()
        

        
        field_name="SURFACE_COND"
        road_cond_df_cleaned[field_name]=road_cond_df[field_name].astype(str)
        grouped_df=road_cond_df_cleaned.groupby([field_name]).size()
        
        field_name="Surface Cond Desc"
        road_cond_df_cleaned[field_name]=road_cond_df[field_name].astype(str)
        grouped_df=road_cond_df_cleaned.groupby([field_name]).size()
        
        grouped_df=road_cond_df_cleaned.groupby(['SURFACE_COND','Surface Cond Desc']).size()
        code=grouped_df.index.get_level_values('SURFACE_COND').tolist()
        description=grouped_df.index.get_level_values('Surface Cond Desc').tolist()
        field_name="SURFACE_COND"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                       
        road_cond_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(road_cond_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
        
        field_name="SURFACE_COND_SEQ"
        road_cond_df_cleaned[field_name]=road_cond_df[field_name].astype(str)
        grouped_df=road_cond_df_cleaned.groupby([field_name]).size()
        
        
        # road_cond_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/road_cond_df_cleaned.pkl")
        road_cond_df_cleaned.to_pickle(self.save_path+"\\"+"road_cond_df_cleaned.pkl")
   
          
        #10
        # self.csv_path='data\ACCIDENT'
        single_csv='SUBDCA.csv'
        print('Cleaning '+single_csv)
        subdca_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        subdca_df_cleaned=pd.DataFrame()
        subdca_df=subdca_df.convert_dtypes()
        # tmp=subdca_df[0:5]
        # print(tmp)
        # print(list(tmp))
        # col_list=['ACCIDENT_NO', 'SUB_DCA_CODE', 'SUB_DCA_SEQ', 'Sub Dca Code Desc']
        
        field_name="ACCIDENT_NO"           
        subdca_df_cleaned[field_name]=subdca_df[field_name].astype(str)
        grouped_df=subdca_df_cleaned.groupby([field_name]).size()
        
        
        field_name="SUB_DCA_CODE"           
        subdca_df_cleaned[field_name]=subdca_df[field_name].astype(str)
        subdca_df_cleaned[field_name] = subdca_df_cleaned[field_name].replace('   ', '<NA>')
        grouped_df=subdca_df_cleaned.groupby([field_name]).size()
        
        field_name="Sub Dca Code Desc"           
        subdca_df_cleaned[field_name]=subdca_df[field_name].astype(str)
        # subdca_df_cleaned[field_name] = subdca_df_cleaned[field_name].replace('   ', '<NA>')
        grouped_df=subdca_df_cleaned.groupby([field_name]).size()
        
        grouped_df=subdca_df_cleaned.groupby(['SUB_DCA_CODE','Sub Dca Code Desc']).size()
        code=grouped_df.index.get_level_values('SUB_DCA_CODE').tolist()
        description=grouped_df.index.get_level_values('Sub Dca Code Desc').tolist()

        # print(code,description)  
        field_name="SUB_DCA_CODE" 
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)
        subdca_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(subdca_df_cleaned[field_name])]

        # for cod in code :
          # print(cod)
          # print(self.label_decoder(field_name,cod))
        
        field_name="SUB_DCA_SEQ"           
        subdca_df_cleaned[field_name]=subdca_df[field_name].astype(str)
        grouped_df=subdca_df_cleaned.groupby([field_name]).size()
        
          
        # subdca_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/subdca_df_cleaned.pkl")
        subdca_df_cleaned.to_pickle(self.save_path+"\\"+"subdca_df_cleaned.pkl")
   
   
          
        #11
        # self.csv_path='data\ACCIDENT'
        single_csv='VEHICLE.csv'
        print('Cleaning '+single_csv)
        vehicle_df=pd.read_csv(self.csv_path+'\\' +single_csv)
        vehicle_df_cleaned=pd.DataFrame()
        vehicle_df=vehicle_df.convert_dtypes()
        # tmp=vehicle_df[0:5]
        # print(tmp)
        # print(list(tmp))
        # col_list=['ACCIDENT_NO', 'VEHICLE_ID', 'VEHICLE_YEAR_MANUF', 
        #           'VEHICLE_DCA_CODE', 'INITIAL_DIRECTION', 
        #           'ROAD_SURFACE_TYPE', 'Road Surface Type Desc', 
        #           'REG_STATE', 
                  
        #           'VEHICLE_BODY_STYLE',  'VEHICLE_MAKE', 'VEHICLE_MODEL', #to check again
                  
        #           'VEHICLE_POWER', 'VEHICLE_TYPE', 'Vehicle Type Desc', 
        #           'VEHICLE_WEIGHT', 'CONSTRUCTION_TYPE', 
        #            'FUEL_TYPE', 'NO_OF_WHEELS', 'NO_OF_CYLINDERS', 
        #           'SEATING_CAPACITY', 'TARE_WEIGHT', 'TOTAL_NO_OCCUPANTS',
        #           'CARRY_CAPACITY','CUBIC_CAPACITY', 
        #           'FINAL_DIRECTION', 'DRIVER_INTENT', 'VEHICLE_MOVEMENT', 
        #           'TRAILER_TYPE', 'VEHICLE_COLOUR_1', 
        #           'VEHICLE_COLOUR_2', 'CAUGHT_FIRE', 'INITIAL_IMPACT', 
        #           'LAMPS', 'LEVEL_OF_DAMAGE',         
        #           'OWNER_POSTCODE', 'TOWED_AWAY_FLAG', 
        #           'TRAFFIC_CONTROL', 'Traffic Control Desc']


        field_name="ACCIDENT_NO"           
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        field_name="VEHICLE_ID"
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
       
        
        field_name="VEHICLE_YEAR_MANUF"
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
       
        
        field_name="VEHICLE_DCA_CODE"
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
     
        
        
        field_name="INITIAL_DIRECTION"
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        description=[ 'East','North','North east','Not known', 'North west' , 'South','South east',
                     'South west','West']
        
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
          # print(cod)
          # print(self.label_decoder(field_name,cod))
        
      
        
      
        field_name="ROAD_SURFACE_TYPE"
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        field_name="Road Surface Type Desc"
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        grouped_df=vehicle_df_cleaned.groupby(['ROAD_SURFACE_TYPE','Road Surface Type Desc']).size()
        code=grouped_df.index.get_level_values('ROAD_SURFACE_TYPE').tolist()
        description=grouped_df.index.get_level_values('Road Surface Type Desc').tolist()
        # print(code,description)
        field_name="ROAD_SURFACE_TYPE"
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
        
     
        field_name="REG_STATE"
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
        # ['<NA>', 'A', 'B', 'D', 'N', 'O', 'Q', 'S', 'T', 'V', 'W', 'Z']
        description=['Not available','Australian Capital Territory', 'Commonwealth', 'Northern Territory', 'New South Wales', 'Overseas', 'Queensland', 'South Australia', 'Tasmania', 'Victoria', 'Western Australia','Not known']
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
          
          
          
       
        field_name='VEHICLE_BODY_STYLE'
        vehicle_df_cleaned[field_name]= vehicle_df[field_name].astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace('      ', '<NA>')
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace('-     ', '<NA>')
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace('CONT-C', 'CONT C')
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace('SED', 'SEDAN')
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace('HORSE', 'HOR FL')
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace('IND/CN', 'INDCON')
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace('MACHNE', 'MACH')
       
       
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.strip()
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
   
        #['4WD', '<NA>', 'AFRAME', 'AG IMP', 'AGR', 'AMB', 'AMPHIB', 'AMUS', 'ARM V', 'B BIN', 'B HOE', 'B TR', 'BD/PMR', 'BOX', 'BUGGY', 'BUS', 'C CARR', 'C CHAS', 'C MIX', 'C PUMP', 'CARAVN', 'CARVN', 'CHAIR', 'COMPAC', 'CONT C', 'CONVN', 'CONVRT', 'COUPE', 'CRANE', 'CVN', 'CYCLE', 'DC UTE', 'DITCH', 'DOZER', 'DUMPER', 'EDUCTR', 'EXCVTR', 'F LIFT', 'F UNIT', 'FLOAT', 'FLUSH', 'FRAME', 'FRE', 'G UNIT', 'GRADER', 'HATCH', 'HEARSE', 'HOE', 'HOR FL', 'HORSE', 'INDCON', 'JEEP', 'L FRM', 'L MARK', 'LADDER', 'LOADER', 'M BILL', 'M CCH', 'M PLAT', 'M STND', 'M TRLY', 'MAC', 'MACH', 'MISC', 'MOPED', 'MOWER', 'MULTI', 'MULTIX', 'NFR', 'OTH', 'OUTFIT', 'P', 'P CARR', 'P MVR', 'P VAN', 'PBCYC', 'PBVEH', 'PMAMUS', 'POLERC', 'R AMUS', 'RDSTR', 'ROLLER', 'S AMRV', 'S AMUS', 'S BULK', 'S CAR', 'S TANK', 'S TIP', 'S TRAY', 'S TRL', 'S WAG', 'SCOOTR', 'SED', 'SEDAN', 'SERV', 'SKELTN', 'SKIP C', 'SKIP/C', 'SNOW M', 'SOLO', 'SP VEH', 'SPRAY', 'SPREAD', 'ST TRK', 'SWEEP', 'SWV1', 'SWV2', 'T JINK', 'T SDS', 'T TRK', 'TANKER', 'TILTER', 'TIPPER', 'TOURER', 'TOWER', 'TRA', 'TRACT', 'TRAILR', 'TRAIN', 'TRAM', 'TRAY', 'TRICAR', 'TRITRA', 'UNK', 'UTI', 'UTIL', 'VAN', 'WAGNM', 'WAGON', 'WHCBUG', 'WINCH', 'WRKP U']
        # =============================================================================
        #         to add label encoder
        # =============================================================================
        

        
        # # =============================================================================
        # #  check this
        # # =============================================================================
        field_name='VEHICLE_MAKE'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)

        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace('      ', '<NA>')
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.strip()
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        # ['4SEA', '<NA>', 'A B C', 'A BU', 'A E C', 'A J S', 'A M', 'A SID', 'A.T.I', 'A1', 'ABARTH', 'ABOX', 'ACE', 'ACUR', 'ADLER', 'ADLY', 'AENG', 'AGUSTA', 'AJP', 'ALFA R', 'ALLIS', 'ALVIS', 'AMACCH', 'AMERIH', 'AMRCAN', 'ANSAIR', 'APEL', 'APRILI', 'ARGO', 'ARIEL', 'ARQI', 'ARQU', 'ASIA', 'ASSA', 'ASTON', 'ATECO', 'ATK', 'ATKINS', 'ATOM', 'ATOMIC', 'ATOMIK', 'AUD', 'AUDI', 'AUST T', 'AUSTIN', 'AUSTRL', 'AUSWDE', 'AVEN', 'AWO', 'B C', 'B C I', 'B KNOX', 'B M W', 'B S A', 'B VELL', 'B.M.', 'B.M.W.', 'BANDIT', 'BAOT', 'BARKER', 'BART', 'BARTCO', 'BCI', 'BEAVER', 'BED', 'BENELL', 'BENT', 'BENZHO', 'BETA', 'BILL', 'BIMOTA', 'BLUE', 'BOBCAT', 'BOLLIN', 'BOLWEL', 'BOMBDR', 'BOOM T', 'BORU', 'BOSS', 'BRAA', 'BRAAAP', 'BREN', 'BRENTW', 'BRIM', 'BRISTL', 'BRP', 'BRP IN', 'BSTONE', 'BUELL', 'BUG', 'BUGAT', 'BUICK', 'BUST', 'BYRN', 'BZOOMA', 'CAD', 'CAGIVA', 'CAMERO', 'CAN', 'CAN AM', 'CARAC', 'CARRAR', 'CASE', 'CATA', 'CATERP', 'CATPLR', 'CF MOT', 'CFMO', 'CFMOTO', 'CHALL', 'CHALLE', 'CHAMB', 'CHERTN', 'CHERY', 'CHEV', 'CHEVRO', 'CHRYS', 'CHRYSL', 'CIMC', 'CITRN', 'CITROE', 'CLAAS', 'CLARK', 'CLASIC', 'CLEVE', 'CLVL', 'CLVLND', 'COACHM', 'COBRA', 'COBRJT', 'COLES', 'CONF', 'CONSUL', 'CORT', 'CROWN', 'CRYO', 'D A', 'D EA', 'D EAGL', 'DAEL', 'DAELIM', 'DAEWOO', 'DAF', 'DAI', 'DAIHAT', 'DAIM', 'DATSUN', 'DELTEK', 'DEMO', 'DENING', 'DENNIS', 'DERBI', 'DESI', 'DEUTCH', 'DEUTZ', 'DGE', 'DHAT', 'DIBLAS', 'DODGE', 'DONG', 'DOOSAN', 'DRIF', 'DUCATI', 'DUNBIE', 'DYNAPC', 'EAGL', 'EBR', 'ELPH', 'ELSTAR', 'ELTRAN', 'ENFL', 'ENFLD', 'ENZ', 'ERID', 'ERIDER', 'ESSEX', 'EUNOS', 'EUROPE', 'EXPR', 'EZGO', 'F HILL', 'F KNT', 'F LI', 'F LINE', 'FAHR', 'FENDT', 'FERG', 'FERRAR', 'FIAT', 'FIP', 'FLEETW', 'FMC', 'FNMO', 'FODEN', 'FOR', 'FORD', 'FORDSN', 'FOTO', 'FOTON', 'FOXI', 'FRANNA', 'FREIGH', 'FREITR', 'FREU', 'FREUHF', 'FRGHTR', 'FRNKLN', 'FTE', 'FURUKA', 'FUSO', 'FYM', 'G M C', 'G T E', 'G WA', 'G WALL', 'GALANT', 'GASG', 'GASGAS', 'GEHL', 'GENI', 'GILERA', 'GM HOL', 'GREA', 'GREAT', 'GROVE', 'GT', 'GTE', 'GUIZZO', 'H DA', 'H DAV', 'H MADE', 'HAKO', 'HAM', 'HAME', 'HAMM', 'HARLEY', 'HARLYD', 'HAUL', 'HAVAL', 'HERC', 'HERCUL', 'HIGE', 'HIGER', 'HINO', 'HITACH', 'HOLAND', 'HOLDEN', 'HONDA', 'HSQV', 'HSQVRN', 'HUAN', 'HUDSON', 'HUM', 'HUMM', 'HUMMER', 'HUNT', 'HUNTER', 'HUSB', 'HUSBRG', 'HUSQVA', 'HYND', 'HYNDAI', 'HYOSUN', 'HYSTER', 'HYU', 'HYUNDA', 'HYUNDI', 'I B C', 'I RAND', 'IBOSA', 'IKONIC', 'INDC', 'INDCON', 'INDIAN', 'INFN', 'INFNTI', 'INTERN', 'IRSB', 'ISEKI', 'ISUZU', 'ITALA', 'IVEC D', 'IVECO', 'J C', 'J C B', 'J DE', 'J DEER', 'JAC', 'JAGUAR', 'JAWA', 'JAYCO', 'JEEP', 'JIAJ', 'JIAJUE', 'JINAN', 'JMC', 'JOHNST', 'JTB', 'K LO', 'K T', 'K T M', 'KALMAR', 'KATO', 'KAWASA', 'KAWASK', 'KAWS', 'KENN', 'KENWOR', 'KENWTH', 'KIA', 'KING', 'KINL', 'KOBELC', 'KOERIN', 'KOMATS', 'KRUEGR', 'KUBOTA', 'KYMC', 'KYMCO', 'L BAGN', 'L MOTO', 'L RO', 'L ROV', 'LADA', 'LAMB G', 'LAN', 'LANCER', 'LANCIA', 'LAND R', 'LANDRO', 'LANRON', 'LARO', 'LARO-D', 'LAVERD', 'LDV', 'LEWCOM', 'LEX', 'LEXUS', 'LEY', 'LIEBHR', 'LIFA', 'LIFAN', 'LINCLN', 'LINER', 'LML', 'LNGHRN', 'LOMB', 'LONC', 'LOTUS', 'LROVER', 'LUCAR', 'LUNA', 'LUSTY', 'M A', 'M A N', 'M C', 'M C A', 'M G', 'M GU', 'M GUZZ', 'M MOVE', 'MACK', 'MACKAY', 'MAHIND', 'MAJOR', 'MANITO', 'MARINR', 'MARSH', 'MARSTA', 'MASER', 'MASS F', 'MATCH', 'MAXC', 'MAXI', 'MAXI T', 'MAXITR', 'MAZ', 'MAZDA', 'MBC', 'MBEL', 'MC L', 'MC LRN', 'MCCOR', 'MCOA', 'MEGE', 'MEGELL', 'MEID', 'MER', 'MERC B', 'MERCBZ', 'MERCED', 'MERLIN', 'MGUZ', 'MISTUB', 'MITSUB', 'MMTB', 'MNI', 'MOBILE', 'MODERN', 'MOFFAT', 'MOFFET', 'MORGAN', 'MORI', 'MORINI', 'MORRIS', 'MOT', 'MOTO G', 'MOTOVE', 'MRT', 'MUSTNG', 'MV A', 'MV AGU', 'MV AUG', 'N HO', 'N HOLL', 'NIS', 'NISSAN', 'NORA', 'NORTON', 'NOT', 'NOT LI', 'ODSSEY', 'OFF', 'OKA', 'OLDS', 'OLIVER', 'OMEGA', 'OPEL', 'OPHEE', 'OTHR', 'OVERLA', 'OZ T', 'P H', 'P M', 'PACIF', 'PACK', 'PAGS', 'PAGSTA', 'PAJE', 'PAKE', 'PARA', 'PEKI', 'PETER', 'PETERB', 'PEU', 'PEUG', 'PEUGEO', 'PGO', 'PIAGGI', 'PICCIN', 'PITP', 'PITPRO', 'PLY', 'POHLNR', 'POLARI', 'PONT', 'PORSCH', 'PRNO', 'PRO', 'PROTON', 'PWM', 'QINGQI', 'R EN', 'R ENF', 'R ROV', 'R W', 'RAM', 'RAMBLR', 'REN', 'RENAUL', 'REPCO', 'RILEY', 'ROAD', 'ROLLS', 'ROVER', 'S J', 'S W M', 'SAAB', 'SACHS', 'SACI', 'SAKAI', 'SAME', 'SAMSNG', 'SCANIA', 'SCHW', 'SCOOTR', 'SCP', 'SEAT', 'SHEN', 'SHER', 'SHERCO', 'SHINER', 'SHINKO', 'SINGER', 'SKAD', 'SKO', 'SKODA', 'SKYT', 'SKYTEA', 'SMART', 'SNOC', 'SNOR', 'SOLO', 'SPAR', 'SSANGY', 'STAND', 'STAR', 'STAT', 'STE', 'STEE', 'STERLI', 'STEVEN', 'STHN', 'STIGA', 'STONEF', 'STUDE', 'SUB', 'SUBARU', 'SUNB', 'SUNBEA', 'SUPR', 'SUZUKI', 'SYDCIT', 'SYM', 'T D', 'T G', 'T G B', 'T T', 'T200', 'TADANO', 'TAG', 'TAGA', 'TALV', 'TATA', 'TAYDUN', 'TAYLOR', 'TDR', 'TECH', 'TEFC', 'TEREX', 'TESL', 'TESLA', 'TGB', 'THE TR', 'THOL', 'THT', 'THUM', 'TIEM', 'TITA', 'TITAN', 'TITI', 'TM', 'TOME', 'TONE', 'TOPSTA', 'TORI', 'TORINO', 'TORO', 'TORPED', 'TOWPAX', 'TOYOTA', 'TRACQP', 'TRAI', 'TRAILE', 'TRAIN', 'TRANTO', 'TRAVEL', 'TRIUM', 'TRIUMP', 'TROLLE', 'TRPLNE', 'TTF', 'U D', 'U D NI', 'ULTIMT', 'UNIVER', 'UNK', 'UNKN', 'UNKW', 'UNSPEC', 'URAL', 'V MO', 'VALIAN', 'VALT', 'VALTRA', 'VAUH', 'VAWD', 'VECT', 'VECTRI', 'VELOC', 'VESPA', 'VIC', 'VICTA', 'VICTOR', 'VICTRY', 'VINC', 'VINCEN', 'VLK', 'VMOTO', 'VOGELE', 'VOL', 'VOLKS', 'VOLKSW', 'VOLVO', 'VW', 'W ST', 'W STAR', 'WACKER', 'WANG', 'WESTER', 'WESTRN', 'WHITE', 'WILLYS', 'WINNEB', 'WIRTGN', 'WOLSE', 'WSTRN', 'XMOT', 'YALE', 'YAMAHA', 'YANMAR', 'YARDMN', 'YIBE', 'YORK', 'YUTONG', 'ZERO', 'ZETOR', 'ZHEJNG', 'ZHONGN', 'ZONG', 'ZZ N']
        # =============================================================================
        #         to add label encoder
        # =============================================================================


        field_name='VEHICLE_MODEL'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace('      ', '<NA>')
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.strip()
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
        # ['4SEA', '<NA>', 'A B C', 'A BU', 'A E C', 'A J S', 'A M', 'A SID', 'A.T.I', 'A1', 'ABARTH', 'ABOX', 'ACE', 'ACUR', 'ADLER', 'ADLY', 'AENG', 'AGUSTA', 'AJP', 'ALFA R', 'ALLIS', 'ALVIS', 'AMACCH', 'AMERIH', 'AMRCAN', 'ANSAIR', 'APEL', 'APRILI', 'ARGO', 'ARIEL', 'ARQI', 'ARQU', 'ASIA', 'ASSA', 'ASTON', 'ATECO', 'ATK', 'ATKINS', 'ATOM', 'ATOMIC', 'ATOMIK', 'AUD', 'AUDI', 'AUST T', 'AUSTIN', 'AUSTRL', 'AUSWDE', 'AVEN', 'AWO', 'B C', 'B C I', 'B KNOX', 'B M W', 'B S A', 'B VELL', 'B.M.', 'B.M.W.', 'BANDIT', 'BAOT', 'BARKER', 'BART', 'BARTCO', 'BCI', 'BEAVER', 'BED', 'BENELL', 'BENT', 'BENZHO', 'BETA', 'BILL', 'BIMOTA', 'BLUE', 'BOBCAT', 'BOLLIN', 'BOLWEL', 'BOMBDR', 'BOOM T', 'BORU', 'BOSS', 'BRAA', 'BRAAAP', 'BREN', 'BRENTW', 'BRIM', 'BRISTL', 'BRP', 'BRP IN', 'BSTONE', 'BUELL', 'BUG', 'BUGAT', 'BUICK', 'BUST', 'BYRN', 'BZOOMA', 'CAD', 'CAGIVA', 'CAMERO', 'CAN', 'CAN AM', 'CARAC', 'CARRAR', 'CASE', 'CATA', 'CATERP', 'CATPLR', 'CF MOT', 'CFMO', 'CFMOTO', 'CHALL', 'CHALLE', 'CHAMB', 'CHERTN', 'CHERY', 'CHEV', 'CHEVRO', 'CHRYS', 'CHRYSL', 'CIMC', 'CITRN', 'CITROE', 'CLAAS', 'CLARK', 'CLASIC', 'CLEVE', 'CLVL', 'CLVLND', 'COACHM', 'COBRA', 'COBRJT', 'COLES', 'CONF', 'CONSUL', 'CORT', 'CROWN', 'CRYO', 'D A', 'D EA', 'D EAGL', 'DAEL', 'DAELIM', 'DAEWOO', 'DAF', 'DAI', 'DAIHAT', 'DAIM', 'DATSUN', 'DELTEK', 'DEMO', 'DENING', 'DENNIS', 'DERBI', 'DESI', 'DEUTCH', 'DEUTZ', 'DGE', 'DHAT', 'DIBLAS', 'DODGE', 'DONG', 'DOOSAN', 'DRIF', 'DUCATI', 'DUNBIE', 'DYNAPC', 'EAGL', 'EBR', 'ELPH', 'ELSTAR', 'ELTRAN', 'ENFL', 'ENFLD', 'ENZ', 'ERID', 'ERIDER', 'ESSEX', 'EUNOS', 'EUROPE', 'EXPR', 'EZGO', 'F HILL', 'F KNT', 'F LI', 'F LINE', 'FAHR', 'FENDT', 'FERG', 'FERRAR', 'FIAT', 'FIP', 'FLEETW', 'FMC', 'FNMO', 'FODEN', 'FOR', 'FORD', 'FORDSN', 'FOTO', 'FOTON', 'FOXI', 'FRANNA', 'FREIGH', 'FREITR', 'FREU', 'FREUHF', 'FRGHTR', 'FRNKLN', 'FTE', 'FURUKA', 'FUSO', 'FYM', 'G M C', 'G T E', 'G WA', 'G WALL', 'GALANT', 'GASG', 'GASGAS', 'GEHL', 'GENI', 'GILERA', 'GM HOL', 'GREA', 'GREAT', 'GROVE', 'GT', 'GTE', 'GUIZZO', 'H DA', 'H DAV', 'H MADE', 'HAKO', 'HAM', 'HAME', 'HAMM', 'HARLEY', 'HARLYD', 'HAUL', 'HAVAL', 'HERC', 'HERCUL', 'HIGE', 'HIGER', 'HINO', 'HITACH', 'HOLAND', 'HOLDEN', 'HONDA', 'HSQV', 'HSQVRN', 'HUAN', 'HUDSON', 'HUM', 'HUMM', 'HUMMER', 'HUNT', 'HUNTER', 'HUSB', 'HUSBRG', 'HUSQVA', 'HYND', 'HYNDAI', 'HYOSUN', 'HYSTER', 'HYU', 'HYUNDA', 'HYUNDI', 'I B C', 'I RAND', 'IBOSA', 'IKONIC', 'INDC', 'INDCON', 'INDIAN', 'INFN', 'INFNTI', 'INTERN', 'IRSB', 'ISEKI', 'ISUZU', 'ITALA', 'IVEC D', 'IVECO', 'J C', 'J C B', 'J DE', 'J DEER', 'JAC', 'JAGUAR', 'JAWA', 'JAYCO', 'JEEP', 'JIAJ', 'JIAJUE', 'JINAN', 'JMC', 'JOHNST', 'JTB', 'K LO', 'K T', 'K T M', 'KALMAR', 'KATO', 'KAWASA', 'KAWASK', 'KAWS', 'KENN', 'KENWOR', 'KENWTH', 'KIA', 'KING', 'KINL', 'KOBELC', 'KOERIN', 'KOMATS', 'KRUEGR', 'KUBOTA', 'KYMC', 'KYMCO', 'L BAGN', 'L MOTO', 'L RO', 'L ROV', 'LADA', 'LAMB G', 'LAN', 'LANCER', 'LANCIA', 'LAND R', 'LANDRO', 'LANRON', 'LARO', 'LARO-D', 'LAVERD', 'LDV', 'LEWCOM', 'LEX', 'LEXUS', 'LEY', 'LIEBHR', 'LIFA', 'LIFAN', 'LINCLN', 'LINER', 'LML', 'LNGHRN', 'LOMB', 'LONC', 'LOTUS', 'LROVER', 'LUCAR', 'LUNA', 'LUSTY', 'M A', 'M A N', 'M C', 'M C A', 'M G', 'M GU', 'M GUZZ', 'M MOVE', 'MACK', 'MACKAY', 'MAHIND', 'MAJOR', 'MANITO', 'MARINR', 'MARSH', 'MARSTA', 'MASER', 'MASS F', 'MATCH', 'MAXC', 'MAXI', 'MAXI T', 'MAXITR', 'MAZ', 'MAZDA', 'MBC', 'MBEL', 'MC L', 'MC LRN', 'MCCOR', 'MCOA', 'MEGE', 'MEGELL', 'MEID', 'MER', 'MERC B', 'MERCBZ', 'MERCED', 'MERLIN', 'MGUZ', 'MISTUB', 'MITSUB', 'MMTB', 'MNI', 'MOBILE', 'MODERN', 'MOFFAT', 'MOFFET', 'MORGAN', 'MORI', 'MORINI', 'MORRIS', 'MOT', 'MOTO G', 'MOTOVE', 'MRT', 'MUSTNG', 'MV A', 'MV AGU', 'MV AUG', 'N HO', 'N HOLL', 'NIS', 'NISSAN', 'NORA', 'NORTON', 'NOT', 'NOT LI', 'ODSSEY', 'OFF', 'OKA', 'OLDS', 'OLIVER', 'OMEGA', 'OPEL', 'OPHEE', 'OTHR', 'OVERLA', 'OZ T', 'P H', 'P M', 'PACIF', 'PACK', 'PAGS', 'PAGSTA', 'PAJE', 'PAKE', 'PARA', 'PEKI', 'PETER', 'PETERB', 'PEU', 'PEUG', 'PEUGEO', 'PGO', 'PIAGGI', 'PICCIN', 'PITP', 'PITPRO', 'PLY', 'POHLNR', 'POLARI', 'PONT', 'PORSCH', 'PRNO', 'PRO', 'PROTON', 'PWM', 'QINGQI', 'R EN', 'R ENF', 'R ROV', 'R W', 'RAM', 'RAMBLR', 'REN', 'RENAUL', 'REPCO', 'RILEY', 'ROAD', 'ROLLS', 'ROVER', 'S J', 'S W M', 'SAAB', 'SACHS', 'SACI', 'SAKAI', 'SAME', 'SAMSNG', 'SCANIA', 'SCHW', 'SCOOTR', 'SCP', 'SEAT', 'SHEN', 'SHER', 'SHERCO', 'SHINER', 'SHINKO', 'SINGER', 'SKAD', 'SKO', 'SKODA', 'SKYT', 'SKYTEA', 'SMART', 'SNOC', 'SNOR', 'SOLO', 'SPAR', 'SSANGY', 'STAND', 'STAR', 'STAT', 'STE', 'STEE', 'STERLI', 'STEVEN', 'STHN', 'STIGA', 'STONEF', 'STUDE', 'SUB', 'SUBARU', 'SUNB', 'SUNBEA', 'SUPR', 'SUZUKI', 'SYDCIT', 'SYM', 'T D', 'T G', 'T G B', 'T T', 'T200', 'TADANO', 'TAG', 'TAGA', 'TALV', 'TATA', 'TAYDUN', 'TAYLOR', 'TDR', 'TECH', 'TEFC', 'TEREX', 'TESL', 'TESLA', 'TGB', 'THE TR', 'THOL', 'THT', 'THUM', 'TIEM', 'TITA', 'TITAN', 'TITI', 'TM', 'TOME', 'TONE', 'TOPSTA', 'TORI', 'TORINO', 'TORO', 'TORPED', 'TOWPAX', 'TOYOTA', 'TRACQP', 'TRAI', 'TRAILE', 'TRAIN', 'TRANTO', 'TRAVEL', 'TRIUM', 'TRIUMP', 'TROLLE', 'TRPLNE', 'TTF', 'U D', 'U D NI', 'ULTIMT', 'UNIVER', 'UNK', 'UNKN', 'UNKW', 'UNSPEC', 'URAL', 'V MO', 'VALIAN', 'VALT', 'VALTRA', 'VAUH', 'VAWD', 'VECT', 'VECTRI', 'VELOC', 'VESPA', 'VIC', 'VICTA', 'VICTOR', 'VICTRY', 'VINC', 'VINCEN', 'VLK', 'VMOTO', 'VOGELE', 'VOL', 'VOLKS', 'VOLKSW', 'VOLVO', 'VW', 'W ST', 'W STAR', 'WACKER', 'WANG', 'WESTER', 'WESTRN', 'WHITE', 'WILLYS', 'WINNEB', 'WIRTGN', 'WOLSE', 'WSTRN', 'XMOT', 'YALE', 'YAMAHA', 'YANMAR', 'YARDMN', 'YIBE', 'YORK', 'YUTONG', 'ZERO', 'ZETOR', 'ZHEJNG', 'ZHONGN', 'ZONG', 'ZZ N']
        # =============================================================================
        #         to add label encoder, very bad formatting
        # =============================================================================



        field_name='VEHICLE_POWER'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        # vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
   
      
        field_name='VEHICLE_TYPE'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        field_name='Vehicle Type Desc'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        grouped_df = vehicle_df_cleaned.groupby(["VEHICLE_TYPE", "Vehicle Type Desc"]).size()
        code=grouped_df.index.get_level_values('VEHICLE_TYPE').tolist()
        description=grouped_df.index.get_level_values('Vehicle Type Desc').tolist()
        # print(code,description)
        field_name='VEHICLE_TYPE'
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
          
        
        
        field_name='VEHICLE_WEIGHT'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name])
   
        
        #check nan
        field_name='CONSTRUCTION_TYPE'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
       
        description= ['Unknown','Articulated', 'Interpretation is not known', 'Rigid']
        # print(code,description)
        
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
          
        
        field_name='FUEL_TYPE'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
        # ['<NA>', 'D', 'E', 'G', 'M', 'O', 'P', 'R', 'S', 'Z']
        description= ['Unknown', 'Diesel', 'Electric', 'Gas','Multi' , 'O','Petrol',
                      'Rotary','S', 'Z']
        
        # print(code,description) 
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
       
        field_name='NO_OF_WHEELS'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        # vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
      
        field_name='NO_OF_CYLINDERS'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        # vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        
        field_name='SEATING_CAPACITY'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        # vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        

        field_name='TARE_WEIGHT'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        # vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
      
        
        field_name='TOTAL_NO_OCCUPANTS'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        # vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
    
       
        field_name='CARRY_CAPACITY'
        vehicle_df_cleaned[field_name]=vehicle_df[field_name].astype(str)
        # vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        
        
        field_name='CUBIC_CAPACITY'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str)
        vehicle_df_cleaned[field_name]=vehicle_df_cleaned[field_name].str.replace(',','')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
         
 
        field_name='FINAL_DIRECTION'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str).str.strip()
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        
       
        field_name='DRIVER_INTENT'
        vehicle_df_cleaned[field_name]=pd.to_numeric(vehicle_df[field_name],errors='coerce')
        mask = pd.to_numeric(vehicle_df_cleaned[field_name]).notnull()
        vehicle_df_cleaned[field_name].loc[mask]=((vehicle_df_cleaned[field_name].loc[mask]).astype(int).round()).astype(str)
        vehicle_df_cleaned[field_name]=vehicle_df_cleaned[field_name].astype(str)
        vehicle_df_cleaned[field_name]=vehicle_df_cleaned[field_name].str.replace('nan', '99')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        
      
        field_name='VEHICLE_MOVEMENT'
        vehicle_df_cleaned[field_name]=pd.to_numeric(vehicle_df[field_name],errors='coerce')
        mask = pd.to_numeric(vehicle_df_cleaned[field_name]).notnull()
        vehicle_df_cleaned[field_name].loc[mask]=((vehicle_df_cleaned[field_name].loc[mask]).astype(int).round()).astype(str)
        vehicle_df_cleaned[field_name]=vehicle_df_cleaned[field_name].astype(str)
        vehicle_df_cleaned[field_name]=vehicle_df_cleaned[field_name].str.replace('nan', '99')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
        #['1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2', '3',
        # '4', '5', '6', '7', '8', '9', '99']
        description= ['Going straight ahead','Parking or unparking','Parked legally', 
                      'Parked illegally', 'Stationary accident','Stationary broken down',
                      'Other stationary','Avoiding animals','Slow/stopping','Out of control',
                      'Wrong way','Turning right','Turning left','Leaving a driveway','"U" turning',
                      'Changing lanes', 'Overtaking','Merging', 'Reversing', 'Not known']
      
        
        
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #   print(cod)
        #   print(self.label_decoder(field_name,cod))
     
        
        field_name='TRAILER_TYPE'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
        # ['<NA>', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']
        description=['No Information','Caravan','Trailer (general)', 'Trailer (boat)',
                     'Horse float', 'Machinery', 'Farm/agricultural equipment',
                     'Not known what is being towed', 'Not applicable', 'Trailer (Exempt)',
                     'Semi Trailer', 'Pig Trailer', 'Dog Trailer']


        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
        
        
        
     
        field_name='VEHICLE_COLOUR_1'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str).str.strip()
        # vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        code=grouped_df.index.to_list()
        # ['BLK', 'BLU', 'BRN', 'CRM', 'FWN', 
        #  'GLD', 'GRN', 'GRY', 'MRN', 'MVE', 
        #  'OGE', 'PNK', 'PUR', 'RED', 'SIL', 'WHI', 
        #  'YLW', 'ZZ']
        # ['<NA>', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']
        description=['Black','Blue','Brown ','Cream', 'Fawn' ,
                     'Gold', 'Green','Grey', 'Maroon','Mauve',
                     'Orange','Pink','Purple','Red','Silver','White',
                     'Yellow', 'Unknown']


        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
        
        
        field_name='VEHICLE_COLOUR_2'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str).str.strip()
        mask=(vehicle_df_cleaned[field_name] =="")
        vehicle_df_cleaned[field_name].loc[mask]='<NA>'
        
        # vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        
 #       ['', 'BLK', 'BLU', 'BRN', 'CRM', 'FWN', 'GLD', 'GRN', 'GRY', 'MRN', 'OGE', 'PNK', 'PUR', 'RED', 'SIL', 'WHI', 'YLW', 'ZZ']
        description=['No Information','Black','Blue','Brown ','Cream', 'Fawn' ,
                     'Gold', 'Green','Grey', 'Maroon','Mauve',
                     'Orange','Pink','Purple','Red','Silver','White',
                     'Yellow', 'Unknown']
        
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
        
            
        
        
        field_name='CAUGHT_FIRE'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        # ['0', '1', '2', '9']
        description=['Not applicable', 'Yes', 'No', 'Not known']
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
        
      
         
        
        field_name='INITIAL_IMPACT'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        # ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '<NA>', 'F', 'N', 'R', 'S', 'T', 'U']
        description=['Towed unit', 'Right front corner', 
                     'Right side forwards', 'Right side rearwards','Right rear corner',
                     'Left front corner', 'Left side forwards', 'Left side rearwards',
                     'Left rear corner','Not known/not applicable','No Information', 
                     'Front', 'None', 'Rear', 'Sidecar', 'Top/roof', 'Undercarriage']
                    

        
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
        
    
      
        field_name='LAMPS'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        # ['0', '1', '2', '9']
        description=['Not applicable', 'Yes', 
                     'No', 'Not known']
        
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
        
        
        field_name='LEVEL_OF_DAMAGE'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        # ['1', '2', '3', '4', '5', '6', '9']
        description=['Minor','Moderate (driveable vehicle)', 'Moderate (unit towed away)',
                     'Major (unit towed away)','Extensive (unrepairable)','Nil damage',
                     'Not known']
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
            

        
        
        field_name='OWNER_POSTCODE'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        

        field_name='TOWED_AWAY_FLAG'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str)
        vehicle_df_cleaned[field_name] = vehicle_df_cleaned[field_name].str.replace(' ', '<NA>')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        code=grouped_df.index.to_list()
        # ['1', '2', '9', '<NA>']
       
        description=['Yes', 'No', 'Not known','Not applicable']
      
        # print(code,description)
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
            
        
        field_name='TRAFFIC_CONTROL'
        vehicle_df_cleaned[field_name]=pd.to_numeric(vehicle_df[field_name],errors='coerce')
        mask = pd.to_numeric(vehicle_df_cleaned[field_name]).notnull()
        vehicle_df_cleaned[field_name].loc[mask]=((vehicle_df_cleaned[field_name].loc[mask]).astype(int).round()).astype(str)
        vehicle_df_cleaned[field_name]=vehicle_df_cleaned[field_name].astype(str)
        vehicle_df_cleaned[field_name]=vehicle_df_cleaned[field_name].str.replace('nan', '99')
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        field_name='Traffic Control Desc'
        vehicle_df_cleaned[field_name]=(vehicle_df[field_name]).astype(str)
        grouped_df=vehicle_df_cleaned.groupby([field_name]).size()
        
        grouped_df=vehicle_df_cleaned.groupby(['TRAFFIC_CONTROL','Traffic Control Desc']).size()
        code=grouped_df.index.get_level_values('TRAFFIC_CONTROL').tolist()
        description=grouped_df.index.get_level_values('Traffic Control Desc').tolist()
        # print(code,description)
        field_name='TRAFFIC_CONTROL'
        self.encoder_label=self.encoder_label.append({'FIELD_NAME':field_name,'CODE_DESCRIPTION':json.dumps(dict(zip(code,description)))}, ignore_index=True)                
        vehicle_df_cleaned[field_name+'_DESC']=[self.label_decoder(field_name,x) for x in tqdm(vehicle_df_cleaned[field_name])]

        # for cod in code :
        #     print(cod)
        #     print(self.label_decoder(field_name,cod))
      
        
        # vehicle_df_cleaned.to_pickle("data/ACCIDENT_CLEANED/vehicle_df_cleaned.pkl")
        vehicle_df_cleaned.to_pickle(self.save_path+"\\"+"vehicle_df_cleaned.pkl")
   
        
        # self.encoder_label.to_pickle("data/ACCIDENT_CLEANED/encoder_label.pkl")
        self.encoder_label.to_pickle(self.save_path+"\\"+"encoder_label.pkl")
   
   

    

if __name__ == '__main__':
    CrashStatPreData=CrashStatPreData(csv_path='data\\ACCIDENT',save_path='data\\ACCIDENT_CLEANED')
    CrashStatPreData.cleanAccidentFiles()
    

