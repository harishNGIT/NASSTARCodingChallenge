import pandas as pd
import numpy as np
import os
import sqlite3
from datetime import datetime
import logging
from multiprocessing import get_logger, log_to_stderr
from concurrent.futures import ProcessPoolExecutor
import glob 
from sqlalchemy import create_engine,text  
import urllib

# Impute within each turbine group

# Configuration
DATABASE_FILE = "wind_turbine_data.db"
DATA_DIR = "./data"
OUTLIER_THRESHOLD = 3  # Threshold for removing outliers (in standard deviations)
ANOMALY_THRESHOLD = 2  # Threshold for detecting anomalies (in standard deviations)
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=LAPTOP-MBV00UBE\\SQLEXPRESS;"
    "DATABASE=test;"
    "Trusted_Connection=yes;"
)
QUOTED_CONNECTION_STRING = urllib.parse.quote_plus(CONNECTION_STRING)

# Use the connection string with SQLAlchemy
DATABASE_URL = f"mssql+pyodbc:///?odbc_connect={QUOTED_CONNECTION_STRING}"

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL) 


def initializeLogging():

    '''Function to  Intialize logging '''

    log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    log_file = "pipeline.log"

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)
    stream_handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = []  # Clear existing handlers to avoid duplicates
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)


def getLastLoadTime(groupName):
    ''' Function to get lastLoadedTimestamp'''
    try:
        with engine.connect() as conn:
            query = text(f"SELECT TOP 1 lastLoadedTimeStamp FROM audit.watermark WHERE groupName='{groupName}' ORDER BY UpdatedTimeStamp DESC")
            result = conn.execute(query).fetchone()
            if result:
                return result[0]  # Return the last load timestamp
            else:
                return None  # No record found, first load
    except Exception as e:
        logging.error(f"Error retrieving last load time: {e}")
        assert False



def updateLastLoadTime(groupName,timestamp):
    ''' Function to Update timestamp'''
    try:
        with engine.connect() as conn:
            with conn.begin(): 
                query = text(f"INSERT INTO audit.watermark (groupName,lastLoadedTimeStamp) VALUES ('{groupName}','{timestamp}')")
                print(query)
                conn.execute(query)
                print(f"Last load time updated to {timestamp}.")
    except Exception as e:
        logging.error(f"Error updating last load time: {e}")
        assert False
  

def LoadAndcleanData(filepath,gropuName):
    ''' Funtion to Load and Clean Data'''
    try:
       df=pd.read_csv(filepath)
    #    print(df)
       last_load_time = getLastLoadTime(gropuName)
       df['timestamp'] = pd.to_datetime(df['timestamp'])
       if   last_load_time:
            # Filter records that have a timestamp greater than the last load timestamp
            # df['data_timestamp'] = pd.to_datetime(df['data_timestamp'])  # Convert to datetime if not already
            df = df[df['timestamp'] > last_load_time]
            logging.info(f"Loaded {len(df)} new records after {last_load_time} for group {gropuName}")

       else:
            logging.info(f"No last load time found. Loading all data for group {gropuName}")    
        
        
        # Step 1: Calculate group means excluding NaNs
       turbineMeans = df.groupby('turbine_id')['power_output'].transform('mean')   
           # Step 2: Fill missing values with precomputed group means
       df['power_output'] = df['power_output'].fillna(turbineMeans)
       # Remove outliers (values beyond OUTLIER_THRESHOLD standard deviations)
       mean = df['power_output'].mean()
       std = df['power_output'].std()
       df = df[(df['power_output'] >= mean - OUTLIER_THRESHOLD * std) & (df['power_output'] <= mean + OUTLIER_THRESHOLD * std)]
       return df 
    except Exception as e:
        logging.error(f"Error in cleaning data {gropuName}: {e}")
        assert False


def ComputeStats(df,period,groupName):
    ''' Funtion to compute statastics'''

    try:
        
        df['period'] = df['timestamp'].dt.to_period(period)
        stats = df.groupby(['turbine_id','period']).agg({
        'power_output': ['min', 'max', 'mean']}).reset_index()
        stats.columns = ['turbine_id', 'period', 'min_power_output', 'max_power_output', 'mean_power_output']
        stats['period'] = stats['period'].dt.start_time
        return stats
    except Exception as e:
        logging.error(f"Error in computeStats data {groupName}: {e}")
        assert False



def IdentifyAnamolies(df,groupName):
    '''Function to Identify anamolies in the data'''
    try:
        mean_power = df.groupby('turbine_id')['power_output'].transform('mean')
        std_power = df.groupby('turbine_id')['power_output'].transform('std')
        
        # Calculate the anomaly condition
        lower_bound = mean_power - ANOMALY_THRESHOLD * std_power
        upper_bound = mean_power + ANOMALY_THRESHOLD * std_power
        df['is_anomaly'] = (df['power_output'] < lower_bound) | (df['power_output'] > upper_bound) 
        return df
    except Exception as e:
        logging.error(f"Error in IdentifyAnamolies data {groupName}: {e}")
        assert False


def processData(filePath,turbineMappingDic):
    '''Function to process data'''
    try:
        initializeLogging()
        groupName=filePath.replace("./raw\\",'').split('.')[0]
        print(filePath,groupName)
        logging.info(f"{groupName} Data Load And Cleaning starts.")
        df=LoadAndcleanData(filePath,groupName)
        if df.empty:
            logging.info(f"No New data for {groupName}")
        else:
        # print(df)
            logging.info(f"{groupName} Data Load And Cleaning Completed.")
            logging.info(f"{groupName} Compute stats started.")
            statsdf=ComputeStats(df,'D',groupName)
            # print(statsdf)
            logging.info("Compute stats Completed.")
            anamolyDf=IdentifyAnamolies(df,groupName)
            anamolyDf=anamolyDf.drop(columns="period")
            

            logging.info(f"{groupName} turbine_data wrirting to database Started")
            anamolyDf.to_sql('turbine_data', con=engine, if_exists='append', index=False)
            logging.info(f"{groupName} turbine_data wrirting to database completed")

            logging.info(f"{groupName} turbine_statastics wrirting to database Started")
            statsdf.to_sql('turbine_statastics', con=engine, if_exists='append', index=False)
            logging.info(f"{groupName} turbine_statastics wrirting to database completed")

            turbines_in_data = df['turbine_id'].unique()
            expected_turbines=turbineMappingDic[groupName]
            # Find the missing turbines
            missing_turbines = list(set(expected_turbines) - set(turbines_in_data))
            if missing_turbines:
                logging.info(f"missing Turbines in {groupName}:{missing_turbines}")

            UpdatedTimestamp=df['timestamp'].max()
            print(UpdatedTimestamp)
            updateLastLoadTime(groupName,UpdatedTimestamp)
            return True
    except Exception as e:
        logging.error(f"Error in IdentifyAnamolies data {groupName}: {e}")
        assert False


def process_files_concurrent(file_paths,turbineMappingDic, max_workers=None):
    '''Function to Load Files Cocurrently'''
# Default to the number of available CPUs if max_workers is not set
    if max_workers is None:
        max_workers = len(file_paths)  # Limit to the number of files or CPUs
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks and collect results
                futures = [
                executor.submit(processData, file_path, turbineMappingDic)
                for file_path in file_paths
            ]
                results = [future.result() for future in futures]
        
    
    return results
