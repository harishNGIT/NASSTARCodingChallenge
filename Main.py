import HelperFunctions.HelperFunctions as f
import pandas as pd
import numpy as np
import os
import sys 
import sqlite3
from datetime import datetime
import logging
from multiprocessing import get_logger, log_to_stderr
from concurrent.futures import ProcessPoolExecutor
import glob 
from sqlalchemy import create_engine,text  
import urllib
def main():  
  

    # logging.info("Data processing starts.")
    # Mapping dictionary to hold which group containes which turbine Ids
    turbineMappingDic={"data_group_1":[1,2,3,4,5],"data_group_2":[6,7,8,9,10],"data_group_3":[11,12,13,14,15]}
    data_Dir="./raw"
    # last_load_time = get_last_load_time() 
    file_list = glob.glob(data_Dir + "/*.csv") 
    result=f.process_files_concurrent(file_list,turbineMappingDic, max_workers=None)
    print(result)

if __name__ == "__main__":
    main()