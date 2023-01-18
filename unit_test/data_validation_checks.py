"""
Import necessary modules
############################################################################## 
"""


import pandas as pd
import os
import sqlite3
from sqlite3 import Error

###############################################################################
# Define function to validate raw data's schema
# ############################################################################## 

def raw_data_schema_check(DB_PATH, DB_FILE_NAME, raw_data_schema):
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    print ("Connecting to the database")
    cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    print ("Reading data from the loaded_data")
    loaded_data = pd.read_sql('select * from loaded_data', cnx)
    loaded_data_columns = list(loaded_data.columns)
    print("Length of the loaded_data column is : ", len(loaded_data_columns))
    print("Length of the raw_data_schema length is : ", len(raw_data_schema))
    
    column_masmatch = 0
    for col in raw_data_schema:
        if col not in loaded_data_columns:
            print("column mismatch: ", col)
            column_masmatch = 1
            
    if column_masmatch == 0:
        print("Raw datas schema is in line with the  schema.py")
    else:
        print("Raw datas schema is NOT in line with the schema.py")
    
    print("Closing the connection")    
    cnx.close()


###############################################################################
# Define function to validate model's input schema
# ############################################################################## 

def model_input_schema_check(DB_PATH, DB_FILE_NAME, raw_data_schema):
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    print ("Connecting to the database")
    cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    print ("Reading the data from the interactions_mapped")
    interactions_mapped = pd.read_sql('select * from interactions_mapped', cnx)
    interactions_mapped_columns = list(interactions_mapped.columns)
    print("Length of the interactions_mapped column is : ", len(interactions_mapped_columns))
    print("Length of the raw_data_schema is : ", len(raw_data_schema))
    
    column_masmatch = 0
    for col in raw_data_schema:
        if col not in interactions_mapped_columns:
            print("column mismatch: ", col)
            column_masmatch = 1
            
    if column_masmatch == 0:
        print("Models input schema is in line with schema.py")
    else:
        print("Models input schema is NOT in line with schema.py")
    
    print("Closing the connection") 
    cnx.close()

    
    
