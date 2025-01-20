#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 12:46:03 2024

@author: aloncohen
"""

import pandas as pd
import numpy as np
import gc
from logger import get_logger

# set logger
logger = get_logger()




def transform_mcc_codes(mcc_codes_path):
    # process mcc_codes table
    import json
    
    # Open and inspect the JSON file
    with open(mcc_codes_path, "r") as file:
        data = json.load(file)
    mcc_codes_df = pd.DataFrame(list(data.items()), columns=["mcc_code", "description"])
    mcc_codes_df['mcc_code'] = mcc_codes_df['mcc_code'].astype(int)
    mcc_codes_df['description'] = mcc_codes_df['description'].astype(str)
    
    mcc_codes_df.to_csv('tables/mcc_codes.csv', index=False) # export to csv file
    logger.info("Saved mcc_codes to disk")


def transform_merchants_and_transactions (transactions_data_df_path, zips_df_path):

    transactions_df = pd.read_parquet(transactions_data_df_path)
        
    merchants_columns = ['merchant_id','mcc', 'zip']
    merchants_df = transactions_df[merchants_columns] # export the rellevat columns to the merchants_df    
              
    transactions_columns = ['id','date','client_id','card_id','merchant_id','amount','use_chip','errors']
    transactions_df = transactions_df[transactions_columns] 
        
    logger.info("Created merchant_df succesfuly")
    logger.info("Created transactions_df succesfuly")
    
    # Process the merchants df
    merchants_df = merchants_df.drop_duplicates(subset='merchant_id') # drop duplicates based on id
    
    merchants_df['mcc'] = merchants_df['mcc'].astype(int) # change column type to int
    
    merchants_df = merchants_df.rename(columns={'merchant_id': 'id','mcc':'mcc_code','zip':'zipcode'}) # rename columns
    
    # convert non-finite values to 0 in order to change the column type 
    merchants_df['zipcode'] = merchants_df['zipcode'].replace([np.inf, -np.inf, np.nan], 0)
    
    
    # Replace non-valid poscode with a random one from the zipcodes df
    zips_df = pd.read_csv(zips_df_path, dtype={"zip": str})
    
    merchants_df['zipcode'] = merchants_df['zipcode'].astype(int).astype(str).str.zfill(5) # make sure the zipcodes are indeed 5 charecters
    
    filtered_df = merchants_df[~merchants_df['zipcode'].isin(zips_df['zip'])]
    merchants_df = merchants_df[merchants_df['zipcode'].isin(zips_df['zip'])]
    
    filtered_df['zipcode'] = np.random.choice(zips_df['zip'], size=len(filtered_df))
    merchants_df = pd.concat([merchants_df,filtered_df])  
    
    merchants_df.to_csv('../tables/merchants_data.parquet', index=False) # export to csv file
    logger.info("Saved merchants_data to disk")
    
    # process the transactions df
    transactions_df['date'] = pd.to_datetime(transactions_df['date']) # change column type to datetime 
    
    transactions_df['amount'] = transactions_df['amount'].str.slice(1).astype("float64") # change column type to float 
    transactions_df = transactions_df.rename(columns={'amount': 'amount_in_usd'}) # rename the amount column
    
    # change column types to string
    transactions_df['use_chip'] = transactions_df['use_chip'].map({'YES': True, 'NO': False}).astype(bool)
    transactions_df['errors'] = transactions_df['errors'].astype(str)
    
    transactions_df.to_parquet('../tables/transactions.parquet', index=False) # export to csv file
    logger.info("Saved transactions to disk")
    
    
    # delete the df from memory
    del merchants_df
    del transactions_df
    gc.collect()    

if __name__ == '__main__':
    transactions_df_path = "../archive/transactions_data.parquet"
    mcc_codes_path = "../archive/mcc_codes.json"
    zips_df_path = "../archive/uszips.csv"
    
    transform_mcc_codes(mcc_codes_path)
    transform_merchants_and_transactions(transactions_df_path, zips_df_path)
    









