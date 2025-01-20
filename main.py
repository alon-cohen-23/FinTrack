#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 31 18:41:55 2024

@author: aloncohen
"""

import pandas as pd
from sqlalchemy import create_engine
import yaml
from logger import get_logger
from db.create_schema import create_database_schema, delete_database_schema

# set logger
logger = get_logger()

# Load YAML configuration
with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)
    
engine_path = config['engine'] 
engine = create_engine(engine_path) 

def main ():
    logger.info("The pipline started")
    
    create_database_schema()
    logger.info("Created the database schema")
    
    cities_df = pd.read_csv("tables/cities.csv")
    cities_df.to_sql("cities", engine, if_exists="append", index=False)
    logger.info("Uploaded cities table")
    
    zipcodes_df = pd.read_csv("tables/zipcodes.csv", dtype={"zipcode": str})
    zipcodes_df.to_sql("zipcodes", engine, if_exists="append", index=False)
    logger.info("Uploaded zipcodes table")
    
    clients_data_df = pd.read_csv("tables/clients_data.csv", dtype={"zipcode": str})
    clients_data_df.to_sql("clients_data", engine, if_exists="append", index=False)
    logger.info("Uploaded clients_data table")
    
    cards_data_df = pd.read_csv("tables/cards_data.csv")
    cards_data_df.to_sql("cards_data", engine, if_exists="append", index=False)
    logger.info("Uploaded cards_data table")
    
    mcc_codes_df = pd.read_csv("tables/mcc_codes.csv")
    mcc_codes_df.to_sql("mcc_codes", engine, if_exists="append", index=False)
    logger.info("Uploaded mcc_codes table")
    
    merchants_data_df = pd.read_csv("tables/merchants_data.parquet", dtype={"zipcode": str})
    merchants_data_df.to_sql("merchants_data", engine, if_exists="append", index=False)
    logger.info("Uploaded merchants_data table")
    
    transactions_df = pd.read_parquet("tables/transactions.parquet")
    transactions_df.to_sql("transactions", engine, if_exists="append", index=False)
    logger.info("Uploaded transactions table")
    
    fraud_labels_df = pd.read_parquet("tables/fraud_labels.parquet")
    fraud_labels_df.to_sql("fraud_labels", engine, if_exists="append", index=False)
    logger.info("Uploaded fraud_lables table")
    
    logger.info("The pipline Ended, enjoy your new database")
    
if __name__ == "__main__":
    main()
    
    
    
    
    
    
    
    
    
    
    
    