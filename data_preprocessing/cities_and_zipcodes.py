#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 12:49:06 2024

@author: aloncohen
"""

import pandas as pd
from logger import get_logger

# set logger
logger = get_logger()

def transform_cities_and_zipcodes (zips_df_path):
    
    zips_df = pd.read_csv(zips_df_path, dtype={"zip": str})
    # Filter null or empty values
    zips_df = zips_df[zips_df["zip"].notna() & (zips_df["zip"].str.strip() != '')]
    zips_df = zips_df[zips_df["city"].notna() & (zips_df["city"].str.strip() != '')]
    
    zips_df['city'] = zips_df['city'].str.strip().str.title()
    
    # create the city df and add city_id column
    cities_df = zips_df.copy()
    cities_df = cities_df.drop_duplicates(subset='city')
    cities_df = cities_df.reset_index(drop=True).reset_index()
    cities_df = cities_df.rename(columns={'index':'id'})
    
    # Create a mapping dictionary from cities_df
    city_to_id = dict(zip(cities_df['city'], cities_df['id']))
    
    # Map city_id to df2 using the mapping dictionary
    zips_df['city_id'] = zips_df['city'].map(city_to_id)
    
    zips_df = zips_df.rename(columns={'zip': 'zipcode'})
    zips_df = zips_df[['zipcode','lat','lng','city_id']]
    
    
    #rename state and county columns
    cities_df = cities_df.rename(columns={'county_name': 'county', 'state_name': 'state'})
    cities_df = cities_df[['id','city','county','state','population','density']]
    
    
    cities_df.to_csv("../tables/cities.csv", index=False)
    logger.info("saved cities to disk")
    
    zips_df.to_csv("../tables/zipcodes.csv", index=False)
    logger.info("saved zipcodes to disk")
    
if __name__ == '__main__':
    zips_df_path = "../archive/uszips.csv"
    transform_cities_and_zipcodes(zips_df_path)
    
    
    
