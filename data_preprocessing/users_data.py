#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 12:45:11 2024

@author: aloncohen
"""


from geopy.geocoders import Nominatim     
from geopy.exc import GeocoderTimedOut
import time
import pandas as pd
import math
from logger import get_logger

# set logger
logger = get_logger()

def get_zipcodes_from_cordinates (geolocator ,latitude, longitude, retries=3, delay=1):
    """
    Gets the latitude and the longitude of each client and return the correct zipcode
    for each one by calling the geopy API. If the API fails to find a zipcode than it returns 0
    as the zipcode and it will be fixed later with the find_zipcode_to_cordinates_by_distance function. 

    """
    for _ in range(retries):
        try:
            location = geolocator.reverse((latitude, longitude), language='en')
            address = location.raw['address']
            zipcode = address.get('postcode','0')
            return zipcode
            
        except GeocoderTimedOut:
            logger.info("Request timed out, retrying...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff
    logger.info(f"Max retries exceeded for {latitude, longitude}")
    return None

def convert_cordinates_to_zipcodes (users_data_df, geolocator_app_name = "my_application", timeout=10):
    """
    Apply the get_zipcodes_from_cordinates function across the whole dataframe.
    """
    
    logger.info("Finding the zipcode for the latitude and longitude of the clients")
    geolocator = Nominatim(user_agent=geolocator_app_name, timeout=timeout)
    
    zipcodes_list = []
    for index, row in users_data_df.iterrows():
        zipcode = get_zipcodes_from_cordinates(geolocator, row['latitude'], row['longitude'])
        zipcodes_list.append(zipcode)
        if index%100 == 0:
            logger.info(f"We passed {index} clients, saving temporary df to disk")
            temp_df = users_data_df.head(index+1).copy()
            temp_df['zipcode']= zipcodes_list
            temp_df.to_csv("tables/clients_data.csv", index=False)
            
            
    users_data_df['zipcode'] = zipcodes_list    
    users_data_df['zipcode'] = users_data_df['zipcode'].astype(str)
    return users_data_df

def haversine(lat1, lng1, lat2, lng2):
    """
    Calculate the distance between two cordinates that contains (lat,lng) of a location.
    """
    
    # Convert latitude and longitude from degrees to radians
    lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
    
    # Differences in coordinates
    dlat = lat2 - lat1
    dlon = lng2 - lng1
    
    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    # Radius of the Earth in kilometers
    r = 6371.0
    
    # Calculate the distance
    distance = r * c
    return distance

def find_zipcode_to_cordinates_by_distance (df, lat, lng):
    min_distance = float('inf')
    postcode = ""
    for index, row in df.iterrows():
        distance = haversine(lat, lng, row['lat'], row['lng'])
        if distance < min_distance and distance!= 0:
            min_distance = distance
            postcode = row['zip']
    return postcode

def transform_clients_data (users_data_df_path, zips_df_path):
    """
    Transform the users_data table into the clients_data table and save it as a csv.
    """
    users_data_df = pd.read_csv(users_data_df_path)
    # Create birth_ym column   
    users_data_df['birth_ym'] =  users_data_df.apply(
        lambda row: str(row['birth_year'])+'-'+str(row['birth_month']), axis = 1) 
    users_data_df['birth_ym'] = pd.to_datetime(users_data_df['birth_ym'], format='%Y-%m')    
       
    # convert cordinates to zipcodes
    users_data_df = convert_cordinates_to_zipcodes (users_data_df, geolocator_app_name = "my_application")
    
    zips_df = pd.read_csv(zips_df_path, dtype={"zip": str})
    
    users_data_df['zipcode'] = users_data_df['zipcode'].astype(str).str.zfill(5)
    
    filtered_rows = users_data_df[~users_data_df['zipcode'].isin(zips_df['zip'])] # filter the rows that does not have a matching postcode
    users_data_df = users_data_df[users_data_df['zipcode'].isin(zips_df['zip'])] 
    
    # get the correct postcodes 
    target_coordinates = list(zip(filtered_rows['latitude'], filtered_rows['longitude']))
    results = [find_zipcode_to_cordinates_by_distance(zips_df, longitude, longitude) for latitude, longitude in target_coordinates]
    
    # get the postcodes to filtered_rows and concat them back to the users_data_df
    filtered_rows['zipcode'] = results
    users_data_df = pd.concat([users_data_df, filtered_rows])
    
    
    # convert column type to int 
    users_data_df['per_capita_income'] = users_data_df['per_capita_income'].str.slice(1).astype("int64") 
    users_data_df['yearly_income'] = users_data_df['yearly_income'].str.slice(1).astype("int64") 
    users_data_df['total_debt'] = users_data_df['total_debt'].str.slice(1).astype("int64") 
    
    # rename the columns
    users_data_df = users_data_df.rename(columns={'per_capita_income': 'per_capita_income_in_usd',
                                                  'yearly_income':'yearly_income_in_usd',
                                                  'total_debt':'total_debt_id_usd'}) 
    
    columns = ['id', 'birth_ym','gender' ,'retirement_age','address','zipcode', 'per_capita_income_in_usd',
                   'yearly_income_in_usd', 'total_debt_id_usd' ,'credit_score', 'num_credit_cards']
    users_data_df = users_data_df[columns]
    
    
    users_data_df.to_csv("../tables/clients_data.csv", index=False)
    logger.info("Saved clients_data to disk")


if __name__ == '__main__':
    
    zips_df_path = "../archive/uszips.csv"
    users_data_df_path = "../archive/users_data.csv"
    transform_clients_data(zips_df_path, users_data_df_path)    
    
 


