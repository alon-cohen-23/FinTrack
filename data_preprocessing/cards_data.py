#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 12:45:30 2024

@author: aloncohen
"""

import pandas as pd
from logger import get_logger


# set logger
logger = get_logger()

def transform_cards_data(cards_data_df_path):
    cards_data_df = pd.read_csv(cards_data_df_path)
    
    # Remove the first character from every value in the 'credit_limit' column
    cards_data_df['credit_limit'] = cards_data_df['credit_limit'].str.slice(1).astype("int64")
    cards_data_df = cards_data_df.rename(columns={'credit_limit': 'credit_limit_in_usd'})
    
    
    cards_data_df['expires'] = pd.to_datetime(cards_data_df['expires'], format='%m/%Y')
    cards_data_df = cards_data_df.rename(columns={'expires': 'expiration_date'})
    
    cards_data_df["acct_open_date"] = pd.to_datetime(cards_data_df["acct_open_date"])
    cards_data_df["year_pin_last_changed"] = cards_data_df["year_pin_last_changed"].astype(int)
    
    cards_data_df['has_chip'] = cards_data_df['has_chip'].map({'YES': True, 'NO': False}).astype(bool)
    cards_data_df['card_on_dark_web'] = cards_data_df['card_on_dark_web'].map({'YES': True, 'NO': False}).astype(bool)
    
    cards_data_df['card_number'] = cards_data_df['card_number'].astype("str")
    
    
    
    # Convert all values in the 'cvv' column to strings and add leading zeros for values < 100
    cards_data_df['cvv'] = cards_data_df['cvv'].apply(
        lambda x: str(x).zfill(3) if x < 100 else str(x)
    )
    
    cards_data_df.to_csv("../tables/cards_data.csv", index=False)
    logger.info("saved cards_data to disk")



if __name__ =='__main__':

    cards_data_df_path = "../archive/cards_data.csv"
    transform_cards_data(cards_data_df_path)
    
