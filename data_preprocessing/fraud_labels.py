#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 15:24:49 2025

@author: aloncohen
"""
import pandas as pd
from logger import get_logger

logger = get_logger()


def transform_fraud_labels(fraud_label_df_path):
    
    
    df = pd.read_parquet(fraud_label_df_path)
    # Rest index and rename columns
    df = df.reset_index()
    df.rename(columns={'index': 'transaction_id', 'target': 'fraud'}, inplace=True)
   
    # convert fraud column to boolean
    df['fraud'] = df['fraud'].map({'Yes': True, 'No': False})
    df['fraud'] = df['fraud'].astype('bool')
    
    
    df.to_parquet('../tables/fraud_labels.parquet', index=False)
    logger.info("saved fraud_lables to disk")
    
if __name__ == '__main__':
    fraud_label_df_path = "../archive/train_fraud_labels.parquet"
    transform_fraud_labels(fraud_label_df_path)      