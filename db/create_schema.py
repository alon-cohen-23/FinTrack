#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 23:51:52 2024

@author: aloncohen
"""

import psycopg2
import yaml
from logger import get_logger
from pathlib import Path

# set logger
logger = get_logger()

# set YAML path
current_file = Path(__file__)
repo_root = current_file.resolve().parent.parent

config_path = repo_root / "config.yaml"

# Load YAML configuration
with open(config_path, 'r') as config_file:
    config = yaml.safe_load(config_file)

database_conn = config['database_conn']    

def execute_query (sql_query):
    # Connect to the database
    conn = psycopg2.connect(**database_conn)
    cur = conn.cursor()
    
    # Execute the query
    cur.execute(sql_query)
    
    # Commit the changes
    conn.commit()
    
    # Close the cursor and connection
    cur.close()
    conn.close()
    
    logger.info("The query was executed succesfuly")

def load_csv_to_table (csv_path, table_name):
    # Connect to the database
    conn = psycopg2.connect(**database_conn)
    cur = conn.cursor()
    # Use the COPY command to load the CSV data into the table
    with open(csv_path, "r") as file:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", file)
    
    conn.commit()
    logger.info("Loaded csv successfully")    
    cur.close()
    conn.close()
    

# SQL statement to create the table if it doesn't exist
create_cities_table = """
CREATE TABLE IF NOT EXISTS cities (
    id BIGINT PRIMARY KEY,
    city VARCHAR(50),
    county VARCHAR(50),
    state VARCHAR(50),
    population BIGINT,
    density DOUBLE PRECISION
);
"""

create_zipcodes_table = """
CREATE TABLE IF NOT EXISTS zipcodes (
    zipcode VARCHAR(5) PRIMARY KEY,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    city_id BIGINT,
    FOREIGN KEY (city_id) REFERENCES cities (id) ON DELETE CASCADE
);
"""

create_clients_data_table = """
CREATE TABLE IF NOT EXISTS clients_data (
    id BIGINT PRIMARY KEY,
    birth_ym TIMESTAMP,
    gender VARCHAR(10),
    retirement_age INTEGER,
    address VARCHAR(50),
    zipcode VARCHAR(5),
    FOREIGN KEY (zipcode) REFERENCES zipcodes (zipcode) ON DELETE CASCADE,
    per_capita_income_in_usd BIGINT,
    yearly_income_in_usd BIGINT,
    total_debt_id_usd BIGINT,
    credit_score INTEGER,
    num_credit_cards INTEGER
);
"""


# SQL statement to create the table if it doesn't exist
create_cards_data_table = """
CREATE TABLE IF NOT EXISTS cards_data (
    id BIGINT PRIMARY KEY,
    client_id BIGINT,
    FOREIGN KEY (client_id) REFERENCES clients_data (id) ON DELETE CASCADE,
    card_brand VARCHAR(20),
    card_type VARCHAR(20),
    card_number VARCHAR(16),
    expiration_date TIMESTAMP,
    cvv VARCHAR (3),
    CONSTRAINT only_digits CHECK (cvv ~ '^\d+$'),
    has_chip BOOLEAN,
    num_cards_issued INTEGER,
    credit_limit_in_usd BIGINT,
    acct_open_date TIMESTAMP CHECK (acct_open_date <= CURRENT_TIMESTAMP),
    year_pin_last_changed INTEGER CHECK (year_pin_last_changed >= EXTRACT(YEAR FROM acct_open_date)),
    card_on_dark_web BOOLEAN
);
"""

create_mcc_codes_table = """
CREATE TABLE IF NOT EXISTS mcc_codes (
    mcc_code INTEGER PRIMARY KEY,
    description VARCHAR(100)
);
"""


# SQL statement to create the table if it doesn't exist
create_merchants_data_table = """
CREATE TABLE IF NOT EXISTS merchants_data (
    id BIGINT PRIMARY KEY,
    mcc_code INTEGER,
    FOREIGN KEY (mcc_code) REFERENCES mcc_codes (mcc_code) ON DELETE CASCADE, 
    zipcode VARCHAR(5),
    FOREIGN KEY (zipcode) REFERENCES zipcodes (zipcode) ON DELETE CASCADE
);
"""

# SQL statement to create the table if it doesn't exist
create_transactions_table = """
CREATE TABLE IF NOT EXISTS transactions (
    id BIGINT PRIMARY KEY,
    date TIMESTAMP,
    client_id BIGINT,
    card_id BIGINT,
    merchant_id BIGINT, 
    amount_in_usd BIGINT,
    FOREIGN KEY (client_id) REFERENCES clients_data (id) ON DELETE CASCADE,
    FOREIGN KEY (merchant_id) REFERENCES merchants_data (id) ON DELETE CASCADE,
    FOREIGN KEY (card_id) REFERENCES cards_data (id) ON DELETE CASCADE,
    use_chip BOOLEAN,
    errors VARCHAR(60)
);
"""
create_fraud_labels_table = """
CREATE TABLE IF NOT EXISTS fraud_labels (
    transaction_id BIGINT PRIMARY KEY,
    fraud BOOLEAN NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
);   
"""

delete_all_data = """
DELETE FROM transactions, merchants_data, mcc_codes, cards_data, clients_data, zipcodes, cities, fraud_labels;
"""

drop_all_tables = """
DROP TABLE IF EXISTS transactions, merchants_data, mcc_codes, cards_data, clients_data, zipcodes, cities, fraud_labels;
"""


def create_database_schema ():
    # The function creates all of the tables needed in the database
    execute_query (create_cities_table)
    execute_query (create_zipcodes_table)
    execute_query (create_clients_data_table)
    execute_query (create_cards_data_table)
    execute_query (create_mcc_codes_table)
    execute_query (create_merchants_data_table)
    execute_query (create_transactions_table)
    execute_query (create_fraud_labels_table)
    logger.info("Schema was created succesfuly")
    
def delete_database_schema ():
    execute_query(drop_all_tables)

def delete_all_tables_data():
    execute_query(delete_all_data)

if __name__ == '__main__':
    print (config_path)

   

    