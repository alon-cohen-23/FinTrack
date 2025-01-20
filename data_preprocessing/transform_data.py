
from db.create_schema import create_database_schema, delete_database_schema
from logger import get_logger
from cities_and_zipcodes import transform_cities_and_zipcodes
from users_data import transform_clients_data
from cards_data import transform_cards_data
from merchants_and_transactions import transform_mcc_codes, transform_merchants_and_transactions
from fraud_labels import transform_fraud_labels

# set logger
logger = get_logger()

def main (zips_df_path, users_data_df_path, cards_data_df_path, mcc_codes_path, transactions_data_df_path, fraud_lables_path):
    """
   This function is responsible for executing all defined transformations
   on the database tables. 
    """   
    
    logger.info("Strating data preprocessing, it will take 15-20 minutes.")
    
    # transform and load the database tables
    transform_cities_and_zipcodes(zips_df_path)
    transform_clients_data(users_data_df_path, zips_df_path)
    transform_cards_data(cards_data_df_path)
    transform_mcc_codes(mcc_codes_path)
    transform_merchants_and_transactions(transactions_data_df_path, zips_df_path)
    transform_fraud_labels(fraud_labels_path)
    logger.info("Ended data preprocessing successfully.")
    
    
if __name__ =='__main__':
    zips_df_path = "../archive/uszips.csv"
    mcc_codes_path = "../archive/mcc_codes.json"
    fraud_labels_path = "../archive/train_fraud_labels.parquet"
    transactions_data_df_path = "../archive/transactions_data.parquet"
    users_data_df_path = "../archive/users_data.csv"
    cards_data_df_path = "../archive/cards_data.csv"
    main (zips_df_path, users_data_df_path, cards_data_df_path,
          mcc_codes_path, transactions_data_df_path, fraud_labels_path)
    

    
    
   
    
    