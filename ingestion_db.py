import pandas as pd
import os
from sqlalchemy import create_engine
import time
import logging

logging.basicConfig(
    filename="logs/.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    """
    Ingests a DataFrame into the database as a table, 
    replacing it if it already exists.
    """
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    """
    Loads all CSV files from the 'data' folder into the database 
    and logs ingestion time.
    """
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join('data', file))
            logging.info(f"Ingesting {file} into database...")
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start) / 60
    logging.info("--------- Ingestion Complete ---------")
    logging.info(f"Total time taken: {total_time:.2f} minutes")

if __name__ == '__main__':
    load_raw_data()
