

import os
import sys
import getopt
import pandas as pd
import numpy as np
import sqlite3

DATA_DIR = os.path.join("..","data")


def connect_db(file_path):
    """
    function to connection to aavail database
    INPUT : the file path of the data base
    OUTPUT : the sqlite connection to the database
    """
    ## YOUR CODE HERE
    try:
        conn = sqlite3.connect(file_path)
        print("...successfully connected to db\n")
    except sqlite3.Error as e:
        print("...unsuccessful connection\n",e)
        conn = None

    return(conn)

def ingest_db_data(conn):
    """
    load and clean the db data
    INPUT : the sqlit connection to the databse
    OUPUT : the customer dataframe
    """
    ## YOUR CODE HERE
    query = """ 
    SELECT 
        CUSTOMER.customer_id,
        CUSTOMER.first_name,
        CUSTOMER.last_name,
        CUSTOMER.DOB,
        CUSTOMER.city,
        CUSTOMER.state,
        COUNTRY.country_name,
        CUSTOMER.gender
    FROM CUSTOMER JOIN COUNTRY ON CUSTOMER.COUNTRY_ID = COUNTRY.COUNTRY_ID
    """
    df = pd.read_sql_query(query, conn)
    df_cleaned = df.drop_duplicates(subset=['customer_id'], keep='last')

    return df_cleaned

def ingest_stream_data(file_path):
    """
    load and clean the stream data
    INPUT : the file path of the stream csv
    OUTPUT : the streams dataframe and a mapping of the customers that churned
    """
    ## YOUR CODE HERE
    df_streams = pd.read_csv(file_path)

    df_streams_cleaned = df_streams.dropna(subset=['stream_id'])
    stoped = df_streams_cleaned.groupby("customer_id")['subscription_stopped'].max() == 1
    stoped = stoped.to_dict()


    return df_streams_cleaned, stoped

def process_dataframes(df_db, df_streams, has_churned, conn):
    """
    create the target dataset from the different data imported 
    INPUT : the customer dataframe, the stream data frame, the map of churned customers and the connection to the database
    OUTPUT : the cleaned data set as described question 4
    """
    ## YOUR CODE HERE
    # take only the customers that are in this streams data batch 
    df_combined = df_db[df_db['customer_id'].isin(df_streams['customer_id'].unique())].copy()
    df_combined['is_subscriber'] = ~df_db['customer_id'].map(has_churned).fillna(False)
    df_combined['customer_name'] = df_combined['first_name'] + ' ' + df_combined['last_name']
    df_combined['age'] = pd.to_datetime('today').year - pd.to_datetime(df_combined['DOB']).dt.year

    query2 = """
    SELECT
        invoice_item_id,
        invoice_item
    FROM INVOICE_ITEM
    """
    df_invoices_items = pd.read_sql_query(query2,conn)

    df_invoice_type_tmp = df_streams[['customer_id', 'invoice_item_id']].groupby('customer_id')\
    .agg(lambda x : x.value_counts().index[0]).reset_index()


    # Create mapping dictionary
    item_map = dict(zip(df_invoices_items['invoice_item_id'], df_invoices_items['invoice_item']))

    # Apply mapping to new column
    df_invoice_type_tmp['subscriber_type'] = df_invoice_type_tmp['invoice_item_id'].map(item_map)
    df_combined = df_combined.merge(df_invoice_type_tmp, on='customer_id', how='left')

    df_num_streams= df_streams.groupby('customer_id').size().reset_index(name='num_streams')
    df_combined = df_combined.merge(df_num_streams, on='customer_id', how='left')

    columns= ['customer_id', 'country_name', 'age', 'customer_name', 'is_subscriber', 'subscriber_type', 'num_streams']
    df_combined = df_combined[columns]

    return df_combined




def update_target(target_file, df_clean, overwrite=False):
    """
    write the clean data in a target file located in the working directory.
    Overwrite the existing target file if overwrite is True, otherwise append the clean data to the target file
    INPUT : the name of the target file, the cleaned dataframe and an overwrite flag
    OUPUT : None
    """
    ## YOUR CODE HERE
    mode = 'w' if overwrite else 'a'
    header = overwrite or not os.path.exists(target_file)
    df_clean.to_csv(target_file, mode=mode, header=header, index=False)
    print(f"Data written to {target_file} in {'overwrite' if overwrite else 'append'} mode.")


if __name__ == "__main__":

    ## collect and handle arguments with getopt or argparse
    ## YOUR CODE HERE
    ## collect args
    arg_string = "%s -d db_filepath -s streams_filepath"%sys.argv[0]
    try:
        optlist, args = getopt.getopt(sys.argv[1:],'d:s:')
    except getopt.GetoptError:
        print(getopt.GetoptError)
        raise Exception(arg_string)

    ## handle args
    streams_file = None
    db_file = None
    for o, a in optlist:
        if o == '-d':
            db_file = a
        if o == '-s':
            streams_file = a
    streams_file = os.path.join(DATA_DIR,streams_file)
    db_file = os.path.join(DATA_DIR,db_file)
    target_file = os.path.join(DATA_DIR, "aavail-target.csv")

    ## make the connection to the database
    ## YOUR CODE HERE
    conn = connect_db(db_file)

    ## ingest the data and transform it
    df_db = ingest_db_data(conn)
    df_streams, has_churned = ingest_stream_data(streams_file)
    ## YOUR CODE HERE

    ## write the transformed data to a csv
    ## YOUR CODE HERE
    df_clean = process_dataframes(df_db, df_streams, has_churned, conn)
    update_target(target_file, df_clean, overwrite=False)

