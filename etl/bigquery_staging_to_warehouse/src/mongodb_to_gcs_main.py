
# coding: utf-8

# In[76]:

import sys
from datetime import datetime
import time
import json
import os

from mongodb_to_gcs_utils import get_last_load_index, upload_file_to_gcs, delete_gcs_object
from mongo import mongodb_connect
from bigquery_merge_table import run_job_main
from normalizer import sale_order_nomalizer

def run(
        main_dataset_name,
        staging_dataset_name,
        table_name,
        last_load_column_name,
        mongodb_db_name,
        mongodb_collection_name,
        merge_job_name,
        config={}
    ):

    # Get data
    last_updated_timestamp = get_last_load_index(
        main_dataset_name=main_dataset_name,
        staging_dataset_name=staging_dataset_name,
        table_name=table_name,
        column_name=last_load_column_name,
        gcs_source="gs://{staging_dataset_name}/mongodb/{mongodb_db_name}/{mongodb_collection_name}/*".format(
            staging_dataset_name = staging_dataset_name,
            mongodb_db_name = mongodb_db_name,
            mongodb_collection_name = mongodb_collection_name
        )
    )
    print('Last load timestamp {} = {}'.format(last_load_column_name, str(last_updated_timestamp)))

    # Connect to MongoDB

    collection = mongodb_connect(mongodb_db_name, mongodb_collection_name)

    # Extract data from mongodb

    sale_orders = collection.find({last_load_column_name: {'$gte': last_updated_timestamp}})

    print('Found ' + str(sale_orders.count()) + ' record(s) in the return data')

    # Write data to file
    file_name = '{mongodb_db_name}_{mongodb_collection_name}_'.format(mongodb_db_name=mongodb_db_name, mongodb_collection_name=mongodb_collection_name) \
                + str(int(time.time())) + '.json'
    file_path = os.path.dirname(os.path.abspath(__file__)) + '/' + file_name

    # Open file
    file = open(file_path,'w')

    for order in sale_orders:
        # De-duplication by checking that the updated time of the record is not <= the MAX last load time
        if config['<some specific attribute here, passed from the config object'] == 1:
            json_text = json.dumps(sale_order_nomalizer(order))
        else:
            json_text = json.dumps(order)
        file.write(json_text + '\n') 
        
    file.close()
    print('Write data to file ' + file_path)

    # Prepare to upload file

    gcs_file_key = 'mongodb/{mongodb_db_name}/{mongodb_collection_name}/{file_name}'.format(
        mongodb_db_name=mongodb_db_name,
        mongodb_collection_name=mongodb_collection_name,
        file_name=file_name
    ) 
    print(upload_file_to_gcs(local_file_path=file_path, gcs_file_key=gcs_file_key))

    if merge_job_name != "":
        try:
            merge_result = run_job_main(merge_job_name)
            if merge_result['status'] == 'done':
                # Delete uploaded file
                delete_gcs_object(gcs_file_key)
                print('MERGE success, delete file from GCS ' + gcs_file_key)
                
                # Delete file on local machine
                os.remove(file_path)
                print('Delete file on local machine ' + file_path)
        except:
            print('MERGE query failed, delete file in GCS to avoid duplication')
            # Delete uploaded file
            delete_gcs_object(gcs_file_key)

            merge_result = {
                'status': 'fail',
                'message': """
                    Failed to run MERGE job, keeping file in local machine for further investigation.\n
                    Hint: does the staging table exist? Go to BigQuery web UI to create it
                    """
            }
    else:
        print('Received no MERGE job name, do not run MERGE')

    print(merge_result)

