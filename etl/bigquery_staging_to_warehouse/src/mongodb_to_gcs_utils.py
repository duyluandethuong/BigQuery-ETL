
# coding: utf-8

# In[76]:
import sys
from datetime import datetime
import time
import json
import os
import uuid
from google.cloud import bigquery
from google.cloud import storage
#Set environment variable

def get_last_load_index(main_dataset_name, staging_dataset_name, table_name, column_name, gcs_source, project_name):
    bigquery_client = bigquery.Client()

    metadata_sql = """
        SELECT COALESCE(MAX({column_name}), '2010-01-01 00:00:00') AS last_load_timestamp 
        FROM `{project_name}`.`{main_dataset_name}`.`{table_name}`
    """.format(
        column_name = column_name,
        project_name = project_name,
        main_dataset_name = main_dataset_name,
        table_name = table_name
    )

    try:
        # If table does exist
        run_metadata_job = bigquery_client.query(metadata_sql)
        metadata = run_metadata_job.result()  # Waits for job to complete.

        last_updated_timestamp = None

        for row in metadata:
            last_updated_timestamp = row.last_load_timestamp
    except:
        # If table does not exist
        print('Table `{project_name}`.`{main_dataset_name}`.`{table_name}` does not exist, assume timestamp 2010-01-01'.format(
                project_name = project_name,
                main_dataset_name = main_dataset_name,
                table_name = table_name
            ))
        
        last_updated_timestamp = datetime(2010, 1, 1, 0, 0, 0, 0)
        
    return last_updated_timestamp


# In[80]:

def upload_file_to_gcs(local_file_path, gcs_file_key):

    client = storage.Client()
    bucket = client.get_bucket('<staging bucket name>')
    blob = bucket.blob(gcs_file_key)
    blob.upload_from_filename(local_file_path)

    print('Upload file to GCS, key = ' + str(gcs_file_key))
    return gcs_file_key


# In[81]:


def delete_gcs_object(object_key):
    from google.cloud import storage

    client = storage.Client()
    bucket = client.get_bucket('<staging bucket name>')

    blob = bucket.blob(object_key)
    blob.delete()

