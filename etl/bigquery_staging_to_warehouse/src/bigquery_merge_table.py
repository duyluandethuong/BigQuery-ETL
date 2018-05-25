
# coding: utf-8

# In[112]:


import os
import subprocess
import sys
import datetime
import math


# In[113]:



# In[114]:


job_start_time = datetime.datetime.now()
print('Job started at ' + str(job_start_time))


# In[115]:


#Set environment variable
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "" # visible in this process + all children


# In[116]:


# Imports the Google Cloud client library
from google.cloud import bigquery

# Instantiates a client
bigquery_client = bigquery.Client()

#Step to conduct
#1. Get last tranformation timestamp
#2. Run query to get new data, insert to table
#3. Update tranformation timestamp


# In[117]:


# Run MERGE SQL
def get_job_metadata(job_name):
    metedata_sql = 'SELECT * FROM `metadata`.`etl_metadata` WHERE job_name = "{job_name}"'.format(job_name = job_name)
    run_metadata_job = bigquery_client.query(metedata_sql)
    metadata = run_metadata_job.result()  # Waits for job to complete.

    metadata_result = {}

    for row in metadata:
        metadata_result['job_name'] = row.job_name
        metadata_result['project_name'] = row.project_name
        metadata_result['staging_dataset_name'] = row.staging_dataset_name
        metadata_result['staging_table_name'] = row.staging_table_name
        metadata_result['destination_dataset_name'] = row.destination_dataset_name
        metadata_result['destination_table_name'] = row.destination_table_name
        metadata_result['timestamp_column'] = row.timestamp_column
        metadata_result['float_column'] = row.float_column
        metadata_result['id_column_name'] = row.id_column_name
        metadata_result['max_column_name'] = row.max_column_name
        metadata_result['last_run_time'] = row.last_run_time
        metadata_result['contains_record_type'] = row.contains_record_type

    return metadata_result


# In[118]:


def create_table_if_not_exist(
        project_name,
        staging_dataset_name,
        staging_table_name,
        destination_dataset_name,
        destination_table_name,
        timestamp_column,
        float_column
    ):
    
    destination_dataset_ref = bigquery_client.dataset(destination_dataset_name)
    destination_table_ref = destination_dataset_ref.table(destination_table_name)

    from google.cloud.exceptions import NotFound
    try:
        table = bigquery_client.get_table(destination_table_ref)
        print('Table already exists in DWH')
        return False
    except NotFound:
        print('Table not exists in DWH')
        # Get schema from staging
        dataset_ref = bigquery_client.dataset(staging_dataset_name)
        table_ref = dataset_ref.table(staging_table_name)
        table = bigquery_client.get_table(table_ref)  # API Request

        # View table properties
        table_schema = table.schema

        # Convert timestamp string to array
        if timestamp_column is not None:
            timestamp_column_array = [x.strip() for x in timestamp_column.split(',')]
        else:
            timestamp_column_array = []
            
        if float_column is not None:
            float_column_array = [x.strip() for x in float_column.split(',')]
        else:
            float_column_array = []

        # Search in schema for column name and replace existing type with timestamp
        destination_table_schema = []
        for column in table_schema:
            if column.name in timestamp_column_array:
                print('Found column {column_name}, update its type to timestamp'.format(column_name = column.name))
                schema_field = bigquery.SchemaField(column.name, 'TIMESTAMP', mode='NULLABLE')
                destination_table_schema.append(schema_field)
            elif column.name in float_column_array:
                print('Found column {column_name}, update its type to float'.format(column_name = column.name))
                schema_field = bigquery.SchemaField(column.name, 'FLOAT', mode='NULLABLE')
                destination_table_schema.append(schema_field)
            else:
                # If field is nested, then use the same child schema
                if column.field_type == 'RECORD':
                    schema_field = bigquery.SchemaField(column.name, column.field_type,
                                                        mode=column.mode, description=column.description,
                                                       fields=column.fields)
                    destination_table_schema.append(schema_field)
                # For normal record
                else:
                    schema_field = bigquery.SchemaField(column.name, column.field_type, mode='NULLABLE')
                    destination_table_schema.append(schema_field)

        new_table = bigquery.Table(destination_table_ref, schema = destination_table_schema)
        new_table = bigquery_client.create_table(new_table)  # API request

        assert new_table.table_id == destination_table_name
        print('Table {project_name}.{destination_dataset_name}.{destination_table_name} has been created'.format(
            project_name = project_name,
            destination_dataset_name = destination_dataset_name,
            destination_table_name = destination_table_name
        ))
        return True;


# In[119]:


def generate_merge_sql(project_name,
                       staging_dataset_name,
                       staging_table_name, 
                       destination_dataset_name, 
                       destination_table_name,
                       id_column_name,
                       max_column_name,
                       timestamp_column,
                       float_column,
                       contains_record_type=0):
    
    # Get table schema
    dataset_ref = bigquery_client.dataset(staging_dataset_name)
    table_ref = dataset_ref.table(staging_table_name)
    table = bigquery_client.get_table(table_ref)  # API Request

    # View table properties
    table_schema = table.schema
    # print(table.description)
    # print(table.num_rows)
    
    # Convert timestamp string to array
    if timestamp_column is not None:
        timestamp_column_array = [x.strip() for x in timestamp_column.split(',')]
    else:
        timestamp_column_array = []

    if float_column is not None:
        float_column_array = [x.strip() for x in float_column.split(',')]
    else:
        float_column_array = []

    # Build an array contain columns' name
    table_column_list = ', '.join([str(column.name) for column in table_schema])
    latest_column_list = ', '.join([str('latest_record_table.' + column.name) for column in table_schema])
    
    update_set_column_list_array = []
    insert_column_list_array = []
    for column in table_schema:
        if column.name in timestamp_column_array:
            set_column = 'dest.{column_name} = CAST(latest_record_table.{column_name} AS TIMESTAMP)'.format(
                            column_name = column.name)
            insert_column = 'CAST(latest_record_table.{column_name} AS TIMESTAMP)'.format(column_name = column.name)
        elif column.name in float_column_array:
            set_column = 'dest.{column_name} = CAST(latest_record_table.{column_name} AS FLOAT64)'.format(
                            column_name = column.name)
            insert_column = 'CAST(latest_record_table.{column_name} AS FLOAT64)'.format(column_name = column.name)
        else:
            set_column = 'dest.{column_name} = latest_record_table.{column_name}'.format(
                            column_name = column.name)
            insert_column = 'latest_record_table.{column_name}'.format(column_name = column.name)
        
        update_set_column_list_array.append(set_column)
        insert_column_list_array.append(insert_column)
    
    #Concat query string for the SET statement
    update_set_column_list = ', '.join([str(each_column) for each_column in update_set_column_list_array])
    insert_column_list = ', '.join([str(each_column) for each_column in insert_column_list_array])
    
    # If query is run against a table whose has RECORD type field, do not add DISTINCT
    if (contains_record_type == 1):
        distinct_statment = ' '
    else:
        distinct_statment = 'DISTINCT '
    
    base_query = """
    #standardSQL
    MERGE `{project_name}`.`{destination_dataset_name}`.`{destination_table_name}` dest
    USING (
      WITH latest AS (
        SELECT {id_column_name}, MAX({max_column_name}) AS max_field
        FROM `{project_name}`.`{staging_dataset_name}`.`{staging_table_name}`
        GROUP BY {id_column_name}
      )

      SELECT {distinct_statment} stg.*
      FROM latest
      JOIN `{project_name}`.`{staging_dataset_name}`.`{staging_table_name}` stg 
      ON stg.{id_column_name} = latest.{id_column_name} AND stg.{max_column_name} = latest.max_field
    ) latest_record_table
    ON latest_record_table.{id_column_name} = dest.{id_column_name}
    WHEN MATCHED THEN
      UPDATE SET {update_set_column_list}
    WHEN NOT MATCHED THEN
      INSERT({table_column_list})
      VALUES({insert_column_list})
    """.format(
        project_name = project_name,
        staging_dataset_name = staging_dataset_name,
        staging_table_name = staging_table_name,
        destination_dataset_name = destination_dataset_name,
        destination_table_name = destination_table_name,
        id_column_name = id_column_name,
        max_column_name = max_column_name,
        table_column_list = table_column_list,
        update_set_column_list = update_set_column_list,
        insert_column_list = insert_column_list,
        distinct_statment = distinct_statment
    )
    return base_query


# In[120]:


# Get job metadata
def run_job_main(job_name):
    print('Running job ' + str(job_name))
    job_metadata = get_job_metadata(job_name)


    # In[121]:


    project_name = job_metadata['project_name']
    staging_dataset_name = job_metadata['staging_dataset_name']
    staging_table_name = job_metadata['staging_table_name']

    destination_dataset_name = job_metadata['destination_dataset_name']
    destination_table_name = job_metadata['destination_table_name']

    id_column_name = job_metadata['id_column_name']
    max_column_name = job_metadata['max_column_name']

    timestamp_column = job_metadata['timestamp_column']
    float_column = job_metadata['float_column']

    contains_record_type = job_metadata['contains_record_type']

    # Check table creation
    create_table_if_not_exist(project_name = project_name,
        staging_dataset_name = staging_dataset_name,
        staging_table_name = staging_table_name,

        destination_dataset_name = destination_dataset_name,
        destination_table_name = destination_table_name,
        timestamp_column = timestamp_column,
        float_column = float_column)

    # Generate MERGE SQL
    merge_sql = generate_merge_sql(project_name = project_name,
        staging_dataset_name = staging_dataset_name,
        staging_table_name = staging_table_name,

        destination_dataset_name = destination_dataset_name,
        destination_table_name = destination_table_name,

        id_column_name = id_column_name,
        max_column_name = max_column_name,

        timestamp_column = timestamp_column,
        float_column = float_column,
        contains_record_type = contains_record_type)

    query_start_time = datetime.datetime.now()
    print('Start query at ' + str(query_start_time))
    print('Running query...')
    print(merge_sql)

    # Run MERGE SQL
    run_merge_job = bigquery_client.query(merge_sql)
    results = run_merge_job.result()  # Waits for job to complete.
    dir(run_merge_job)

    query_end_time = datetime.datetime.now()
    print('End query at ' + str(query_end_time))

    query_run_time = math.floor((query_end_time - query_start_time).seconds)
    print('Query end - running for ' + str(query_run_time) + ' seconds')

    #Finish
    job_end_time = datetime.datetime.now()
    print('Job end at ' + str(job_end_time))
    return {
        'status': 'done',
        'timestamp': job_end_time
    }
