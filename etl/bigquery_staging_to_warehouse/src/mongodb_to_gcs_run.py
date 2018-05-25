from mongodb_to_gcs_main import run

import sys
import json

run_object = json.loads(sys.argv[1])

print('\nRunning job with input object:\n')
print(run_object)
print('\n')

run(main_dataset_name=run_object['main_dataset_name'],
staging_dataset_name=run_object['staging_dataset_name'],
table_name=run_object['table_name'],
last_load_column_name=run_object['last_load_column_name'],
mongodb_db_name=run_object['mongodb_db_name'],
mongodb_collection_name=run_object['mongodb_collection_name'],
merge_job_name=run_object['merge_job_name'],
config=run_object['config']
)

print('\nJob end')