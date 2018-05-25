import sys
from bigquery_merge_table import run_job_main
# Run Main Job
job_name = sys.argv[1]
run_job_main(job_name)
