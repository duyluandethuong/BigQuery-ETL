#
# bigquery ETL Dockerfile
#
FROM jupyter/datascience-notebook
RUN conda install pip
RUN /opt/conda/bin/pip install google-cloud
RUN /opt/conda/bin/pip install pymongo
RUN /opt/conda/bin/pip install google-cloud-bigquery
RUN /opt/conda/bin/pip install google-cloud-storage
COPY <RELATIVE path to your sevice account JSON key file> <repeate RELATIVE path to your sevice account JSON key file>
ENV GOOGLE_APPLICATION_CREDENTIALS="/src/<key file name>.json"
