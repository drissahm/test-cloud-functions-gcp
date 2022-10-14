from google.cloud import storage
from google.cloud import bigquery
import pandas as pd


def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
      event (dict): Event payload.
      context (google.cloud.functions.Context): Metadata for the event.
    """
    # it is mandatory initialize the storage client
    client_st = storage.Client()
    client_bq = bigquery.Client()

    file = event

    # read the csv source file
    df = pd.read_csv(f"gs://cloud-function-test-cih/{file['name']}", index_col = False, sep = ";")

    # make the necessary transformations
    df.rename(columns={"DATE OPERATION": "DATE_OPERATION", " DATE VALEUR": "DATE_VALEUR", " LIBELLE":"LIBELLE"," DEBIT": "DEBIT", " CREDIT": "CREDIT"}, inplace = True)
    df['DEBIT'] = df['DEBIT'].astype('str').str.replace(r',', '.')
    df['CREDIT'] = df['CREDIT'].astype('str').str.replace(r',', '.')
    df['DEBIT'] = df['DEBIT'].astype('float')
    df['CREDIT'] = df['CREDIT'].astype('float')
    df['DATE_OPERATION'] = pd.to_datetime(df['DATE_OPERATION'], format='%d/%m/%Y %H:%M:%S')
    df['DATE_VALEUR'] = pd.to_datetime(df['DATE_VALEUR'], format='%d/%m/%Y %H:%M:%S')

    # define the schema for the bigquery table
    my_schema = [{'name': 'DATE_OPERATION', 'type': 'DATE'}, {'name': 'DATE_VALEUR', 'type': 'DATE'}, {'name': 'LIBELLE', 'type': 'STRING'},{'name': 'DEBIT', 'type': 'FLOAT'},{'name': 'CREDIT', 'type': 'FLOAT'}]
    # upload the dataframe to big query
    df.to_gbq('cih_monitoring.bank_statement_import', 'ma-cl-cih', chunksize=10000, if_exists='replace', table_schema=my_schema)

    # insert the imported data into the clean table while removing duplicates
    insert_command = """CREATE OR REPLACE TABLE cih_monitoring.bank_statement_clean AS
    (SELECT * FROM cih_monitoring.bank_statement_clean) UNION DISTINCT
    (SELECT * FROM cih_monitoring.bank_statement_import);"""
    query_job = client_bq.query(insert_command)
