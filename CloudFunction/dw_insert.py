import logging
import base64
from google.cloud import bigquery
import json

def dw_insert(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print("Message: " + pubsub_message)
    dw_message = json.loads(pubsub_message)
    dataset_info = dw_message["datasetInfo"]
    payload = base64.b64decode(dw_message["payload"]).decode('utf-8')

    client = bigquery.Client()
    dataset_id = dataset_info["datasetName"]
    table_id = dataset_info["tableName"]
    print("Dataset name:" + dataset_id);
    print("Table name: " + table_id);
    print("payload: " + payload);
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)

    valid_json_string = "[" + payload + "]"  # or "[{0}]".format(your_string)
    rows_to_insert = json.loads(valid_json_string)

    print(*rows_to_insert)
    errors = client.insert_rows(table, rows_to_insert)
    if not errors:
        print('Loaded {} row(s) into {}:{}'.format(len(rows_to_insert), dataset_id, table_id))
    else:
        print('Errors:')
        for error in errors:
            print(error)
