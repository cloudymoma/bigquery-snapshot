import os
import re
import json
import time
import requests
import functions_framework
import google_crc32c
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery,storage
from google.cloud import bigquery_v2
from datetime import date,timedelta,datetime
from datetime import datetime,timezone
from google.cloud.bigquery import Dataset
from google.cloud import secretmanager


def validate_project(client,proj_id):
    try:
        datasets = client.list_datasets(proj_id)
        return True
    except Exception:
        # raise ValueError('The project id {} is not valid'.format(proj_id))
        return False

def validate_dataset(client,ds_id):
    try:
        tables = client.get_dataset(ds_id)
        return True
    except Exception:
        # raise ValueError('The dataset id {} is not valid'.format(ds_id))
        return False

def validate_table(client,tbl_id):
    try:
        table = client.get_table(tbl_id)
        return True
    except Exception:
        # raise ValueError('The dataset id {} is not valid'.format(ds_id))
        return False

def get_tbl_prop(client, tbl_id):
    try:
        tbl = client.get_table(tbl_id)
    except Exception:
        raise ValueError('Failed to get metadata of table {}'.format(tbl_id))
    tbl_properties = {}
    tbl_properties['location'] = tbl.location
    tbl_properties['num_bytes'] = tbl.num_bytes
    tbl_properties['num_rows'] = tbl.num_rows
    tbl_properties['view_query']=tbl.view_query
    tbl_properties['mview_query'] = tbl.mview_query
    tbl_properties['table_type'] = tbl.table_type
    if tbl.table_type == 'TABLE':
       tbl_properties['table_and_view'] = 'table' #integer partition
    else:
        tbl_properties['table_and_view'] = 'view' # not table

    return tbl_properties

def access_secret_version(project_id, secret_id, version_id):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected.")
        return response

    payload = response.payload.data.decode("UTF-8")
    payload = json.loads(payload)
    payload = json.dumps(payload, indent=4)
    with open("sa.json", "w") as outfile:
        outfile.write(payload)

@functions_framework.http
def bq_snapshot(request):
    
    req = request.get_json(silent=True)
    
    client = bigquery.Client()
    gcs_client=storage.Client()
    current_ts=datetime.now()
    exp_ts=current_ts + timedelta(days=7)
    current_ts_str = current_ts.strftime("%Y-%m-%dT%H-%M-%S")
    exp_ts_str=str(exp_ts.replace(tzinfo=timezone.utc).isoformat())
    project_id = req['project_id']
    secret_id = req['secret_id']
    version_id = req['version_id']
    access_secret_version(project_id=project_id, secret_id=secret_id, version_id=version_id)


    scope= [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

    path_to_credential = "sa.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
    credentials = service_account.Credentials.from_service_account_file(path_to_credential,scopes=scope)
    auth_req=google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    token=credentials.token



    print('----------------Phase 1 - Parsing Input ----------------')
    list_ops_by_table=[]
    input_project = req['project']
    input_dataset = req['dataset']
    input_table = req['table']

    if input_dataset == '*':
        datasets = client.list_datasets(input_project)
        if datasets:
            for dataset in datasets:
                if not dataset.dataset_id.endswith("-snapshot"):
                    ds_id=input_project + '.' + dataset.dataset_id
                    tables = client.list_tables(ds_id)
                    dest_dataset =  dataset.dataset_id + '-snapshot'
                    if tables:
                        for table in tables:
                            tbl_id=ds_id + '.' + table.table_id
                            tbl_properties=get_tbl_prop(client,tbl_id)
                            dict_ops_by_table = {
                                'input_project':input_project,
                                'input_dataset':dataset.dataset_id,
                                'input_table':table.table_id,
                                'tbl_id':tbl_id,
                                'tbl_properties':tbl_properties
                            }
                            list_ops_by_table.append(dict_ops_by_table)
                else:
                    print('No tables found in dataset {}'.format(ds_id))
        else:
            print('No dataset found in project {}'.format(input_project))
    elif input_table == '*':
        ds_id = input_project + '.' + input_dataset
        tables = client.list_tables(ds_id)
        if tables:
            for table in tables:
                tbl_id = ds_id + '.' + table.table_id
                tbl_properties = get_tbl_prop(client, tbl_id)
                dict_ops_by_table = {
                    'input_project': input_project,
                    'input_dataset': input_dataset,
                    'input_table': table.table_id,
                    'tbl_id':tbl_id,
                    'tbl_properties': tbl_properties
                }
                list_ops_by_table.append(dict_ops_by_table)
        else:
            print('No tables found in dataset {}'.format(ds_id))
    else:
        tbl_id=input_project + '.' + input_dataset + '.' + input_table
        tbl_properties = get_tbl_prop(client, tbl_id)
        dict_ops_by_table = {
            'input_project': input_project,
            'input_dataset': input_dataset,
            'input_table': input_table,
            'tbl_id':tbl_id,
            'tbl_properties': tbl_properties
        }
        list_ops_by_table.append(dict_ops_by_table)
    list_ops_by_phy_table = [x for x in list_ops_by_table if x['tbl_properties']['table_and_view'] == 'table']
    print('Complete parsing inputs and find {} tables'.format(str(len(list_ops_by_phy_table))))



    print('----------------Phase 2 - Processing Tables ----------------')


    for item in list_ops_by_phy_table:
        input_project=item['input_project']
        input_dataset=item['input_dataset']
        input_table=item['input_table']
        location=item['tbl_properties']['location']
        dest_project=input_project
        dest_dataset=input_dataset + '_snapshot'
        snap_dataset_id=dest_project + '.' + dest_dataset
        if not validate_dataset(client,snap_dataset_id):
            snap_dataset=Dataset(snap_dataset_id)
            snap_dataset.location =location
            snap_dataset=client.create_dataset(snap_dataset,timeout=30)
            print('Dataset {} created in region {}'.format(snap_dataset.dataset_id,snap_dataset.location))



        dest_table=input_table + '_' + current_ts_str
        dest_tbl_id=dest_project + '.' + dest_dataset + '.' + dest_table
        if validate_table(client,dest_tbl_id):
                raise ValueError('Table {} already exist, please give a new table name.'.format(dest_table))


        bq_endpoint = 'https://bigquery.googleapis.com/bigquery/v2/projects/{}/jobs'.format(input_project)

        request_body=json.dumps({
                      "configuration": {
                        "copy": {

                          "sourceTables": [
                            {
                              "projectId": input_project,
                              "datasetId": input_dataset,
                              "tableId": input_table
                            }
                          ],
                          "destinationTable": {
                            "projectId": dest_project,
                            "datasetId": dest_dataset,
                            "tableId": dest_table
                          },
                          "operationType": "SNAPSHOT",
                          "writeDisposition": "WRITE_EMPTY",
                          "destinationExpirationTime": exp_ts_str
                        }
                      }
                    })

        headers = {'Authorization': 'Bearer {}'.format(token)}
        response = requests.post(bq_endpoint, headers=headers,data=request_body)
        src_tbl_id = input_project + '.' + input_dataset + '.' + input_table
        dest_tbl_id = dest_project + '.' + dest_dataset + '.' + dest_table
        if response.status_code ==200:
            job_id = json.loads(response.content.decode("utf-8"))['id'].split('.')[1]
            print('Snapshot {} for table {} submitted with jobid {}'.format(dest_tbl_id,src_tbl_id,job_id))
            snapshot_job=client.get_job(job_id,location=location)
            job_state=snapshot_job.state
            job_created = snapshot_job.created
            wait_index=1
            while job_state == 'RUNNING':
                print('waiting for {} seconds'.format(str(wait_index * 5)))
                time.sleep(5)
                snapshot_job = client.get_job(job_id,location=location)
                job_state = snapshot_job.state
                wait_index += 1
            job_ended = snapshot_job.ended
            job_duration = job_ended - job_created
            print('Job {} completed in {}'.format(job_id,str(job_duration)))
        else:
            print('Snapshot {} for table {} failed'.format(dest_tbl_id, src_tbl_id))
            print(response.text)
    return 'ok' 
