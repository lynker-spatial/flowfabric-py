# catalog_utils.py
import requests
import pandas as pd

# autofill streamflow parameters
def auto_streamflow_params(dataset_id):
    """
    Auto-generate streamflow parameters for a given dataset

    :param dataset_id: The id of the dataset
    :type dataset_id: str

    :return: Dictionary containing the recommended streamflow parameters
    """
    catalog = requests.get("https://flowfabric.lynker-spatial.com/catalog")
    json_data = catalog.json()['provider_groups']
    df = pd.json_normalize(json_data, record_path="datasets")
    datasets = df.to_dict(orient='records')
    data = next((dataset for dataset in datasets if dataset.get("id") == dataset_id), None)

    # determine if it is a reanalysis or not
    is_reanalysis = False
    if data['query_mode'] is not None and data['query_mode'] == 'absolute':
        is_reanalysis = True
    if data['configuration'] is not None and 'reanalysis' in data['configuration']:
        is_reanalysis = True

    if is_reanalysis:
        if 'default_start_time' in data:
            start_time = data['default_start_time']
        elif 'min_time' in data:
            start_time = data['min_time']
        else:
            start_time = "2018-01-01T00:00:00Z"

        if 'default_end_time' in data:
            end_time = data['default_end_time']
        elif 'max_time' in data:
            end_time = data['max_time']
        else:
            end_time = "2018-01-31T23:59:59Z"

        params = {
            'query_mode': 'absolute',
            'start_time': start_time,
            'end_time': end_time,
            'scope': data['default_scope'] if 'default_scope' in data else 'all',
            'format': data['default_format'] if 'default_format' in data else 'json',
            'mode': data['default_mode'] if 'default_mode' in data else 'sync',
        }
    else:
        params = {
            'query_mode': 'run',
            'issue_time': data['issue_time'] if 'issue_time' in data else 'latest',
            'scope': data['default_scope'] if 'default_scope' in data else 'all',
            'lead_start': data['lead_start'] if 'lead_start' in data else 0,
            'lead_end': data['lead_end'] if 'lead_end' in data else 0,
            'format': data['default_format'] if 'default_format' in data else 'json',
            'mode': data['default_mode'] if 'default_mode' in data else 'sync',
        }

    catalog.close()
    return params
