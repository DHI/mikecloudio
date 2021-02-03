import pandas as pd

from mikecloudio.request import create_header, request


def get_multidimensional_dataset(connection, path, data_type='md'):
    dataset_id, project_id = connection.get_dataset_id_from_path(path, return_subproject_id=True)
    header = create_header(connection.api_key,
                           dhi_project_id=project_id,
                           dhi_dataset_id=dataset_id,
                           dhi_service_id=data_type)

    command = f"api/{data_type}/{dataset_id}"

    dataset = request(command, connection.url, header, json_key=None)
    return dataset


def get_datetimes_from_dataset(dataset):
    return pd.to_datetime([time['v'] for time in dataset['temporalDomain']['times']])
