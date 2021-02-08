import pandas as pd
import requests

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


def get_item_info_from_dataset(dataset):
    return pd.DataFrame(dataset['items']).set_index('i')


def request_multidimensional_data(connection, path, query, data_type='md'):
    """

    :param connection: Connection
    :param path: dataset path, e.g. GHM/GHM_CHIRPS/GridData
    :param query:
    :param data_type: 'md' for multidimensional
    :return:
    """
    dataset_id, project_id = connection.get_dataset_id_from_path(path, return_subproject_id=True)
    header = create_header(connection.api_key,
                           dhi_project_id=project_id,
                           dhi_dataset_id=dataset_id,
                           dhi_service_id=data_type,
                           Content_Type='application/json',
                           api_version="1")

    command = f"api/{data_type}/{dataset_id}/query"
    url = connection.url + command
    response = requests.post(url, json=query, headers=header)
    return response.json()


def create_test_point():
    from shapely.geometry import Point
    return Point(100.0983376133463, 14.26718894186121)


def create_multidimensional_data_query(item_indices=None, time_range=None, srid=4326,
                                       geometry_wkt_str=None,
                                       include_geometries=False,
                                       include_values=True):
    """

    :param item_indices:
    :param time_range:
    :param geometry_wkt_str: geometry WKT string
        Geometries are specified as WKT: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
    :param srid:
    :param include_geometries:
    :param include_values:
    :return:
    """
    item_indices = [0] if item_indices is None else item_indices
    time_range = [0, 10] if time_range is None else time_range

    return {
        "itemFilter": {
            "itemIndices": item_indices
        },
        "spatialFilter": {
            "geometry": geometry_wkt_str,
            "srid": srid
        },
        "temporalFilter": {
            "type": "TemporalIndexFilter",
            "from": min(time_range),
            "to": max(time_range)
        },
        "verticalFilter": None,
        "includeGeometries": include_geometries,
        "includeValues": include_values
    }
