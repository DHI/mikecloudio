import json
import requests
import pandas as pd

from mikecloudio.timeseries import query_yes_no
from mikecloudio.dataset import Dataset


def request(command, service_url, headers, json_key="data"):
    url = service_url + command
    response = requests.get(url, headers=headers)
    validate_response(response)
    if json_key is None:
        return response.json()

    return response.json()[json_key]


def create_header(api_key, **kwargs):
    if len(kwargs) == 0:
        return {'dhi-open-api-key': api_key}
    else:
        header = create_header(api_key)
        for key in list(kwargs.keys()):
            header_key = key.replace("_", "-")
            header[header_key] = kwargs.pop(key)
        return header


def create_command(commands, delimiter='/'):
    return delimiter.join(commands)


def validate_response(response):
    if response.status_code == 401:
        raise ValueError("Not authorized to make this request.")

    if response.status_code >= 300:
        raise ValueError("Request failed: Check api key.")


def read_api_key_from_text_file(api_key_file_path):
    with open(api_key_file_path) as file:
        return file.readline()


class Connection:

    def __init__(self, api_key, project_name=None, project_id=None,
                 service_url="https://core-metadata-prod.azurewebsites.net/"):
        """
        Connect and interact with MIKE CLOUD data,
        e.g. list all projects, get, create, update, or delete datasets.
        :param api_key: api key that gives access to desired projects
        :type api_key: str
        :param project_id: project ID
        :type project_id: str
        :param project_name: name of the project
        :type project_name: str
        :param service_url: metadata service url
        :type service_url: str
        """
        self.url = service_url
        self._api_key = api_key
        self.project_id = project_id
        self.project_name = project_name

        self._upload_url = None
        self._projects = None
        self._header = create_header(api_key)

        self.validate_project(project_id, project_name)

    def validate_project(self, project_id, project_name):
        if project_id is None and project_name is None:
            raise Exception("Please specify either project_id or project_name.")
        if project_id is None:
            self.project_id = self.get_project_id_from_name(project_name)
        if project_name is None:
            self.project_name = self.get_project_name_from_id(project_id)

    @property
    def api_key(self):
        return self._api_key

    @property
    def projects(self):
        if self._projects is None:
            self._projects = self.request_projects()

        return self._projects

    @property
    def upload_url(self):
        if self._upload_url is None:
            self._upload_url = self.request_upload_url()

        return self._upload_url

    def request(self, command):
        return request(command, self.url, self._header)

    def request_projects(self):
        """
        Request all available projects.
        :return: DataFrame
        """
        return pd.DataFrame(self.request("api/project/list"))

    def request_subprojects(self, project_id=None):
        """
        Request all available subprojects.
        :return: DataFrame
        """
        if project_id is None:
            project_id = self.project_id

        return pd.DataFrame(self.request(f"api/project/{project_id}/subprojects"))

    def request_upload_url(self):
        return self.request("api/transfer/upload-url")

    def get_project_id_from_name(self, project_name):
        projects = self.projects
        project_id = projects['id'].loc[projects['name'] == project_name]
        if len(project_id) == 0:
            raise Exception(f"Invalid {project_name}")

        return project_id.values[0]

    def get_project_name_from_id(self, project_id):
        projects = self.projects
        project_name = projects['name'].loc[projects['id'] == project_id]
        if len(project_name) == 0:
            raise Exception(f"Invalid {project_id}")

        return project_name.values[0]

    def get_subproject_id_from_path(self, path, delimiter='/', project_id=None):
        subprojects = self.request_subprojects(project_id)
        subproject_names = path.split(delimiter)

        if len(subproject_names) == 1:
            subproject_names.insert(0, self.project_name)

        subproject_id = subprojects['id'].loc[subprojects['name'] == subproject_names[1]]
        if len(subproject_id) == 0:
            raise Exception(f"Invalid name {subproject_names[1]}")

        subproject_id = subproject_id.values[0]

        if len(subproject_names) > 2:
            remaining_path = delimiter.join(subproject_names[1:])
            return self.get_subproject_id_from_path(remaining_path, delimiter, subproject_id)
        else:
            return subproject_id

    def get_dataset_id_from_path(self, path, delimiter='/', return_subproject_id=False):
        subprojects_and_dataset = path.split(delimiter)
        path = delimiter.join(subprojects_and_dataset[:-1])
        dataset_name = subprojects_and_dataset[-1]
        subproject_id = self.get_subproject_id_from_path(path)
        datasets = self.request_datasets(subproject_id)
        dataset_id = datasets[datasets.name == dataset_name].id

        if len(dataset_id) == 0:
            raise Exception(f"Invalid name {dataset_name}")
        dataset_id = dataset_id.values[0]

        if return_subproject_id:
            return dataset_id, subproject_id

        return dataset_id

    def request_datasets(self, project_id=None, extend=False):
        if project_id is None:
            project_id = self.project_id

        command = f"api/project/{project_id}/dataset/list"
        if extend:
            command += "-summaries"

        return pd.DataFrame(self.request(command))

    def create_ds(self, name, descr, prop_ds=None, metadata_ds=None, prop_ts=None, content_type="application/json"):
        """
        function to create a new dataset
        :rtype: object
        :param name: name of new dataset
        :type name: str
        :param descr: write something that describes the dataset
        :type descr: str 
        :param prop_ds: properties of the dataset can be added additionally as dict
        :type prop_ds: dict
        :param metadata_ds: metadata of the dataset can be added additionally as dict
        :type metadata_ds: dict
        :param prop_ts: defines the properties for the timeseries:
        <name> and <dataType> must be included in the dictionaries of the prop_ts list.
        Every property defined here must be defined in create_ts() properties too.
        Example: [{"name": "a1", "dataType": "Text"},..]
        Types allowed: "DateTime", "Long", "Double", "Boolean", "Text"
        :type prop_ts: list
        :param content_type: default set to application/json;
        other options: text/plain, text/csv, text/json etc. (see api docs)
        :type content_type: str 
        :return: returns a new Dataset object
        :rtype: mikecloudio.dataset.Dataset
        """
        if prop_ds is None:
            prop_ds = {}
        if metadata_ds is None:
            metadata_ds = {}
        if prop_ts is None:
            prop_ts = []
        if not isinstance(prop_ts, list):
            raise ValueError("prop_ts must be of type list containing type dict")

        types = ["DateTime", "Long", "Double", "Boolean", "Text"]
        count = 0
        for i in range(len(prop_ts)):
            if "name" not in prop_ts[i].keys() or "dataType" not in prop_ts[i].keys():
                raise ValueError("properties timeseries (prop_ts) must contain keys 'name' and 'dataType'")
            for typ in types:
                if prop_ts[i]["dataType"] == typ:
                    count += 1
        if count != len(prop_ts):
            raise ValueError("dataType must be 'DateTime', 'Long', 'Double', 'Boolean' or 'Text'")

        if self.project_id == "":
            raise ValueError("set project ID with function setProject() first")

        header = {'dhi-open-api-key': '{0}'.format(self._api_key), 'Content-Type': '{0}'.format(content_type),
                  'dhi-project-id': '{0}'.format(self.project_id), 'dhi-service-id': "timeseries"}

        url = self.url + "api/ts/dataset"

        dict_ = {"timeSeriesSchema": {
            "properties":
                prop_ts

        },

            "datasetProperties": {
                "name": name,
                "description": descr,
                "metadata": metadata_ds,
                "properties": prop_ds
            }

        }

        body = json.dumps(dict_)
        response = requests.post(url, headers=header, data=body)
        json_ = response.json()

        if response.status_code == 401:
            raise ValueError("not authorized to make this request")
        elif response.status_code >= 300:
            print("json response: ", json_)
            raise ValueError("request failed")
        ds = Dataset(connection=self, id_dataset=json_["id"])
        return ds

    def create_dataset(self, name=None, id=None):
        pass

    def get_ds(self, name="", id="", project_id=None):
        """
        function to create a Dataset object according the project id / or project name
        :param id: ID of dataset
        :param name: name of dataset
        :return: Dataset instance
        :rtype: Dataset
        """
        if name != "" and id == "":
            id = self.query_ds_id(name, project_id)

            if id == "":
                raise ValueError("dataset of name {0} does not exist".format(name))
            dataset = Dataset(connection=self, name_dataset=name)

        if name != "" and id != "":
            dataset = Dataset(connection=self, id_dataset=id, name_dataset=name)

        else:
            if id == "":
                raise ValueError("id of dataset was not defined or does not exist")
            dataset = Dataset(connection=self, id_dataset=id)
        return dataset

    # updates a Dataset: not tested yet
    def update_ds(self, dataset_id, name_update, descr_update, type_ds="file", temp_info=None,
                  spat_info=None, add_prop=None, metadata=None):

        if self.project_id == "":
            raise ValueError("set project ID with function setProject() first")

        if temp_info is None:
            temp_info = {}
        if spat_info is None:
            spat_info = {}
        if add_prop is None:
            add_prop = {}
        if metadata is None:
            metadata = {}

        url = self.url + "api/project/{0}/dataset".format(self.project_id)

        dict_ = {
            "id": dataset_id,
            "name": name_update,
            "description": descr_update,
            "datasetType": type_ds,
            "temporalInformation": temp_info,
            "spatialInformation": spat_info,
            "metadata": metadata,
            "properties": add_prop,
            "tags": [
                "string"
            ]
        }

        body = json.dumps(dict_)

        response = requests.put(url, headers=self._header, data=body)
        if response.status_code >= 300:
            raise ValueError("request failed")

        json_ = response.json()
        return json_

    def del_ds(self, name="", id=""):
        """
        function to request deletion of a dataset
        :param id: id of dataset
        :type id: str
        :param name: name of dataset
        :type name: str
        """

        if name != "" and id == "":
            id = self.query_ds_id(name)

        confirm = query_yes_no("Are you sure you want to delete " + name + " " + id + " ?")
        if confirm is True:
            url = self.url + "api/project/{0}/dataset/{1}".format(self.project_id, id)
            response = requests.delete(url, headers=self._header)
            if response.status_code == 401:
                raise ValueError("not authorized to make this request")
            elif response.status_code >= 300:
                raise ValueError("request failed")

    def query_ds_id(self, name, project_id):
        """
        function to query the dataset id with the help of function list_ds()
        :param name: name of the dataset
        :return: id of the dataset
        :rtype: str
        """
        df = self.request_datasets(project_id)
        _id = ""

        if df.empty:
            raise ValueError("no datasets found for this project")
        for i in range(len(df)):
            if df["name"][i] == name:
                _id = df["id"][i]
                break
        if _id == "":
            raise ValueError("timeseries of name {0} does not exist".format(name))

        return _id

    def query_ds_name(self, id, project_id):
        """
        function to query the dataset id with the help of function list_ds()
        :param id: dataset id
        :return: name of the dataset
        :rtype: str
        """
        df = self.request_datasets(project_id)
        _name = ""

        if df.empty:
            raise ValueError("no datasets found for this project")
        for i in range(len(df)):
            if df["id"][i] == id:
                _name = df["name"][i]
                break
        if _name == "":
            raise ValueError("dataset of id {0} does not exist".format(id))

        return _name


class Project:

    def __init__(self, project_id, project_name):
        self._id = project_id
        self._name = project_name
