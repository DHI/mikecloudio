import requests
import json
import pandas as pd
import warnings

from mikecloudio.timeseries import Dataset, query_yes_no


class Connection:

    def __init__(self, api_key, id_proj="", name_proj="", ds_object="",
                 url="https://core-metadata-prod.azurewebsites.net/"):
        """
        Connect and interact with MIKE CLOUD data,
        e.g. list all projects, get, create, update, or delete datasets
        :param api_key: api key that gives access to desired projects
        :type api_key: str
        :param id_proj: project ID
        :type id_proj: str
        :param name_proj: name of the project
        :type name_proj: str
        :param ds_object: instance of dataset if already created from another connection
        :type ds_object: mikecloudio.timeseries.Dataset
        """
        self.metadata_service_url = url
        self._api_key = api_key
        self._header = {'dhi-open-api-key': '{0}'.format(self._api_key)}
        self._uploadURL = ""
        self.ds = ds_object
        self._id_proj = id_proj
        self._name_proj = name_proj

        if id_proj == "" and name_proj != "":
            self._id_proj = self.query_proj_id(name_proj)
            self._name_proj = name_proj

        elif id_proj != "" and name_proj == "":
            self._name_proj = self.query_proj_name(id_proj)
            self._id_proj = id_proj

        if self._id_proj == "" and self._name_proj == "":
            warnings.warn("neither project ID nor project name set. Call function set_project()")

    @staticmethod
    def read_api_key(api_key_file_path):
        with open(api_key_file_path) as file:
            return file.readline()

    def get_id(self):
        """
        Getter for project id
        :return: project id
        """
        return self._id_proj

    def get_api_key(self):
        """
        Getter for API KEY
        :return: API KEY
        """
        return self._api_key

    def get_header(self):
        """
        Getter for header
        :return: header
        """
        return self._header

    def list_projects(self):
        """
        function to request all available projects with given api key
        :return: DataFrame
        """
        url = self.metadata_service_url + "api/project/list"
        response = requests.get(url, headers=self._header)
        if response.status_code >= 300:
            raise ValueError("request failed: check api key")
        if response.status_code == 401:
            raise ValueError("not authorized to make this request")
        json_ = response.json()
        df = pd.DataFrame(json_["data"])
        return df

    def get_upload_url(self):
        """
        function to request upload url
        :return: upload url
        :rtype: str
        """
        url = self.metadata_service_url + "api/transfer/upload-url"
        response = requests.get(url, headers=self._header)
        if response.status_code >= 300:
            raise ValueError("request failed: check api key")
        json_ = response.json()
        self._uploadURL = json_["data"]
        return json_["data"]

    def set_project(self, name="", id=""):
        """
        function to set project ID if not done in the constructor already. User can use id or the project name
        :param id: project ID
        :type id: str
        :param name: project name according to list_project() request
        :type name: str
        :return: Project object
        :rtype: Project
        """

        if name != "" and id == "":
            self._id_proj = self.query_proj_id(name)
        else:
            self._id_proj = id
        proj = Project(id)
        return proj

    def list_ds(self, extended=False):
        """
        function to list all datasets with project id
        :param extended: if set to true a different request is made with alternative response
        :type extended: bool
        :return: dataframe with all datasets
        :rtype: pd.DataFrame
        """
        if self._id_proj == "":
            raise ValueError("set project ID with function setProject() first")
        if extended is True:
            url = self.metadata_service_url + "api/project/{0}/dataset/list".format(self._id_proj)
        else:
            url = self.metadata_service_url + "api/project/{0}/dataset/list-summaries".format(self._id_proj)
        response = requests.get(url, headers=self._header)
        json_ = response.json()
        if response.status_code == 401:
            raise ValueError("not authorized to make this request")
        elif response.status_code >= 300:
            print("json response: ", json_)
            raise ValueError("request failed")

        else:
            df = pd.DataFrame(json_["data"])

        return df

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
        :rtype: mikecloudio.timeseries.Dataset
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

        if self._id_proj == "":
            raise ValueError("set project ID with function setProject() first")

        header = {'dhi-open-api-key': '{0}'.format(self._api_key), 'Content-Type': '{0}'.format(content_type),
                  'dhi-project-id': '{0}'.format(self._id_proj), 'dhi-service-id': "timeseries"}

        url = self.metadata_service_url + "api/ts/dataset"

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

    def get_ds(self, name="", id=""):
        """
        function to create a Dataset object according the project id / or project name
        :param id: ID of dataset
        :param name: name of dataset
        :return: Dataset instance
        :rtype: Dataset
        """
        if name != "" and id == "":
            id = self.query_ds_id(name)

            if id == "":
                raise ValueError("dataset of name {0} does not exist".format(name))
            self.ds = Dataset(connection=self, name_dataset=name)

        if name != "" and id != "":
            self.ds = Dataset(connection=self, id_dataset=id, name_dataset=name)

        else:
            if id == "":
                raise ValueError("id of dataset was not defined or does not exist")
            self.ds = Dataset(connection=self, id_dataset=id)
        return self.ds

    # updates a Dataset: not tested yet
    def update_ds(self, dataset_id, name_update, descr_update, type_ds="file", temp_info=None,
                  spat_info=None, add_prop=None, metadata=None):

        if self._id_proj == "":
            raise ValueError("set project ID with function setProject() first")

        if temp_info is None:
            temp_info = {}
        if spat_info is None:
            spat_info = {}
        if add_prop is None:
            add_prop = {}
        if metadata is None:
            metadata = {}

        url = self.metadata_service_url + "api/project/{0}/dataset".format(self._id_proj)

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
            url = self.metadata_service_url + "api/project/{0}/dataset/{1}".format(self._id_proj, id)
            response = requests.delete(url, headers=self._header)
            if response.status_code == 401:
                raise ValueError("not authorized to make this request")
            elif response.status_code >= 300:
                raise ValueError("request failed")

    def query_proj_id(self, name):
        """
        function to query the project id with the help of function list_projects()
        :param name: name of project
        :return: id of the project
        :rtype: str
        """
        df = self.list_projects()
        _id = ""
        if df.empty:
            raise ValueError("no projects found for this api token")
        for i in range(len(df)):
            if df["name"][i] == name:
                _id = df["id"][i]
                break
        return _id

    def query_proj_name(self, id_proj):
        """
        function to query the project name with the help of function list_projects()
        :param id_proj: id of the project
        :return: id of the project
        :return: project name
        :rtype: str
        """
        df = self.list_projects()
        _name = ""
        if df.empty:
            raise ValueError("no projects found for this api token")
        for i in range(len(df)):
            if df["id"][i] == id_proj:
                _name = df["name"][i]
                break
        return _name

    def query_ds_id(self, name):
        """
        function to query the dataset id with the help of function list_ds()
        :param name: name of the dataset
        :return: id of the dataset
        :rtype: str
        """
        df = self.list_ds()
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

    def query_ds_name(self, id):
        """
        function to query the dataset id with the help of function list_ds()
        :param id: dataset id
        :return: name of the dataset
        :rtype: str
        """
        df = self.list_ds()
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

    def __init__(self, id_project="", name_project=""):
        self._id = id_project
        self._name = name_project
