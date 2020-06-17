import requests
import json
import pandas as pd
import sys


class ConnectMikeCloud:
    metadata_service_url = "https://core-metadata-prod.azurewebsites.net/"

    def __init__(self, api_key, id_proj="", proj=None, ds_object=None):
        """
        this class creates a connection to MIKE CLOUD and can be used to list get all projects, get datasets of projects
        create, update, and delete datasets
        :param api_key: api key that gives access to desired projects
        :type api_key: str
        :param id_proj:
        :type id_proj: str
        :param proj:
        :type proj: object
        """

        self._api_key = api_key
        self._header = {'dhi-open-api-key': '{0}'.format(self._api_key)}
        self._uploadURL = ""
        self.ds = ds_object
        if proj is None:
            self.proj = proj

        if id_proj != "":
            self.proj = Project(id_proj)
            self._id_proj = id_proj

        else:
            self.proj = proj
            self._id_proj = id_proj
            self.proj = Project(self._id_proj)

    def get_id(self):
        return self._id_proj

    def get_api_key(self):
        return self._api_key

    def get_header(self):
        return self._header

    def list_projects(self):
        """
        function to request all available projects with given api key
        :return: DataFrame
        """
        url = self.metadata_service_url + "api/project/list"
        response = requests.get(url, headers=self._header)
        print("Status: ", response.status_code)
        json_ = response.json()
        df = pd.DataFrame(json_["data"])
        return df

    def get_upload_url(self):
        """
        function to request upload url
        :return: str
        """
        url = self.metadata_service_url + "api/transfer/upload-url"
        response = requests.get(url, headers=self._header)
        print("Status: ", response.status_code)
        json_ = response.json()
        self._uploadURL = json_["data"]
        return json_["data"]

    def set_project(self, id_proj="", proj_name=""):
        """
        function to set project ID if not done in the constructor already. User can use id or the project name
        :param id_proj: project ID
        :type id_proj: str
        :param proj_name: project name according to list_project() request
        :type proj_name: str
        :return: Project object
        :rtype: object
        """

        if proj_name != "" and id_proj == "":
            self._id_proj = self.query_proj_id(proj_name)
        else:
            self._id_proj = id_proj
        self.proj = Project(id_proj)
        return self.proj

    def list_ds(self):
        """
        function to list all datasets with project id
        :return:
        :rtype: pd.DataFrame
        """
        if self._id_proj == "":
            raise ValueError("set project ID with function setProject() first")

        url = self.metadata_service_url + "api/project/{0}/dataset/list-summaries".format(self._id_proj)
        response = requests.get(url, headers=self._header)
        json_ = response.json()
        print("Status: ", response.status_code)
        if response.status_code >= 300:
            print("json: ", json_)
            df = pd.DataFrame(json_)
        else:
            df = pd.DataFrame(json_["data"])

        return df

    def create_ds(self, ds_name, ds_description, properties=None, metadata=None, content_type="application/json"):
        """
        function to create a new dataset
        :rtype: object
        :param ds_name: name of new dataset
        :type ds_name: str
        :param ds_description: write something that describes the dataset
        :type ds_description: str 
        :param properties: properties of the dataset can be added additionally as dict
        :type properties: dict
        :param metadata: metadata of the dataset can be added additionally as dict
        :type metadata: dict
        :param content_type: generally set to application/json;
        other options: text/plain, text/csv, text/json etc. (see api docs)
        :type content_type: str 
        :return: returns a new Dataset object
        """
        if properties is None:
            properties = {}
        if metadata is None:
            metadata = {}

        if self._id_proj == "":
            raise ValueError("set project ID with function setProject() first")
        header = {'dhi-open-api-key': '{0}'.format(self._api_key), 'Content-Type': '{0}'.format(content_type),
                  'dhi-project-id': '{0}'.format(self._id_proj), 'dhi-service-id': "timeseries"}

        url = self.metadata_service_url + "api/ts/dataset"

        def conv_json(na, desc, prop, meta):
            """
            converts dictionary to json
            """
            dict_ = {"timeSeriesSchema": {
                "properties": [
                ]
            },

                "datasetProperties": {
                    "name": na,
                    "description": desc,
                    "metadata": meta,
                    "properties": prop
                }

            }

            return dict_

        conv = conv_json(ds_name, ds_description, properties, metadata)
        body = json.dumps(conv)
        response = requests.post(url, headers=header, data=body)
        print("Status: ", response.status_code)
        json_ = response.json()
        ds = Dataset(connection=self, dataset_id=json_["id"])
        return ds

    def get_ds(self, name_ds="", id_dataset=""):
        """
        function to create a Dataset object according the project id / or project name
        :param id_dataset:
        :param name_ds:
        :return: Dataset object
        :rtype: object
        """
        if name_ds != "" and id_dataset == "":
            id_dataset = self.query_ds_id(name_ds)
            if id_dataset == "":
                raise ValueError("dataset of name {0} does not exist".format(name_ds))
        if id_dataset == "":
            raise ValueError("id of dataset was not defined or does not exist")
        self.ds = Dataset(connection=self, dataset_id=id_dataset)
        return self.ds

    # updates a Dataset: not tested yet
    def update_ds(self, dataset_id, upd_name, upd_descr, ds_type="file", temp_info=None,
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

        def conv_json(na, desc, typ, ds_id, temporal, spatial,
                      properties, meta):

            dict_ = {
                "id": ds_id,
                "name": na,
                "description": desc,
                "datasetType": typ,
                "temporalInformation": temporal,
                "spatialInformation": spatial,
                "metadata": meta,
                "properties": properties,
                "tags": [
                    "string"
                ]
            }
            return dict_

        conv = conv_json(dataset_id, upd_name, upd_descr, ds_type, temp_info, spat_info,
                         add_prop, metadata)
        body = json.dumps(conv)

        response = requests.put(url, headers=self._header, data=body)
        print("Status: ", response.status_code)
        json_ = response.json()
        return json_

    def del_ds(self, id_ds="", name_ds=""):
        """
        function to request deletion of a dataset
        :param id_ds: id of dataset
        :type id_ds: str
        :param name_ds: name of dataset
        :type name_ds: str
        """

        if name_ds != "" and id_ds == "":
            id_ds = self.query_ds_id(name_ds)
            if id_ds == "":
                raise ValueError("timeseries of name {0} does not exist".format(name_ds))

        confirm = query_yes_no("Are you sure you want to delete " + name_ds + " " + id_ds + " ?")
        if confirm is True:
            url = self.metadata_service_url + "api/project/{0}/dataset/{1}".format(self._id_proj, id_ds)
            response = requests.delete(url, headers=self._header)
            print("Status: ", response.status_code)

    def query_proj_id(self, proj_name):
        """
        function to query the project id with the help of function list_projects()
        :param proj_name:
        :return: id of the project
        :rtype: str
        """
        df = self.list_projects()
        _id = ""
        if df.empty:
            raise ValueError("no projects found for this api token")
        for i in range(len(df)):
            if df["name"][i] == proj_name:
                _id = df["id"][i]
                break
        return _id

    def query_ds_id(self, ds_name):
        """
        function to query the dataset id with the help of function list_ds()
        :param ds_name:
        :return: id of the dataset
        :rtype: str
        """
        df = self.list_ds()
        _id = ""

        if df.empty:
            raise ValueError("no datasets found for this project")
        for i in range(len(df)):
            if df["name"][i] == ds_name:
                _id = df["id"][i]
                break
        return _id


class Project:

    def __init__(self, project_id):
        self._id = project_id


class Dataset:

    def __init__(self, connection, dataset_id=""):
        self.con = connection
        self._id_proj = connection.get_id()
        self._id = dataset_id
        self.ts = None
        self._header = {'dhi-open-api-key': '{0}'.format(connection.get_api_key()),
                        'Content-Type': 'application/json',
                        'dhi-project-id': '{0}'.format(self._id_proj),
                        'dhi-dataset-id': '{0}'.format(self._id),
                        'dhi-service-id': 'timeseries',
                        }

    # does not work yet
    def update_properties(self, properties, ts_name="", ts_id=""):
        if ts_name != "" and ts_id == "":
            ts_id = self.query_ts_id(ts_name)
            if ts_id == "":
                raise ValueError("timeseries of name {0} does not exist".format(ts_name))

        url = self.con.metadata_service_url + "api/ts/{0}/{1}".format(self._id, ts_id)

        def conv_json(prop):
            dict_ = {
                  "properties": prop
            }
            return dict_

        conv = conv_json(properties)
        body = json.dumps(conv)
        print(body)
        response = requests.put(url, headers=self._header, data=body)
        print("Status: ", response.status_code)
        json_ = response.json()
        return json_

    def get_id(self):
        return self._id

    def get_ds_info(self):
        """
        function to get dataset details
        :return: json with details
        """

        if self._id == "":
            raise ValueError("dataset id not set")

        url = self.con.metadata_service_url + "api/project/{0}/dataset/{1}".format(self._id_proj, self._id)
        response = requests.get(url, headers=self._header)
        print("Status: ", response.status_code)
        json_ = response.json()
        return json_

    def set_ds_id(self, dataset_id):
        """
        function to set dataset id if not defined in constructor
        :param dataset_id: id of dataset
        :type dataset_id: str
        """
        self._id = dataset_id

    def list_ts(self):
        """
        request all timeseries related to dataset it
        :return: dataframe with response
        :rtype: pd.DataFrame
        """
        url = self.con.metadata_service_url + "api/ts/{0}/timeseries/list".format(self._id)
        response = requests.get(url, headers=self._header)
        print("Status: ", response.status_code)
        resp_dict = response.json()["data"]
        df = pd.DataFrame(resp_dict)
        return df

    def query_ts_id(self, ts_name):
        """
        function to query timeseries id by its name
        :param ts_name: timeseries name
        :return: timeseries id
        :rtype: str
        """
        df = self.list_ts()
        _id = ""
        if df.empty:
            raise ValueError("no timeseries found for this dataset")
        for i in range(len(df)):
            if df["item"][i]["name"] == ts_name:
                _id = df["id"][i]
                break
        return _id

    def get_ts(self, ts_name="", ts_id=""):
        """
        function to get_ts by name or id and return a Timeseries object
        :param ts_name: timeseries name
        :param ts_id: timeseries id
        :return: Timeseries object
        :rtype: object
        """
        if ts_name != "" and ts_id == "":
            ts_id = self.query_ts_id(ts_name)
            if ts_id == "":
                raise ValueError("timeseries of name {0} does not exist".format(ts_name))
        if ts_id == "":
            raise ValueError("id of timeseries was not defined or does not exist")
        self.ts = Timeseries(dataset=self, timeseries_id=ts_id)
        return self.ts

    # muss noch auf properties angepasst werden
    def create_ts(self, name, unit="eumUmeter", item="eumIWaterLevel", data_type="Single", data_fields=None,
                  properties=None):
        """
        function to create a timeseries
        :param name: desired name of timeseries
        :type name: str
        :param unit: default "eumUmeter"
        :param item: default "eumIWaterLevel"
        :param data_type: default "Single"
        :param data_fields: additional values can be assigned of type single, text or flag
        :param properties: assign additional properties
        :return: returns an instance of Timeseries corresponding to the created one
        :rtype: object
        """
        if data_fields is None:
            data_fields = []
        if not isinstance(data_fields, list):
            raise ValueError('data fields must be of type list containing dictionaries, e.g. '
                             '[{"name": "example"},{"name": "example2"}]')
        if properties is None:
            properties = {}
        if not isinstance(properties, dict):
            raise ValueError("properties must be of type dictionary")
        url = self.con.metadata_service_url + "api/ts/{0}/timeseries".format(self._id)

        def conv_json(na, un, it, typ, fields, prop):
            dict_ = {
                "item": {
                    "name": na,
                    "unit": un,
                    "item": it,
                    "dataType": typ
                },
                "properties": prop,
                "dataFields": fields

            }
            return dict_

        conv = conv_json(name, unit, item, data_type, data_fields, properties)
        body = json.dumps(conv)
        response = requests.post(url, headers=self._header, data=body)
        print("Status: ", response.status_code)
        dict_resp = response.json()
        if response.status_code <= 400:
            raise ValueError("request failed")

        ts = Timeseries(dataset=self, timeseries_id=dict_resp["id"])
        return ts

    def del_ds(self):
        url = self.con.metadata_service_url + "api/project/{0}/dataset/{1}".format(self._id_proj, self._id)
        confirm = query_yes_no("Are you sure you want to delete " + self._id_proj + " ?")
        if confirm is True:
            response = requests.delete(url, headers=self.con.get_header())
            print("Status: ", response.status_code)

    def del_ts(self, timeseries_id="", timeseries_name=""):

        if timeseries_name != "" and timeseries_id == "":
            timeseries_id = self.query_ts_id(timeseries_name)
            if timeseries_id == "":
                raise ValueError("timeseries of name {0} does not exist".format(timeseries_name))

        confirm = query_yes_no("Are you sure you want to delete " + timeseries_name + " " + timeseries_id + " ?")
        if confirm is True:
            url = self.con.metadata_service_url + "api/ts/{0}/timeseries/{1}".format(self._id, timeseries_id)
            response = requests.delete(url, headers=self._header)
            print("Status: ", response.status_code)


class Timeseries:

    def __init__(self, dataset, timeseries_id):
        self.ds = dataset
        self._id = timeseries_id
        self._id_proj = self.ds.con.get_id()
        self._id_ds = self.ds.get_id()
        self._header = {'dhi-open-api-key': '{0}'.format(self.ds.con.get_api_key()),
                        'Content-Type': 'application/json',
                        'dhi-project-id': '{0}'.format(self._id_proj),
                        'dhi-dataset-id': '{0}'.format(self._id_ds),
                        'dhi-service-id': 'timeseries',
                        }

    def get_data(self, time_from=None, time_to=None):
        if time_from is None and time_to is None:
            url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values"\
                .format(self._id_ds, self._id)
        elif time_from is None and time_to is not None:
            url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values?to={2}"\
                .format(self._id_ds, self._id, time_to)
        elif time_from is not None and time_to is None:
            url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values?from={2}"\
                .format(self._id_ds, self._id, time_from)
        elif time_from is not None and time_to is not None:
            url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values?from={2}&to={3}"\
                .format(self._id_ds, self._id, time_from, time_to)

        response = requests.get(url, headers=self._header)
        print("Status: ", response.status_code)
        if response.status_code > 300 and time_from is not None and time_to is not None:
            raise ValueError("request failed. make sure times are in format {yyyy-MM-ddTHHmmss}")
        json_ = response.json()
        df = pd.DataFrame(json_["data"])
        df.rename(columns={0: 'timestamp', 1: 'value'}, inplace=True)

        return df

    # additional parameters not working yet
    def add_data(self, dataframe, prop=None):

        if prop is None:
            prop = []

        url = self.ds.con.metadata_service_url + "api/upload/{0}/timeseries/{1}/json".format(self._id_ds,
                                                                                             self._id)

        def conv_json(list_):
            dict_ = {"data": list_
                     }
            return dict_

        list_values = []

        if not prop:
            for i in range(len(dataframe)):
                list_values.append(["{0}".format(dataframe.timestamp.iloc[i]), dataframe.value.iloc[i]])

        else:
            prop_list = ["".format(dataframe.timestamp), dataframe.value]
            for i in range(len(dataframe)):
                for k in range(len(prop)):
                    prop_list.append(prop[k])
                list_values.append(prop_list)
        conv = conv_json(list_values)
        body = json.dumps(conv)

        response = requests.post(url, headers=self._header, data=body)
        print("Status: ", response.status_code)
        if response.status_code < 300:
            print("added {0} values to {1}".format(len(list_values), self._id))

    def get_info(self):
        url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}".format(self._id_ds, self._id)
        response = requests.get(url, headers=self._header)
        print("Status: ", response.status_code)
        resp_json = response.json()
        return resp_json

    def del_ts(self):
        pass

    def edit_prop(self):
        pass

    def add_json(self):
        pass

    def add_csv(self):

        pass

    def del_data(self, time_from, time_to):
        pass


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")
