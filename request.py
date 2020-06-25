import requests
import json
import pandas as pd
import sys
import matplotlib.pyplot as plt
from pathlib import Path
import warnings


class ConnectMikeCloud:
    metadata_service_url = "https://core-metadata-prod.azurewebsites.net/"

    def __init__(self, api_key, id_proj=None, name_proj=None, ds_object=None):
        """
        this class creates a connection to MIKE CLOUD and can be used to list get all projects, get datasets of projects
        create, update, and delete datasets
        :param api_key: api key that gives access to desired projects
        :type api_key: str
        :param id_proj: project ID
        :type id_proj: str
        :param name_proj: name of the project
        :type name_proj: str
        """

        self._api_key = api_key
        self._header = {'dhi-open-api-key': '{0}'.format(self._api_key)}
        self._uploadURL = ""
        self.ds = ds_object

        if id_proj is None:
            self._id_proj = self.query_proj_id(name_proj)

        else:
            self._id_proj = id_proj
            self._name_proj = name_proj

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
        if response.status_code >= 300:
            raise ValueError("request failed: check api key")
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
        :rtype: object
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
        :return:
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
        if response.status_code >= 300:
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
        :param prop_ts: the properties defined for the timeseries:
        <name> and <dataType> must be included in the prop_ts dictionary.
        Every property defined here must be defined in create_ts() properties too.
        :type prop_ts: dict
        :param content_type: generally set to application/json;
        other options: text/plain, text/csv, text/json etc. (see api docs)
        :type content_type: str 
        :return: returns a new Dataset object
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
        if response.status_code >= 300:
            print("Status: ", response.status_code)
            raise ValueError("request failed")
        json_ = response.json()
        ds = Dataset(connection=self, dataset_id=json_["id"])
        return ds

    def get_ds(self, name="", id=""):
        """
        function to create a Dataset object according the project id / or project name
        :param id:
        :param name:
        :return: Dataset object
        :rtype: object
        """
        if name != "" and id == "":
            id = self.query_ds_id(name)
            if id == "":
                raise ValueError("dataset of name {0} does not exist".format(name))
        if id == "":
            raise ValueError("id of dataset was not defined or does not exist")
        self.ds = Dataset(connection=self, dataset_id=id)
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
            if response.status_code >= 300:
                raise ValueError("request failed")

    def query_proj_id(self, name):
        """
        function to query the project id with the help of function list_projects()
        :param name:
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

    def query_ds_id(self, name):
        """
        function to query the dataset id with the help of function list_ds()
        :param name:
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
    def update_properties(self, properties, name="", id=""):
        if name != "" and id == "":
            id = self.query_ts_id(name)
            if id == "":
                raise ValueError("timeseries of name {0} does not exist".format(name))

        url = self.con.metadata_service_url + "api/ts/{0}/{1}".format(self._id, id)

        dict_ = {
              "properties": properties
        }

        body = json.dumps(dict_)
        response = requests.put(url, headers=self._header, data=body)
        if response.status_code >= 300:
            raise ValueError("request failed")

        json_ = response.json()
        return json_

    def get_id(self):
        return self._id

    def get_info(self, extended=False):
        """
        function to get dataset details
        :return: json with details
        """

        if self._id == "":
            raise ValueError("dataset id not set")

        if extended is True:
            url = self.con.metadata_service_url + "api/ts/{0}".format(self._id)
        else:
            url = self.con.metadata_service_url + "api/project/{0}/dataset/{1}".format(self._id_proj, self._id)
        response = requests.get(url, headers=self._header)
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
        if response.status_code >= 400:
            raise ValueError("request failed")
        resp_dict = response.json()["data"]
        df = pd.DataFrame(resp_dict)
        return df

    def query_ts_id(self, name):
        """
        function to query timeseries id by its name
        :param name: timeseries name
        :return: timeseries id
        :rtype: str
        """
        df = self.list_ts()
        _id = ""
        if df.empty:
            raise ValueError("no timeseries found for this dataset")

        count = 0

        for i in range(len(df)):
            if df["item"][i]["name"] == name:
                _id = df["id"][i]
                count += 1
            if count >= 2:
                warning = "Warning: {0} timeseries with name '{1}' exist. Choose by ID to avoid errors"\
                    .format(count, name)
                warnings.warn(warning)

        if _id == "":
            raise ValueError("timeseries of name {0} does not exist".format(name))
        return _id

    def check_ts_exist(self, name):
        df = self.list_ts()
        state = False
        if df.empty:
            print("no timeseries found for this dataset")
            state = False
            return state

        for i in range(len(df)):
            if df["item"][i]["name"] == name:
                state = True
                break
        return state

    def get_ts(self, name="", id=""):
        """
        function to get_ts by name or id and return a Timeseries object
        :param name: timeseries name
        :param id: timeseries id
        :return: Timeseries object
        :rtype: object
        """
        if name != "" and id == "":
            id = self.query_ts_id(name)

        if id == "":
            raise ValueError("id of timeseries was not defined or does not exist")
        self.ts = Timeseries(dataset=self, timeseries_id=id)
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
        :type data_fields: list
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

        js = self.get_info(extended=True)
        state = False
        for key in properties:
            for i in range(len(js["timeSeriesProperties"])):
                if key == js["timeSeriesProperties"][i]["name"]:
                    state = True
            if not state:
                raise ValueError(
                    "properties name must fit to name defined in dataset - timeSeriesProperties: \n{0}".format(
                        js["timeSeriesProperties"]))
        datafield_types = ["DateTime", "Single", "Double", "Flag", "Text"]
        format_cor = False
        for i in range(len(data_fields)):
            for j in range(len(datafield_types)):
                if data_fields[i]["dataType"] == datafield_types[j]:
                    format_cor = True
            if not format_cor:
                raise ValueError(
                    "dataTypes of data fields must be of the following types: \n{0}".format(
                        datafield_types))

        url = self.con.metadata_service_url + "api/ts/{0}/timeseries".format(self._id)

        dict_ = {
            "item": {
                "name": name,
                "unit": unit,
                "item": item,
                "dataType": data_type
            },
            "properties": properties,
            "dataFields": data_fields

        }

        body = json.dumps(dict_)
        response = requests.post(url, headers=self._header, data=body)
        if response.status_code == 500 and properties is not None:
            print("Status: ", response.status_code)
            raise ValueError("request failed: "
                             "make sure that dataType of dataset-timeseriesProperties fits to data type in properties")
        if response.status_code >= 300:
            print("Status: ", response.status_code)
            raise ValueError("request failed")

        dict_resp = response.json()
        ts = Timeseries(dataset=self, timeseries_id=dict_resp["id"])
        return ts

    def del_ds(self):
        url = self.con.metadata_service_url + "api/project/{0}/dataset/{1}".format(self._id_proj, self._id)
        confirm = query_yes_no("Are you sure you want to delete " + self._id_proj + " ?")
        if confirm is True:
            response = requests.delete(url, headers=self.con.get_header())
            if response.status_code >= 300:
                raise ValueError("deletion request failed")

    def del_ts(self, name="", id=""):

        if name != "" and id == "":
            id = self.query_ts_id(name)

        confirm = query_yes_no("Are you sure you want to delete " + name + " " + id + " ?")
        if confirm is True:
            url = self.con.metadata_service_url + "api/ts/{0}/timeseries/{1}".format(self._id, id)
            response = requests.delete(url, headers=self._header)
            if response.status_code >= 300:
                raise ValueError("deletion request failed")


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
        url = None
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

        if response.status_code > 300 and time_from is not None or response.status_code > 300 and time_to is not None:
            raise ValueError("request failed - validate that times are given in format {yyyy-MM-ddTHHmmss}")

        json_ = response.json()
        if response.status_code > 300:
            return json_
        df = pd.DataFrame(json_["data"])
        js = self.get_info()

        columns = {0: "timestamp", 1: js["item"]["item"]}

        for i in range(2, len(df.columns)):
            columns[i] = js["dataFields"][i-2]["name"]

        df.rename(columns=columns, inplace=True)

        return df

    def add_data(self, dataframe, columns=None):
        """
        add data to Mike Cloud API in form of a dataframe
        :param dataframe: dataframe containing data with timestamp as index; order of columns
        must correspond to 1st: main value, 2-nth: dataFields order
        :type dataframe: pandas.core.frame.DataFrame
        :param columns: list of names of additional columns within the dataframe;
        list values must correspond to 1st: main value, 2-nth: dataFields order
        :type columns: list
        :return:
        """
        url = self.ds.con.metadata_service_url + "api/upload/{0}/timeseries/{1}/json".format(self._id_ds,
                                                                                             self._id)
        if 0 in dataframe.index:
            raise ValueError("dataframe index must be set to timestamp")

        list_values = []
        list_cur = []
        js = self.get_info()

        if not columns:
            if len(dataframe.columns)-1 != len(js["dataFields"]):
                raise ValueError("dataframe amount of columns must fit to dataFields defined in timeseries: "
                                 "specify columns or adjust dataframe size (example: "
                                 "2 dataFields are defined plus the main value -> dataframe must contain 3 columns.\n "
                                 "Defined DataFields: \n{0}".format(js["dataFields"]))
            for i in range(len(dataframe)):
                list_cur.clear()
                list_cur.append("{0}".format(dataframe.index[i]))
                for j in range(len(dataframe.columns)):
                    list_cur.append(dataframe.iloc[i][j])
                    if j < len(dataframe.columns)-1:
                        if js["dataFields"][j]["name"] != dataframe.columns[j+1]:
                            warning = "make sure order of columns correspond to 1st: main value, 2-nth: " \
                                      "dataFields order.\nDefined DataFields: \n{0}".format(js["dataFields"])
                            warnings.warn(warning)

                list_values.append(list_cur)

        else:
            if len(columns)-1 != len(js["dataFields"]):
                raise ValueError("Amount of columns must fit to dataFields defined in timeseries: "
                                 "specify columns or adjust dataframe size (example: "
                                 "2 dataFields are defined plus the main value -> 3 columns must be given).\n"
                                 "Defined DataFields: \n{0}".format(js["dataFields"]))
            for i in range(len(dataframe)):
                list_cur.clear()
                list_cur.append("{0}".format(dataframe.index[i]))
                for j in range(len(columns)):
                    list_cur.append(dataframe[columns[j]].iloc[i])
                    if j < len(dataframe.columns)-1:
                        if js["dataFields"][j]["name"] != dataframe.columns[j + 1]:
                            warning = "make sure order of columns correspond to 1st: main value, 2-nth: " \
                                      "dataFields order.\nDefined DataFields: \n{0}".format(js["dataFields"])
                            warnings.warn(warning)
                list_values.append(list_cur)

        dict_ = {"data": list_values
                 }

        body = json.dumps(dict_)
        response = requests.post(url, headers=self._header, data=body)
        if response.status_code < 300:
            print("added {0} values to {1}".format(len(list_values), self._id))
        elif response.status_code == 500:
            raise ValueError("failed POST request: the amount of columns must fit the amount of dataFields "
                             "defined in the timeseries attribute ")
        else:
            raise ValueError("failed POST request.")

    def add_csv(self, path, columns=None):
        path = Path(path)
        df = pd.read_csv(path)
        df.set_index(df.columns[0], inplace=True)
        self.add_data(df, columns=columns)

    def get_info(self):
        url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}".format(self._id_ds, self._id)
        response = requests.get(url, headers=self._header)
        if response.status_code >= 300:
            raise ValueError("GET request failed")

        resp_json = response.json()
        return resp_json

    def plot(self, time_from=None, time_to=None):
        """
        function to plot data of the timeseries object
        :param time_from: define from what timesteü to plot
        :param time_to: define to what timestep to plot
        """
        df = None
        if time_from is None and time_to is None:
            df = self.get_data()

        elif time_from is None and time_to is not None:
            df = self.get_data(time_to=time_to)

        elif time_from is not None and time_to is None:
            df = self.get_data(time_from=time_from)

        elif time_from is not None and time_to is not None:
            df = self.get_data(time_from=time_from, time_to=time_to)
        if df.empty:
            raise ValueError("no data in timeseries")

        for j in range(1, len(df.columns)):
            for i in range(len(df)):
                if isinstance(df.iloc[:, j][i], str):
                    df.iloc[:, j][i] = None

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df_data = df.set_index("timestamp")
        ax = plt.gca()
        df_data.plot(kind='line', ax=ax)
        plt.show()

    def del_ts(self):
        confirm = query_yes_no("Are you sure you want to delete " + self._id + " ?")
        if confirm is True:
            url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}".format(self._id_ds, self._id)
            response = requests.delete(url, headers=self._header)
            if response.status_code >= 300:
                raise ValueError("deletion request failed")

    def del_data(self, time_from, time_to=None):
        """
        function to delete data from timeseries; if no 'to' time defined will delete all values to latest timestep
        :param time_from:
        :param time_to:
        :return:
        """
        if time_to is None:
            url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values?from={2}" \
                .format(self._id_ds, self._id, time_from)
        else:
            url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values?from={2]&to={3}" \
                .format(self._id_ds, self._id, time_from, time_to)

        response = requests.delete(url, headers=self._header)
        if response.status_code > 300:
            raise ValueError("request failed. make sure times are in format {yyyy-MM-ddTHHmmss}")


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
