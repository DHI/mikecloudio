import json
import warnings

import pandas as pd
import requests

from mikecloudio.timeseries import Timeseries, query_yes_no


class Dataset:

    def __init__(self, connection, id_dataset="", name_dataset=""):
        self.con = connection
        self._id_proj = connection.project_id
        self._id = id_dataset
        self._name = name_dataset

        if id_dataset == "" and name_dataset != "":
            self._id = self.con.query_ds_id(name_dataset)
            self._name = name_dataset

        elif id_dataset != "" and name_dataset == "":
            self._name = self.con.query_ds_name(id_dataset)
            self._id = id_dataset

        if self._id == "" and self._name == "":
            warnings.warn("neither dataset id nor dataset name were not defined (at least one value required).")

        self.ts = None
        self._header = {'dhi-open-api-key': '{0}'.format(connection._api_key),
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
        """
        Getter function for dataset ID
        :return: ID of instance
        """
        return self._id

    def get_info(self, extended=False):
        """
        function to get dataset details
        :param extended: set to True if extra information needed.
        :type extended: bool
        :return: dictionary with details
        :rtype: dict
        """

        if self._id == "":
            raise ValueError("dataset id not set")

        if extended is True:
            url = self.con.metadata_service_url + "api/ts/{0}".format(self._id)
        else:
            url = self.con.metadata_service_url + "api/project/{0}/dataset/{1}".format(self._id_proj, self._id)
        response = requests.get(url, headers=self._header)
        dict_ = response.json()
        return dict_

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
        :return: dataframe with all timeseries in dataset
        :rtype: pd.DataFrame
        """
        url = self.con.url + "api/ts/{0}/timeseries/list".format(self._id)
        response = requests.get(url, headers=self._header)
        if response.status_code >= 400:
            raise ValueError("request failed")
        resp_dict = response.json()["data"]
        df = pd.DataFrame(resp_dict)
        return df

    def query_ts_id(self, name):
        """
        function to query timeseries ID by its name
        :param name: timeseries name
        :return: timeseries ID
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

    def query_ts_name(self, id):
        """
        function to query timeseries name by its ID
        :param id: timeseries ID
        :return: timeseries name
        :rtype: str
        """
        df = self.list_ts()
        _name = ""
        if df.empty:
            raise ValueError("no timeseries found for this dataset")

        for i in range(len(df)):
            if df["id"][i] == id:
                _name = df["item"][i]["name"]

        if _name == "":
            raise ValueError("timeseries with id {0} does not exist".format(id))
        return _name

    def check_ts_exist(self, name):
        """
        function to check if timeseries exists in dataset
        :param name: name of timeseries
        :return: state true if exists otherwise false
        :rtype: bool
        """
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
        :type name: str
        :param id: timeseries id
        :type id: str
        :return: Timeseries object
        :rtype: Timeseries
        """
        if name != "" and id == "":
            id = self.query_ts_id(name)

        if id == "":
            raise ValueError("id of timeseries was not defined or does not exist")
        self.ts = Timeseries(dataset=self, id_timeseries=id, name_timeseries=name)
        return self.ts

    # muss noch auf properties angepasst werden
    def create_ts(self, name, unit="eumUmeter", item="eumIWaterLevel", data_type="Single", data_fields=None,
                  properties=None):
        """
        function to create a timeseries
        :param name: desired name of timeseries
        :type name: str
        :param unit: unit value - must be part of DHI convention
        :type unit: str
        :param item: parameter value - must be part of DHI convention
        :type item: str
        :param data_type: accepted data types: Text, Date, Int32, Int64, Single, Double, Int16
        :param data_fields: define how many columns the timeseries comprises and their names and dataTypes.
        List of dictionaries, example: [{"name": "pressure", "dataType": "Double"},..,{}]
        possible data field dataTypes: DateTime, Single, Double, Flag, Text
        :type data_fields: list
        :param properties: assign additional properties for the timeseries as defined in the dataset timeseries schema
        :return: returns an instance of Timeseries corresponding to the created one
        :rtype: Timeseries
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
        ts = Timeseries(dataset=self, id_timeseries=dict_resp["id"])
        return ts

    def del_ds(self):
        """
        function to delete dataset of the current instance
        """
        url = self.con.metadata_service_url + "api/project/{0}/dataset/{1}".format(self._id_proj, self._id)
        confirm = query_yes_no("Are you sure you want to delete " + self._id_proj + " ?")
        if confirm is True:
            response = requests.delete(url, headers=self.con.header())
            if response.status_code >= 300:
                raise ValueError("deletion request failed")

    def del_ts(self, name="", id=""):
        """
        function to delete a timeseries based on name or id
        :param name: name of timeseries
        :param id: ID of timeseries
        """
        if name != "" and id == "":
            id = self.query_ts_id(name)

        confirm = query_yes_no("Are you sure you want to delete " + name + " " + id + " ?")
        if confirm is True:
            url = self.con.metadata_service_url + "api/ts/{0}/timeseries/{1}".format(self._id, id)
            response = requests.delete(url, headers=self._header)
            if response.status_code >= 300:
                raise ValueError("deletion request failed")