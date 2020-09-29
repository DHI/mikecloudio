import json
import sys
import warnings
from pathlib import Path
# import matplotlib.pyplot as plt

import pandas as pd
import requests


class Timeseries:

    def __init__(self, dataset, id_timeseries="", name_timeseries=""):
        self.ds = dataset
        self._id_ds = self.ds._id
        self._id_proj = self.ds.con.project_id
        self._id = id_timeseries
        self._name = name_timeseries

        if id_timeseries == "" and name_timeseries != "":
            self._id = self.ds.query_ts_id(name_timeseries)
            self._name = name_timeseries

        elif id_timeseries != "" and name_timeseries == "":
            self._name = self.ds.query_ts_name(id_timeseries)
            self._id = id_timeseries

        if self._name == "" and self._id == "":
            warnings.warn("neither timeseries id nor timerseries name were not defined (at least one value required).")

        self._header = {'dhi-open-api-key': '{0}'.format(self.ds.con._api_key),
                        'Content-Type': 'application/json',
                        'dhi-project-id': '{0}'.format(self._id_proj),
                        'dhi-dataset-id': '{0}'.format(self._id_ds),
                        'dhi-service-id': 'timeseries',
                        }

    def get_data(self, time_from=None, time_to=None):
        """
        function to request data in timeseries
        :param time_from: specify from what timestamp data is requested; format: yyyy-mm-ddThhmmss.
        If None, will return from first timestamp.
        :param time_to: specify to what timestamp data is requested; format: yyyy-mm-ddThhmmss.
        If None, will return up to latest timestamp.
        :return: dataframe containing the timeseries data
        :rtype: pd.DataFrame
        """
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

                list_values.append(list_cur.copy())

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
                list_values.append(list_cur.copy())

        dict_ = {"data": list_values
                 }

        body = json.dumps(dict_)
        response = requests.post(url, headers=self._header, data=body)
        if response.status_code < 300:
            print("added {0} values to {1}".format(len(list_values), self._id))
        elif response.status_code == 500:
            raise ValueError("failed POST request: error source may be the amount of columns - must fit the amount of dataFields "
                             "defined in the timeseries attribute ")
        else:
            raise ValueError("failed POST request.")

    def add_csv(self, path, columns=None):
        """
        add data in form of a csv-file. Format required:
        1st column: timestamp, 2nd: main value, 3rd - nth: additional values according to defined dataFields
        in timeseries
        :param path: path of csv-file
        :type path: str
        :param columns: optional to define which columns to be added. Format:
        [<main value>, <additional values according to dataFields>, <additional values according to dataFields>, ...]
        :type columns: list
        """
        path = Path(path)
        df = pd.read_csv(path)
        df.set_index(df.columns[0], inplace=True)
        self.add_data(df, columns=columns)

    def get_info(self):
        """
        get detailled information about timeseries
        :return: a dictionary with the information
        :rtype: dict
        """
        url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}".format(self._id_ds, self._id)
        response = requests.get(url, headers=self._header)
        if response.status_code >= 300:
            raise ValueError("GET request failed")

        dict_ = response.json()
        return dict_

    def plot(self, time_from=None, time_to=None, columns=None):
        """
        function to plot data of the timeseries object
        :param time_from: specify from what timestamp data is requested; format: yyyy-mm-ddThhmmss.
        If None, will return from first timestamp.
        :param time_to: specify to what timestamp data is requested; format: yyyy-mm-ddThhmmss.
        If None, will return up to latest timestamp.
        :param columns: a list of the column names to be plotted
        :type list
        :return: figure to adapt visualization
        :rtype: matplotlib.figure.Figure
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
        fig, ax = plt.subplots()
        if columns is not None:
            if not isinstance(columns, list):
                raise ValueError("columns parameter must be a list")
            for i in range(len(columns)):
                df_data[columns[i]].plot(kind="line", ax=ax, title=self._name, legend=True)
        else:
            df_data.plot(kind='line', ax=ax, title=self._name, legend=True)
        plt.legend(loc='upper left', bbox_to_anchor=(1.0, 0.5))
        plt.close(fig)
        return fig

    def del_ts(self):
        """
        function to delete corresponding timeseries of the timeseries instance
        """
        confirm = query_yes_no("Are you sure you want to delete " + self._id + " ?")
        if confirm is True:
            url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}".format(self._id_ds, self._id)
            response = requests.delete(url, headers=self._header)
            if response.status_code >= 300:
                raise ValueError("deletion request failed")

    def del_data(self, time_from=None, time_to=None):
        """
        function to delete data from timeseries; if no 'to' time defined will delete all values to latest timestep
        :param time_from: specify from what timestamp data is deleted; format: yyyy-mm-ddThhmmss.
        If None, will delete all data from first timestamp.
        :param time_to: specify to what timestamp data is deleted; format: yyyy-mm-ddThhmmss.
        If None, will return up to latest timestamp.
        :return:
        """
        if time_to is None and time_from is not None:
            confirm = query_yes_no("Are you sure you want to delete all data from " + time_from + " in timeseries " + self._name + " ?")
            if confirm is True:
                url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values?from={2}" \
                    .format(self._id_ds, self._id, time_from)

        elif time_from is None and time_to is not None:
            confirm = query_yes_no("Are you sure you want to delete all data until " + time_to + " in timeseries " + self._name + " ?")
            if confirm is True:
                url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values?to={2}" \
                    .format(self._id_ds, self._id, time_to)

        elif time_from is None and time_to is None:
            confirm = query_yes_no("Are you sure you want to delete all data in timeseries " + self._name + " ?")
            if confirm is True:
                url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values" \
                    .format(self._id_ds, self._id)

        else:
            confirm = query_yes_no("Are you sure you want to delete all data from " + time_from + " to " + time_to + " in timeseries " + self._name + " ?")
            if confirm is True:
                url = self.ds.con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values?from={2}&to={3}" \
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