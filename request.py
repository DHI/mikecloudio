import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
import sys

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


class ConnectMikeCloud:
    metadata_service_url = "https://core-metadata-prod.azurewebsites.net/"
    url = metadata_service_url + "api/project/list"

    def __init__(self, api_key, projectID="", projectName="", project={}):
        self._api_key = api_key
        self._header = {'dhi-open-api-key': '{0}'.format(self._api_key)}
        self._uploadURL = ""

        if projectID != "" and projectName != "":
            self.proj = Project(projectID)
            self.projName = projectName
            self.projID = projectID

        else:
            print("yet to set project ID and project name")
            self.proj = project
            self.projID = projectID
            self.projName = projectName

    def getAllProjects(self):
        response = requests.get(self.url, headers=self._header)
        json_ = response.json()
        df = pd.DataFrame(json_["data"])
        return df

    def getUploadURL(self):
        url = self.metadata_service_url + "api/transfer/upload-url"
        response = requests.get(url, headers=self._header)
        print("Status: ", response.status_code)
        json_ = response.json()
        self._uploadURL = json_["data"]
        return json_["data"]

    def setProject(self, project_id, name=""):
        self.proj = Project(project_id)
        self.projID = project_id
        self.projName = name

    def listDataset(self):

        if self.projID == "" or self.projName == "":
            raise ValueError("set project ID and project name")

        url = self.metadata_service_url + "api/project/{0}/dataset/list-summaries".format(self.projID)
        print(url)
        response = requests.get(url, headers=self._header)
        json_ = response.json()
        print("Status: ", response.status_code)
        if response.status_code >= 300:
            print("json: ", json_)
        else:
            df = pd.DataFrame(json_["data"])

        return df

    def createDataset(self, name, description, properties={}, metadata={}, contentType="application/json"):
        if self.projID == "" or self.projName == "":
            raise ValueError("set project ID and project name with function setProject() first")
        header = {'dhi-open-api-key': '{0}'.format(self._api_key), 'Content-Type': '{0}'.format(contentType),
                  'dhi-project-id': '{0}'.format(self.projID), 'dhi-service-id': "timeseries"}

        url = self.metadata_service_url + "api/ts/dataset"

        def conv_json(name, description, properties, metadata):
            dict_ = {"timeSeriesSchema": {
                "properties": [
                ]
            },

                "datasetProperties": {
                    "name": name,
                    "description": description,
                    "metadata": metadata,
                    "properties": properties
                }

            }

            return dict_

        conv = conv_json(name, description, properties, metadata)
        body = json.dumps(conv)
        print(url)
        # print("json: ",body)

        response = requests.post(url, headers=header, data=body)
        print("Status: ", response.status_code)
        print(response)
        json_ = response.json()
        return json_

    #     def importDataset(self,name,description,readerName,writerName,readerParameters=None,writerParameters=None):
    #         if self.projID == "" or self.projName == "":
    #             raise ValueError("set project ID and project name with function setProject() first")
    #         url = self.metadata_service_url + "api/transfer/upload-convert"
    #         if self._uploadURL == "":
    #             raise ValueError("upload URL not set: call function getUploadURL()")
    #         def conv_json(name,description,readerName,writerName,readerParameters=None,writerParameters=None):
    #             dict_ = {"uploadURL": self._uploadURL,
    #                     "outputDatasetData":
    #                         {"name": name,
    #                          "description": description
    #                         },
    #                     "projectID": self.projID,
    #                     "readerParameters": [readerParameters],
    #                     "writerParameters:": [writerParameters],
    #                     "readerName": readerName,
    #                     "writerName": writerName}
    #             return dict_

    #         conv = conv_json(name,description,readerName,writerName,readerParameters,writerParameters)
    #         body = json.dumps(conv)

    #         print(url)
    #         #print("json: ",body)
    #         response = requests.get(url,headers=self._header,data=body)
    #         json_ = response.json()
    #         print("Status: ",response.status_code)
    #         print(json_)

    # need to add information on what the parameters are
    def setTemporalInformation(self, startTime, endTime, interval="string",
                               resolution="string"):  # ISO timeformat: "2020-05-27T11:12:57.126Z"
        dictTempInfo = {
            "startTime": startTime,
            "endTime": endTime,
            "interval": interval,
            "resolution": resolution
        }
        return dictTempInfo

    # need to add information on what the parameters arae
    def setSpatialInformation(self, location={}, primarySpatialReference="string", resolution="string", srid=0):
        dictSpatInfo = {
            "location": location,
            "primarySpatialReference": primarySpatialReference,
            "resolution": resolution,
            "srid": srid
        }
        return dictSpatInfo

    # updates a Dataset: not tested yet
    def updateDataset(self, datasetID, name, description, datasetType="file", temporalInformation={},
                      spatialInformation={}, additionalProperties={}, metadata={}):

        if self.projID == "" or self.projName == "":
            raise ValueError("set project ID and project name with function setProject() first")

        url = self.metadata_service_url + "api/project/{0}/dataset".format(self.projID)

        def conv_json(name, description, datasetType, datasetID, temporalInformation={}, spatialInformation={},
                      additionalProperties={}, metadata={}):
            dict_ = {
                "id": datasetID,
                "name": name,
                "description": description,
                "datasetType": datasetType,
                "temporalInformation": temporalInformation,
                "spatialInformation": spatialInformation,
                "metadata": metadata,
                "properties": additionalProperties,
                "tags": [
                    "string"
                ]
            }
            return dict_

        conv = conv_json(datasetID, name, description, datasetType, temporalInformation={}, spatialInformation={},
                         additionalProperties={}, metadata={})
        body = json.dumps(conv)

        print(url)
        # print("json: ",body)

        response = requests.put(url, headers=self._header, data=body)
        print("Status: ", response.status_code)
        print(response)
        json_ = response.json()

    def delDataset(self, datasetID):
        confirm = query_yes_no("Are you sure you want to delete " + self.projID + " ?")
        if confirm == True:
            url = self.metadata_service_url + "api/project/{0}/dataset/{1}".format(self.projID, datasetID)
            response = requests.delete(url, headers=self._header)
        print("Status: ", response.status_code)

    def queryDatasetByName(self):
        pass


class Project:

    def __init__(self, project_id):
        self.project_id = project_id

    def __repr__(self):
        return str(self)


class Dataset():

    def __init__(self, connection, dataset_id=None):
        self._con = connection
        self.projID = self._con.projID
        self.projName = self._con.projName
        self._dataset_id = dataset_id
        self._header = {'dhi-open-api-key': '{0}'.format(self._con._api_key),
                        'Content-Type': 'application/json',
                        'dhi-project-id': '{0}'.format(self.projID),
                        'dhi-dataset-id': '{0}'.format(self._dataset_id),
                        'dhi-service-id': 'timeseries',
                        }
        self._body = ''
        self.ts = {}

    def addAdditionalProperty(self):
        pass

    def getDatasetInfo(self):
        if self._dataset_id is None:
            print("No datasetID defined yet")
            pass
        self.url = self._con.metadata_service_url + "api/project/{0}/dataset/{1}".format(self.projID, self._dataset_id)
        print(self.url)
        response = requests.get(self.url, headers=self._header)
        print("Status: ", response.status_code)
        json_ = response.json()
        df = pd.DataFrame(json_)

        return json_

    def setDatasetID(self, datasetID):
        self._dataset_id = datasetID

    def getTimeseries(self):
        url = self._con.metadata_service_url + "api/ts/{0}/timeseries/list".format(self._dataset_id)
        print(url)
        response = requests.get(url, headers=self._header)
        print("Status: ", response.status_code)
        json_ = response.json()
        return json_

    # muss noch auf properties angepasst werden
    def createTimeseries(self, name, unit="eumUmeter", item="eumIWaterLevel", dataType="Single", dataFields=None,
                         properties={}):

        url = self._con.metadata_service_url + "api/ts/{0}/timeseries".format(self._dataset_id)
        print(url)

        def conv_json(name, unit, item, dataType, dataFields):
            dict_ = {"item":
                         {"name": name,
                          "unit": unit,
                          "item": item,
                          "dataType": dataType},
                     "dataFields": []}
            return dict_

        conv = conv_json(name, unit, item, dataType, dataFields)
        body = json.dumps(conv)

        print(body)
        response = requests.post(url, headers=self._header, data=body)
        print("Status: ", response.status_code)

        json_ = response.json()
        df = json_normalize(json_)

        return df, json_

    def delDataset(self):
        url = self._con.metadata_service_url + "api/project/{0}/dataset/{1}".format(self.projID, self._dataset_id)
        confirm = query_yes_no("Are you sure you want to delete " + self.projID + " ?")
        if confirm == True:
            response = requests.delete(url, headers=self._con._header)
            print("Status: ", response.status_code)

    def delTimeseries(self, tsID):
        url = self._con.metadata_service_url + "api/ts/{0}/timeseries/{1}".format(self._dataset_id, tsID)
        print(url)
        confirm = query_yes_no("Are you sure you want to delete " + tsID + " ?")
        if confirm == True:
            response = requests.delete(url, headers=self._header)
            print("Status: ", response.status_code)

    def queryTimeseriesByName(self):
        pass


class Timeseries():

    def __init__(self, dataset, timeseriesID):
        self.DS = dataset
        self.tsID = timeseriesID
        self.projID = self.DS._con.projID
        self._dataset_id = self.DS._dataset_id
        self._header = {'dhi-open-api-key': '{0}'.format(self.DS._con._api_key),
                        'Content-Type': 'application/json',
                        'dhi-project-id': '{0}'.format(self.projID),
                        'dhi-dataset-id': '{0}'.format(self._dataset_id),
                        'dhi-service-id': 'timeseries',
                        }

    def getTSData(self, time_from=None, time_to=None):
        url = self.DS._con.metadata_service_url + "api/ts/{0}/timeseries/{1}/values".format(self._dataset_id, self.tsID)
        print(url)
        response = requests.get(url, headers=self._header)
        print("Status: ", response.status_code)
        json_ = response.json()
        df = pd.DataFrame(json_)

        return json_, df


    # additional parameters not working yet
    def addDataDF(self, df, addProperties=[]):
        url = self.DS._con.metadata_service_url + "api/upload/{0}/timeseries/{1}/json".format(self._dataset_id,
                                                                                              self.tsID)

        def conv_json(list_):
            dict_ = {"data":
                         list_
                     }
            return dict_

        list_values = []

        if addProperties == []:
            for i in range(len(df)):
                list_values.append(["{0}".format(df.timestamp.iloc[i]), df.value.iloc[i]])

        else:
            prop_list = ["".format(df.timestamp), df.value]
            for i in range(len(df)):
                for k in range(len(addProperties)):
                    prop_list.append(addProperties[k])
                list_values.append(prop_list)
        conv = conv_json(list_values)
        body = json.dumps(conv)

        response = requests.post(url, headers=self._header, data=body)
        print("Status: ", response.status_code)
        print("added {0} values to {1}".format(len(list_values), self.tsID))

    def delTimeseries(self):
        pass

    def editTSProperties(self):
        pass

    def getTSDetails(self):
        pass

    def addDataJSON(self):

        pass

    def addDataCSV(self):

        pass

    def delData(self, time_from, time_to):
        pass
