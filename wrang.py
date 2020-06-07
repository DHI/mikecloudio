from datetime import datetime, timedelta, timezone, date
import pandas as pd


def importExcel(path, columnOfValue, columnOfTimestamp, timezone_hr=2, timezone_name="MUN"):
    df = pd.read_excel(path)
    df.rename(columns={columnOfValue: 'value', columnOfTimestamp: 'timestamp'}, inplace=True)
    # munTimeDelta = timedelta(hours=timezone_hr)
    # munTZObject = timezone(munTimeDelta, name=timezone_name)

    tStamp = df["timestamp"]
    df.timestamp.apply(
        lambda x: datetime.isoformat(x))
    # for i in range(len(tStamp)):
    # tStamp_utc = tStamp.replace(tzinfo=munTZObject)
    # df.timestamp = datetime.isoformat(tStamp[i])

    return df


def splitDF(df,distinctFeature):

    UniqueNames = df[distinctFeature].unique()

    # create a data frame dictionary to store your data frames
    DataFrameDict = {elem : pd.DataFrame for elem in UniqueNames}
    list_=[]
    for key in DataFrameDict.keys():
        DataFrameDict[key] = df[:][df[distinctFeature] == key].reset_index()
        list_.append(key)
    return DataFrameDict,list_


# need to add information on what the parameters are
def set_temp_info(start_time, end_time, interval="string", resolution="string"):
    """
    function to create a dictionary with typicial values of temporal information for the dataset
    :param start_time:
    :param end_time:
    :param interval:
    :param resolution:
    :return: dictionary with temporal information
    :rtype: dict
    """
    # ISO timeformat: "2020-05-27T11:12:57.126Z"
    temp_info = {
        "startTime": start_time,
        "endTime": end_time,
        "interval": interval,
        "resolution": resolution
    }
    return temp_info


# need to add information on what the parameters are
def set_spat_info(location=None, primary_spatial_reference="string", resolution="string", srid=0):
    """
    function to create a dictionary with typicial values of spatial information for the dataset
    :param location:
    :param primary_spatial_reference:
    :param resolution:
    :param srid:
    :return: returns dictionary of spatial information
    :rtype: dict
    :
    """
    if location is None:
        location = {}

    spat_info = {
        "location": location,
        "primarySpatialReference": primary_spatial_reference,
        "resolution": resolution,
        "srid": srid
    }
    return spat_info
