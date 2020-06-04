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