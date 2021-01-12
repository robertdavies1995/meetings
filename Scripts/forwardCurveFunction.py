import logging

import numpy as np
import pandas as pd
import azure.storage.blob as blb
from io import StringIO
import datetime
import datedelta


def openLatestBlob(blobContainer, blobName):
    for file in container.list_blobs():
        if file.name[11:13] == blobName:
            date.append(datetime.datetime.strptime(file.name[0:10], '%Y/%m/%d'))
    for file in container.list_blobs():
        if file.name[11:13] == blobName and datetime.datetime.strptime(file.name[0:10], '%Y/%m/%d') == max(date):
            fileIn = file.name
    string = str(blob.get_blob_client(blobContainer, fileIn).download_blob().readall(), 'utf-8')
    return pd.read_csv(StringIO(string))


def bestfitforwardcurve(dataframe):
    logging.info('Started BestFitForwardCurve Function..')

    quarters = []
    delta = []
    df = dataframe
    df['Date'] = df['DateNo']
    df['DF_date'] = df['DateNo']

    logging.info('Calculating Forward Curve Values..')

    for i in range(0, len(df) - 1):
        df.loc[i, 'Interpolation'] = (df.loc[i + 1, 'DF'] - df.loc[i, 'DF']) / (
                    df.loc[i + 1, 'Date'] - df.loc[i, 'Date'])
    today = datetime.date.today() + datetime.timedelta(days=2)
    for i in range(0, 41):
        quarters.append(today + datedelta.datedelta(months=3 * i))
    for i in quarters:
        delta.append(i - deltadate)
    fc = pd.DataFrame(data={'Date': delta})
    fc['Date'] = fc['Date'].astype('timedelta64[D]')
    fr = pd.merge_asof(fc, df, 'Date')
    fr.loc[0, 'InterpDF'] = 1
    for i in range(1, len(fr)):
        fr.loc[i, 'InterpDF'] = fr.loc[i, 'DF'] + (
                    (fr.loc[i, 'Date'] - fr.loc[i, 'DF_date']) * fr.loc[i, 'Interpolation'])
    for i in range(0, len(fr) - 1):
        fr.loc[i, 'Forward Rate'] = 100 * ((360 * ((fr.loc[i, 'InterpDF'] / fr.loc[i + 1, 'InterpDF']) - 1)) / (
                    fr.loc[i + 1, 'Date'] - fr.loc[i, 'Date']))
    forwardcurve = fr.drop(['DF', 'DF_date', 'Interpolation', 'InterpDF'], axis=1)
    forwardcurve = forwardcurve.dropna(subset=['Forward Rate'])
    forwardcurve = forwardcurve.reset_index(drop=True)

    logging.info('Forward Curve Values Calculated')
    logging.info('Calculating Polynomial Coefficients..')

    polycoefficient = np.polyfit(forwardcurve['Date'], forwardcurve['Forward Rate'], 4)

    logging.info('Polynomial Coefficients Calculated')
    logging.info('Calculating Polynomial..')

    for i in range(0, len(forwardcurve)):
        forwardcurve.loc[i, 'x4'] = (forwardcurve.loc[i, 'Date'] ** 4) * polycoefficient[0]
        forwardcurve.loc[i, 'x3'] = (forwardcurve.loc[i, 'Date'] ** 3) * polycoefficient[1]
        forwardcurve.loc[i, 'x2'] = (forwardcurve.loc[i, 'Date'] ** 2) * polycoefficient[2]
        forwardcurve.loc[i, 'x'] = (forwardcurve.loc[i, 'Date']) * polycoefficient[3]
        forwardcurve.loc[i, 'c'] = polycoefficient[4]
        forwardcurve.loc[i, 'Poly'] = forwardcurve.loc[i, 'x4'] + forwardcurve.loc[i, 'x3'] + forwardcurve.loc[
            i, 'x2'] + forwardcurve.loc[i, 'x'] + forwardcurve.loc[i, 'c']

    logging.info('Polynomial Line Calculated')

    return forwardcurve[['Date', 'Poly']].to_csv(index_label='Index', encoding='utf-8')
    logging.info('Saved Polynomial Forward Curve Values for {today} to Blob'.format(today=datetime.date.today()))


def uploadBlob(content, blobContainer, blobName, fileDate):
    blob_up = blob.get_blob_client(blobContainer, '{y}/{m}/{d}/{y}{m}{d}_{name}.csv'.format(dt=fileDate.date(), y=fileDate.year,
                                                                                     m='{:02d}'.format(fileDate.month), d='{:02d}'.format(fileDate.day),
                                                                                     name=blobName))
    blob_up.upload_blob(content, blob_type="BlockBlob", overwrite=True)


date = []
deltadate = datetime.date(1899, 12, 30)

logging.info('Connecting to Blob..')

connString = r'blobConnection'
blob = blb.BlobServiceClient.from_connection_string(conn_str=connString)

container = blb.ContainerClient.from_connection_string(conn_str=connString, container_name='bbg')
logging.info('Connected to Blob')

logging.info('Opening Discount Rates File..')

df = openLatestBlob('bbg', 'DF')

logging.info('Formatting Discount Rates File..')

df['tenor'] = pd.to_datetime(df['tenor'], format='%Y-%m-%d')
DF = df[['series_id', 'value', 'tenor']]
for i in range(0, len(DF)):
    DF.loc[i, 'tenorValue'] = pd.to_numeric(DF.loc[i, 'series_id'].split(' ')[1][:-1])
    DF.loc[i, 'tenorType'] = DF.loc[i, 'series_id'].split(' ')[1][-1:]
    DF.loc[i, 'Months'] = [DF.loc[i, 'tenorValue'] * 12 if x == 'Y' else DF.loc[i, 'tenorValue'] for x in
                           DF.loc[i, 'tenorType']]
    DF.loc[i, 'Date'] = [DF.loc[i, 'tenor'] + pd.DateOffset(days=DF.loc[i, 'tenorValue']) if x == 'D' else DF.loc[
                                                                                                               i, 'tenor'] + pd.DateOffset(
        months=DF.loc[i, 'Months']) for x in DF.loc[i, 'tenorType']]
    DF['Date'] = pd.to_datetime(DF['Date'], format='%Y-%m-%d').dt.date
    DF.loc[i, 'DateNo'] = (DF.loc[i, 'Date'] - deltadate).days
DF['DF'] = DF['value']
DF = DF[['DateNo', 'DF']].sort_values(by=['DateNo'])
DF = DF.reset_index(drop=True)

logging.info('Formatting Discount Rates Completed')

output = bestfitforwardcurve(DF)

logging.info('Uploading to Azure Blob Storage')

uploadBlob(output, 'bbg', 'ForwardCurve', max(date))

logging.info('Function Complete')
