import pandas as pd
from datetime import datetime
import time
from pandas.tseries.offsets import BDay
import zipfile
from requests import get
from io import BytesIO, StringIO

import csv

CCYs=["EUR","USD","GBP"]

def Main():
    print "Do Something here"
    
def iCap():
    url='http://www3.icap.com/sef/marketdata/ussef/ICAPSEFMarketDataUSSEF.csv'
    
    
    Dates=[]
    Indexes=[]
    
    data = pd.read_csv(url,sep='|')
    

    fData=data[(data["Asset_Class"]=="EQ") & (data["Sub_Prod"]=="VAR")& (data["Traded_Currency"].isin(CCYs))]
    
    Result=pd.DataFrame()#index=3, columns=["Ticker","Maturity"])
    Result["Ticker"]=fData["Internal_Prod_ID"]

        
    
    for Trade in fData["Internal_Prod_ID"]:
        index= Trade.split('_',3)[1]
        index=index.split(' ',1)[0]
        
        eDate= Trade.split('_',3)[2]
        eDate=eDate.split('X',2)[1]
        eDate=datetime.strptime(eDate,"%y%m%d")
        Dates.append(eDate.strftime("%b-%y"))
        Indexes.append(index)
        
        
        
        
        
    Result["Maturity"]=Dates
    Result["Ticker"]=Indexes
    Result["Notional"]=fData["Notional_Traded_Currency_NDA"]
    Result["SEF"]="ICAP"
#    print data.loc[()]

    return Result
    
def Tradition():
    today = pd.datetime.today()
    dt= (today - BDay(1)).strftime("%Y%m%d")
    
    print dt
    url='http://www.traditionsef.com/dailyactivity/SEF16_MKTDATA_TFSU_'+dt+'.csv'
    data = pd.read_csv(url,sep='|')
    
    print data.columns.values
    
    
    
    fData=data[(data["Asset_Class"]=="EQ") & (data["Sub_Prod"]=="VAR")& (data["Traded_Currency"].isin(CCYs))]
    
    print fData["Last_Price"]
    
    for trade in fData["Internal_Prod_Des"]:
        print trade
        
        
def GetDTCC():
     
    today = pd.datetime.today()
    dt= (today - BDay(1)).strftime("%Y_%m_%d")
    
    Path="C:/ComplianceTesting/"
    fName="Trades_Report_ISIN_Duco.zip"
    #zf = zipfile.ZipFile(Path+fName, 'r')
    
    
    request=get('https://kgc0418-tdw-data-0.s3.amazonaws.com/slices/CUMULATIVE_EQUITIES_'+dt+'.zip')
    
    
    
    zip_file = zipfile.ZipFile(BytesIO(request.content),'r')
    
    #zip_file=zipfile.ZipFile(Path+fName, 'r')
    
    
    files = zip_file.namelist()
    rData=csv.reader(zip_file.open(files[0]),delimiter=",")
    
    print rData
    
    
    
    print files
    
    rD=zip_file.open(files[0])
    
    print rD
    print type(rD)
    rD2=csv.reader(zip_file.open(files[0]),delimiter=",")
    print rD2
    
    data=pd.read_csv(rD,engine='python',skipfooter=1)
    data=data.dropna(axis=1,how="all")
    fData=data[((data["PRICE_NOTATION_TYPE"].isin(CCYs)))]
    print fData
    
if __name__=="__main__":
    print "DTCC Report Generator"
    GetDTCC()
    