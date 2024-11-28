import yfinance as yf
import pandas as pd
import datetime
from datetime import timedelta
from git import Repo
import os
class Intraday:
    def __init__(self,tickers=[],interval="",start_intraday= -1,end_intraday= -1):
        self.dict_symbols = {
        "ZN=F":["ZN","10-Year T-Note Futures"],
        "ZB=F":["ZB","US-Treasury Bond Futures"],
        "ZT=F":["ZT","2-Year T-Note Futures"],
        "CL=F":["CL","Crude Oil futures"],
        "GC=F":["GC","Gold futures"],
        "NQ=F":["NQ","Nasdaq 100 futures"],
        "ES=F":["ES","E-mini S&P 500"],
        "DX-Y.NYB":["DXY","US Dollar Index"],
        "^DJI":["DJI","Dow Jones Industrial Average"],
        "^GSPC":["GSPC","S&P 500"]}
        if tickers!=[]:
            tempdic={}
            for ticker in tickers:
                if ticker in self.dict_symbols:
                    tempdic[ticker]=self.dict_symbols[ticker]
                else:
                    print(f'Data with respect to {ticker} not found in Yahoo Finance')
            self.dict_symbols=tempdic
        self.tickers=list(self.dict_symbols.keys())
        self.symbols=[i[0] for i in list(self.dict_symbols.values())]
        self.interval=interval
        self.start_intraday=start_intraday
        self.end_intraday=end_intraday
        print('Current Ticker Dictionary:',self.dict_symbols)
        

    def fetch_data_yfinance(self,specific_tickers=[]): #Extracts datetime in UTC
        # Get data from Yahoo Finance
        today = (datetime.datetime.now())
        if self.start_intraday!=-1 and self.end_intraday!=-1:
            end=(today-timedelta(days=self.end_intraday)).strftime("%Y-%m-%d")
            start = (today - timedelta(days=self.start_intraday)).strftime("%Y-%m-%d")
            data = yf.download(tickers=self.tickers, start=start, end=end, interval=self.interval) 
        elif self.start_intraday==self.end_intraday==-1:
            data = yf.download(tickers=self.tickers, interval=self.interval) 
        
        # Return data for specific tickers as a dictionary 
        try:        
            if specific_tickers!=[]:
                alltickerdata={}
                stackeddata=data.stack(level=0,future_stack=False)
                stackeddata.index.names=['Datetime','Price']
                for col in stackeddata.columns:
                    col_data=stackeddata[col].unstack()
                    alltickerdata[col]=col_data
                return alltickerdata
            else:
                return data
        except Exception as e:
            print(e)
            return data
    
    
    def clone_data_github(self,repo_url="",token="",username=""): 
        destination_folder='Git_repo_files'
        if repo_url==token==username=="":
            token = "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"  #GitHub PAT token (60days)
            username = "krishangguptafibonacciresearch"  # GitHub Username
            repo_url="https://github.com/krishangguptafibonacciresearch/distro_project.git" #Only Read Access Granted

        try:
            repo_url = repo_url.replace("https://", f"https://{username}:{token}@")
            os.makedirs(destination_folder, exist_ok=True)
            Repo.clone_from(repo_url, destination_folder)
            print("Repository cloned successfully!")
        except Exception as e:
            print(f"Error: {e}")
    

            
if __name__=='__main__':
    mydata=Intraday(tickers=["ZN=F","ZT=F"],interval='1d',start_intraday=729,end_intraday=1)
    dic=(mydata.fetch_data_yfinance(['ZN=F','ZT=F']))
    print(dic['ZN=F'])