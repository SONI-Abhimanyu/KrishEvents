import os
import pandas as pd
import datetime
from datetime import timedelta
from intradaydata import Intraday
import matplotlib.pyplot as plt
import seaborn as sns
from events import Events
class Returns:
    def __init__(self,output_folder="plots_folder",dataframe=pd.DataFrame()):
        self.colors = {
        "deep_black": "#000000",
        "golden_yellow": "#FFD700",
        "dark_slate_gray": "#2F4F4F",
        "ivory": "#FFFFF0",
        "fibonacci_blue": "#0066CC",
        "sage_green": "#8FBC8F",
        "light_gray": "#D3D3D3",
        }
        self.sessions=[
        "London 0-7 ET",
        "US Open 7-10 ET",
        "US Mid 10-15 ET",
        "US Close 15-17 ET",
        "Asia 18-24 ET",
        "All day"]
        self.output_folder = output_folder
        self.dataframe=dataframe
        os.makedirs(self.output_folder, exist_ok=True)

    def get_session(self,timestamp):
            hour = timestamp.hour
            minute = timestamp.minute
            if 18 <= hour < 24:
                return "Asia 18-24 ET"
            elif 0 <= hour < 7 :#or (hour == 6 and minute < 30):
                return "London 0-7 ET"
            elif 7 <= hour < 10 :#or (hour == 6 and minute >= 30):
                return "US Open 7-10 ET"
            elif 10 <= hour < 15:
                return "US Mid 10-15 ET"
            elif 15 <= hour < 17:
                return "US Close 15-17 ET"
            else:
                return "Other"
        
    def filter_data(self,start_date="",end_date="",month_day_filter=[],to_eastern=False,to_sessions=True):
        df=self.dataframe.copy()
        df['timestamp']=df.index
        df['timestamp']=pd.to_datetime(df['timestamp'])
        if to_eastern==True:
            df["timestamp"] = df["timestamp"].dt.tz_convert("US/Eastern")
            df.index= df['timestamp']
        if to_sessions==True:
            df['session'] = df['timestamp'].apply(self.get_session)
        
        if month_day_filter==[]:
            if start_date==end_date=="":
                filtered_df=df
            elif start_date=="" and end_date!="":
                end_date=pd.to_datetime(end_date).date()
                filtered_df = df[(df['timestamp'].dt.date <= end_date)]
            elif start_date!="" and end_date=="":
                start_date=pd.to_datetime(start_date).date()
                filtered_df=  df[(df['timestamp'].dt.date >= start_date)]
            else:
                start_date=pd.to_datetime(start_date).date()
                end_date=pd.to_datetime(end_date).date()
                filtered_df = df[(df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)]

        else:
            month=month_day_filter[0]
            day1=month_day_filter[1]
            day2=month_day_filter[2]
            filtered_df = df[(df['timestamp'].dt.month == month) & (df['timestamp'].dt.day >= day1) & (df['timestamp'].dt.day <= day2)]

        return(filtered_df)
    
    def calculate_return_bps(self,group):
        return abs(group["Close"].iloc[-1] - group["Close"].iloc[0]) * 16

    def get_daily_session_returns(self,df):
        daily_session_returns = (
            df.groupby([df["timestamp"].dt.date, "session"], group_keys=False)
            .apply(self.calculate_return_bps, include_groups=False)
            .reset_index()
        )
        daily_session_returns.columns=["date", "session", "return"]
        return daily_session_returns
    
    def get_daily_returns(self,df):
        daily_returns_all = (
                    df.groupby(filtered_df["timestamp"].dt.date)
                    .apply(self.calculate_return_bps)
                    .reset_index()
                )
        daily_returns_all.columns = ["date", "return"]
        return daily_returns_all

    
    def plot_daily_session_returns(self,filtered_df,tickersymbol,interval):
        # start_date=list((daily_session_returns['date']).unique())[0]
        # end_date=list((daily_session_returns['date']).unique())[-1]
        start_date=(filtered_df['timestamp'].dt.date.tolist())[0]
        end_date=(filtered_df['timestamp'].dt.date.tolist())[-1]
        print(start_date,end_date)
        sessions = self.sessions

        plt.figure(figsize=(24, 18))
        sns.set_style("darkgrid")
        list_stats = []
        for i, session in enumerate(sessions, 1):
            plt.subplot(3, 2, i)
            if session != "All day":
                daily_session_returns=(self.get_daily_session_returns(filtered_df))
                session_returns = daily_session_returns[daily_session_returns["session"] == session]["return"]
            else:
                # daily_returns_all = (
                #     filtered_df.groupby(filtered_df["timestamp"].dt.date)
                #     .apply(self.calculate_session_return_bps)
                #     .reset_index()
                # )
                # daily_returns_all.columns = ["date", "return"]
                daily_returns_all=self.get_daily_returns(filtered_df)
                session_returns = daily_returns_all["return"]

            sns.histplot(
                session_returns, kde=True, stat="density", linewidth=0, color="skyblue"
            )
            sns.kdeplot(session_returns, color="darkblue", linewidth=2)
            plt.title(f"{session}", fontsize=18)
            plt.xlabel("Session return in TV bps", fontsize=16)
            plt.ylabel("Density", fontsize=16)

            mean = session_returns.mean()
            median = session_returns.median()
            perc95 = session_returns.quantile(0.95)
            perc99 = session_returns.quantile(0.99)
            std = session_returns.std()
            skew = session_returns.skew()
            kurt = session_returns.kurtosis()

            stats_text = f"Mean: {mean:.2f}\nMedian: {median:.2f}\nStd: {std:.1f}\n95%ile: {perc95:.1f}\n99%ile: {perc99:.1f}\nSkew: {skew:.1f}\nKurt: {kurt:.1f}"
            plt.text(
                0.95,
                0.95,
                stats_text,
                transform=plt.gca().transAxes,
                verticalalignment="top",
                horizontalalignment="right",
                bbox=dict(
                    boxstyle="round",
                    facecolor=self.colors["ivory"],
                    edgecolor=self.colors["dark_slate_gray"],
                    alpha=0.8,
                ),
                color=self.colors["dark_slate_gray"],
                fontsize=20,
            )

            list_stats.append(
                session_returns.describe(
                    percentiles=[0.05, 0.25, 0.5, 0.68, 0.90, 0.95, 0.99, 0.997]
                )
            )
        plt.tight_layout()
        plt.suptitle(
            f"Distribution of Returns {tickersymbol} with interval of {interval}: ABS(End - Start) across trading sessions: {start_date} to {end_date}",
            fontsize=20,
            y=1.02,
        )
        plt.savefig(
            os.path.join(self.output_folder, f"{tickersymbol}_{interval}_Returns_Distribution_{start_date}_{end_date}.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

        df_stats = pd.concat(list_stats, axis=1)
        df_stats.columns = sessions
        df_stats.to_csv(os.path.join(self.output_folder, f"{tickersymbol}_{interval}_Returns_{start_date}_{end_date}_stats.csv"))
        print(df_stats.round(1))


    # def calculate_session_volatility_return_bps(self,group):
    #     return abs(group["Close"].iloc[-1] - group["Close"].iloc[0]) * 16

    def get_daily_session_volatility_returns(self,df):
        # daily_returns = (
        #     df.groupby([df["timestamp"].dt.date, "session"], group_keys=False)
        #     .apply(self.calculate_session_return_bps, include_groups=False)
        #     .reset_index()
        # )
        # daily_returns.columns=["date", "session", "return"]
        # return daily_returns
        session_volatility_df = df.groupby([df["timestamp"].dt.date, "session"]).agg({
                                                            "High": ["max"],
                                                            "Low": ["min"]})
        session_volatility_df["return"] = 16 * (session_volatility_df["High"]["max"] - session_volatility_df["Low"]["min"])
        session_volatility_df = session_volatility_df.reset_index()
        session_volatility_df.columns = ["date", "session", "high", "low", "return"]
        session_volatility_df = session_volatility_df.sort_values(["date", "session"])
        return session_volatility_df

    def get_daily_volatility_returns(self,df):
        all_df = df.groupby([df["timestamp"].dt.date]).agg({ "High": ["max"],
                                                   "Low": ["min"]})
        all_df["return"] = 16 * (all_df["High"]["max"] - all_df["Low"]["min"])
        all_df = all_df.reset_index()
        all_df.columns = ["date", "high", "low", "return"]
        all_df = all_df.sort_values(["date"])
        return all_df
    
    def plot_daily_session_volatility_returns(self,filtered_df,tickersymbol,interval):
        start_date=(filtered_df['timestamp'].dt.date.tolist())[0]
        end_date=(filtered_df['timestamp'].dt.date.tolist())[-1]
        sessions=self.sessions
        print(start_date,end_date)
        # Analyze distributions
        list_stats = []
        plt.figure(figsize=(24, 18))
        sns.set_style("darkgrid")

        for i, session in enumerate(sessions, 1):
            plt.subplot(3, 2, i)
            if session == "All day":
                all_volatility_df=self.get_daily_volatility_returns(filtered_df)
                session_returns = all_volatility_df["return"]
            else:
                session_volatility_df=self.get_daily_session_volatility_returns(filtered_df)
                session_returns = session_volatility_df.loc[session_volatility_df["session"] == session, ["return"]]

            sns.histplot(
                session_returns, kde=True, stat="density", linewidth=0, color="skyblue"
            )
            sns.kdeplot(session_returns, color="darkblue", linewidth=2)
            plt.title(f"{session}", fontsize=18)
            plt.xlabel("Session return in TV bps", fontsize=16)
            plt.ylabel("Density", fontsize=16)
            plt.legend("", frameon=False)

            mean = session_returns.mean()
            median = session_returns.median()
            std = session_returns.std()
            perc95 = session_returns.quantile(0.95)
            perc99 = session_returns.quantile(0.99)
            skew = session_returns.skew()
            kurt = session_returns.kurtosis()

            if isinstance(session_returns, pd.DataFrame):
                mean, median, std, perc95, perc99, skew, kurt = [
                    x.iloc[0] for x in [mean, median, std, perc95, perc99, skew, kurt]
                ]

            list_stats.append(
                session_returns.describe(
                    percentiles=[0.05, 0.25, 0.5, 0.68, 0.90, 0.95, 0.99, 0.997]
                )
            )

            stats_text = f"Mean: {mean:.2f}\nMedian: {median:.2f}\nStd: {std:.1f}\n95%ile: {perc95:.1f}\n99%ile: {perc99:.1f}\nSkew: {skew:.1f}\nKurt: {kurt:.1f}"
            plt.text(
                0.95,
                0.95,
                stats_text,
                transform=plt.gca().transAxes,
                verticalalignment="top",
                horizontalalignment="right",
                bbox=dict(
                    boxstyle="round",
                    facecolor=self.colors["ivory"],
                    edgecolor=self.colors["dark_slate_gray"],
                    alpha=0.8,
                ),
                color=self.colors["dark_slate_gray"],
                fontsize=20,
            )

        plt.tight_layout()
        plt.suptitle(
            f"Distribution of Volatility {tickersymbol} with interval of {interval}: (High - Low) across trading sessions: {start_date} to {end_date}",
            fontsize=20,
            y=1.02,
        )
        plt.savefig(
            os.path.join(self.output_folder, f"{tickersymbol}_{interval}_Volatility_Distribution_{start_date}_{end_date}_High_Low_.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

        df_stats = pd.concat(list_stats, axis=1)
        df_stats.columns = sessions
        df_stats.to_csv(os.path.join(self.output_folder, f"{tickersymbol}_{interval}_Volatility_Returns_{start_date}_{end_date}_High_Low_stats.csv"))
        print(df_stats.round(1))

    def plot_daily_volatility_returns(self,filtered_df,tickersymbol,interval):
        session=["All day"]
        start_date=(filtered_df['timestamp'].dt.date.tolist())[0]
        end_date=(filtered_df['timestamp'].dt.date.tolist())[-1]
        print(start_date,end_date)
        # Analyze distributions
        list_stats = []
        plt.figure(figsize=(24, 18))
        sns.set_style("darkgrid")
 
        daily_volatility_returns=(self.get_daily_volatility_returns(filtered_df))
        session_returns = daily_volatility_returns["return"]
        
        sns.histplot(
            session_returns, kde=True, stat="density", linewidth=0, color="skyblue"
        )
        sns.kdeplot(session_returns, color="darkblue", linewidth=2)
        plt.title(f"{session[0]}", fontsize=18)
        plt.xlabel("Session return in TV bps", fontsize=16)
        plt.ylabel("Density", fontsize=16)
        plt.legend("", frameon=False)

        mean = session_returns.mean()
        median = session_returns.median()
        std = session_returns.std()
        perc95 = session_returns.quantile(0.95)
        perc99 = session_returns.quantile(0.99)
        skew = session_returns.skew()
        kurt = session_returns.kurtosis()

        if isinstance(session_returns, pd.DataFrame):
            mean, median, std, perc95, perc99, skew, kurt = [
                x.iloc[0] for x in [mean, median, std, perc95, perc99, skew, kurt]
            ]

        list_stats.append(
            session_returns.describe(
                percentiles=[0.05, 0.25, 0.5, 0.68, 0.90, 0.95, 0.99, 0.997]
            )
        )

        stats_text = f"Mean: {mean:.2f}\nMedian: {median:.2f}\nStd: {std:.1f}\n95%ile: {perc95:.1f}\n99%ile: {perc99:.1f}\nSkew: {skew:.1f}\nKurt: {kurt:.1f}"
        plt.text(
            0.95,
            0.95,
            stats_text,
            transform=plt.gca().transAxes,
            verticalalignment="top",
            horizontalalignment="right",
            bbox=dict(
                boxstyle="round",
                facecolor=self.colors["ivory"],
                edgecolor=self.colors["dark_slate_gray"],
                alpha=0.8,
            ),
            color=self.colors["dark_slate_gray"],
            fontsize=20,
        )

        plt.tight_layout()
        plt.suptitle(
            f"Distribution of Volatility {tickersymbol} with interval of {interval}: (High - Low) across all day: {start_date} to {end_date}",
            fontsize=20,
            y=1.02,
        )
        plt.savefig(
            os.path.join(self.output_folder, f"{tickersymbol}_{interval}_Volatility_Distribution_{start_date}_{end_date}_High_Low_.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

        df_stats = pd.concat(list_stats, axis=1)
        df_stats.columns = session
        df_stats.to_csv(os.path.join(self.output_folder, f"{tickersymbol}_{interval}_Volatility_Returns_{start_date}_{end_date}_High_Low_stats.csv"))
        print(df_stats.round(1))


    def tag_events(self,ev,pc):
        events_df=ev.copy()
        price_df=pc.copy()
        price_df.index.name=None
        price_df.reset_index(inplace=True)
        events_df['timestamp']=events_df['datetime']
        events_df = events_df.groupby('timestamp').agg({
            'event': lambda x: ', '.join(map(str, x)),  # Combine using a comma-separated string
        }).reset_index()

        price_df = price_df.sort_values('timestamp')
        events_df = events_df.sort_values('timestamp')   

        exact_event_df = pd.merge(price_df, events_df, on='timestamp', how='left', suffixes=('', '_exact'))

        # Before Event (backward merge)
        before_event_df = pd.merge_asof(price_df, events_df, on='timestamp', direction='backward')

        # After Event (forward merge) with the condition that the event timestamp should be strictly greater than price timestamp
        after_event_df = pd.merge_asof(price_df, events_df, on='timestamp', direction='forward')

        # Step 3: Merge these results into the final DataFrame
        final_df = price_df.copy()

        # Adding the columns to the final DataFrame
        final_df['event_before'] = before_event_df['event']
        final_df['exact_event'] = exact_event_df['event']
        final_df['event_after'] = after_event_df['event']

        final_df.dropna(how='all',inplace=True)
        final_df.index=final_df['index']
        final_df.index.name=pc.index.name
        final_df.drop('index',axis=1,inplace=True)
        print(final_df)
        return final_df

if __name__=='__main__':
    myevents=Events('events_data.xlsx')
    est_events=myevents.add_and_change_tz(myevents.all,'datetime',current_tz='Asia/Kolkata',final_tz="US/Eastern")

    # 1h interval data
    hourly_data=Intraday(tickers=["ZN=F","ZT=F"],interval='1h',start_intraday=729,end_intraday=1)
    hdata=(hourly_data.fetch_data_yfinance(['ZN=F']))
    tickersymbol=hourly_data.dict_symbols['ZN=F'][0]
    interval=hourly_data.interval
    hdata=list(hdata.values())[0]
    hdata=hdata.dropna(axis=0)

    myplots=Returns(dataframe=hdata)
    outputfolder=myplots.output_folder
    filtered_df=(myplots.filter_data(month_day_filter=[12,15,31],to_eastern=True,to_sessions=True))
    tagged_df=myplots.tag_events(est_events,filtered_df)
    myevents.save_sheet(tagged_df,os.path.join(outputfolder,f'{tickersymbol}_{interval}_events_tagged.csv'))
    
    daily_session_returns=(myplots.get_daily_session_returns(tagged_df))
    print(daily_session_returns)
    myplots.plot_daily_session_returns(tagged_df,tickersymbol,interval)
    myplots.plot_daily_session_volatility_returns(tagged_df,tickersymbol,interval)
    
    # 1d interval data
    daily_data=Intraday(tickers=["ZN=F","ZT=F"],interval='1d')
    ddata=(daily_data.fetch_data_yfinance(['ZN=F']))
    tickersymbol=daily_data.dict_symbols['ZN=F'][0]
    interval=daily_data.interval
    ddata=list(ddata.values())[0]
    ddata=ddata.dropna(axis=0)

    myplots=Returns(dataframe=ddata)
    outputfolder=myplots.output_folder
    filtered_df=(myplots.filter_data(month_day_filter=[12,15,31],to_eastern=False,to_sessions=False))
    est_filtered_df=myevents.add_and_change_tz(filtered_df,'timestamp',current_tz='Asia/Kolkata',final_tz="US/Eastern")
    tagged_df=myplots.tag_events(est_events,est_filtered_df)
    myevents.save_sheet(tagged_df,os.path.join(outputfolder,f'{tickersymbol}_{interval}_events_tagged.csv'))
    
    daily_volatility_returns=(myplots.get_daily_volatility_returns(tagged_df))
    print(daily_volatility_returns)
    myplots.plot_daily_volatility_returns(tagged_df,tickersymbol,interval)



