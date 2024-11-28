import numpy as np
import pandas as pd


class Events:
    def __init__(self, excel):
        self.excel = excel
        self.sheets_dic = pd.read_excel(self.excel, sheet_name=None)
        self.all = self.merge_sheets(self.sheets_dic)

    def format_sheet(self, key, df):
        df.columns = df.columns.str.strip().str.lower()
        df = df.dropna(how="all")
        df["year"] = key  # track the sheet name
        df["is_date"] = df["time"].str.contains(key, na=False)
        df["datetime"] = df["time"].where(df["is_date"] == True)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["datetime"] = df["datetime"].ffill()
        df["date"] = df["datetime"]
        df["onlytime"] = df["time"].where(df["is_date"] == False)
        df["onlytime"] = df["onlytime"].astype(str).str.split().str[1]
        df["onlytime"] = df["onlytime"].fillna("")
        df["datetime"] = df.apply(
            lambda row: (
                str(row["date"]) + " " + str(row["onlytime"])
                if str(row["date"]) != "" and str(row["onlytime"]) != ""
                else str(row["date"])
            ),
            axis=1,
        )
        df["datetime"] = df["datetime"].apply(
            lambda dt: (
                " ".join([dt.split()[0], dt.split()[2]]) if len(dt.split()) > 2 else dt
            )
        )
        df["datetime"] = df["datetime"].where(df["is_date"] == False)
        df["datetime"] = pd.to_datetime(df["datetime"])  # .where(df['is_date']==False)
        finaldf = df[["datetime", "event", "year"]]
        finaldf.dropna(inplace=True)
        return finaldf

    def merge_sheets(self, sheets_dic):
        sheets_list = []
        for key in sheets_dic:
            try:
                float(key)
            except Exception as e:
                print(e)
                pass
            else:
                sheets_list.append(self.format_sheet(key, sheets_dic[key]))
        merged_sheet = pd.concat(sheets_list[::-1], ignore_index=True)
        return merged_sheet

    def save_sheet(self, sheet, name="combined.csv"):
        sheet.to_csv(name)

    def add_and_change_tz(
        self, originaldf, tz_col, current_tz="Asia/Kolkata", final_tz="US/Eastern"
    ):
        df = originaldf.copy()
        df[tz_col] = df[tz_col].dt.tz_localize(current_tz)
        df[tz_col] = df[tz_col].dt.tz_convert(final_tz)
        return df


if __name__ == "__main__":
    path = "events_data.xlsx"
    myevents = Events(path)
    normal_combined = myevents.all
    est_combined = myevents.change_tz(
        myevents.all, "datetime", current_tz="Asia/Kolkata", final_tz="US/Eastern"
    )
    myevents.save_sheet(normal_combined, "allevents.csv")
    myevents.save_sheet(
        myevents.change_tz(
            myevents.all, "datetime", current_tz="Asia/Kolkata", final_tz="Asia/Kolkata"
        ),
        "ist_events.csv",
    )
    myevents.save_sheet(est_combined, "est_events.csv")
