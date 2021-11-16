import numpy as np
import pandas as pd
from datetime import datetime
import datadotworld as dw

#__________preprocess-LOG_AnimalCenter-AustinTX_Intakes____________________________________________________________________________________________________
# 0
# data = pd.read_csv("https://query.data.world/s/nltr6xsnpwrhcfx4dacus3gc73kmbz")
query = dw.query('siyeh/austin-animal-center-live-data', 
                 'SELECT * FROM austin_animal_center_intakes')
data = query.dataframe
# 1
data.drop_duplicates(inplace=True)
# 2
data.drop("monthyear", axis=1, inplace=True)
# 3
data["name"].fillna("Unknown", inplace=True)
data["name"] = data["name"].str.replace("*", "", regex=False)
data.loc[data["name"]=="", "name"] = "Unknown"
# 4
data["sex_upon_intake"].fillna("Unknown", inplace=True)
# 5
data.loc[data["name"]==data["animal_id"], "name"] = "Unknown"
# 6
data["in_time2"] = np.where(data.datetime.dt.hour < 12, "AM", "PM")
data["datetime"] = data["datetime"].astype("str")
data[["in_date", "in_time1"]] = data["datetime"].str.split(pat=" ", n=1, expand=True)
# 7
data["in_date"] = pd.to_datetime(data["in_date"], format="%Y-%m-%d")
data.drop("datetime", axis=1, inplace=True)
# 8
data.rename(columns={"intake_type":"in_type",
                     "intake_condition":"in_condition",
                     "sex_upon_intake":"in_sex",
                     "age_upon_intake":"in_age"}, inplace=True)
# 9
data[["aui1", "aui2"]] = data["in_age"].str.split(n=1, expand=True)
data["aui1"] = data["aui1"].astype("int")
data.loc[data["aui1"]<0, "aui1"] = 0
# 10
conditions = [(data["aui2"]=="day") | (data["aui2"]=="days"),
              (data["aui2"]=="week") | (data["aui2"]=="weeks"),
              (data["aui2"]=="month") | (data["aui2"]=="months"),
              (data["aui2"]=="year") | (data["aui2"]=="years")]
values = [1/30, 7/30, 1, 12]
data["aui3"] = np.select(conditions, values)
# 11
data["in_age2"] = data["aui1"] * data["aui3"]
data.drop(["aui1", "aui2", "aui3"], axis=1, inplace=True)
# 12
data.sort_values(by="in_date", inplace=True)
# 13
data.to_csv("preprocessed_AnimalCenter-AustinTX_Intakes.csv", index=False)

print("FINISHED preprocessing AnimalCenter-AustinTX_Intakes")




#__________preprocess-LOG_AnimalCenter-AustinTX_Outcomes____________________________________________________________________________________________________
# 0
# data = pd.read_csv("https://query.data.world/s/xiz7cr6gvmuegh4bcl4r2trdrz5hnp")
query = dw.query('siyeh/austin-animal-center-live-data', 
                 'SELECT * FROM austin_animal_center_outcomes')
data = query.dataframe
# 1
data.drop_duplicates(inplace=True)
# 2
data.drop("monthyear", axis=1, inplace=True)
# 3
data["name"].fillna("Unknown", inplace=True)
data["name"] = data["name"].str.replace("*", "", regex=False)
data.loc[data["name"]=="", "name"] = "Unknown"
# 4
data["outcome_type"].fillna("Unknown", inplace=True)
# 5
data["outcome_subtype"].fillna("No Subtype", inplace=True)
# 6
data["sex_upon_outcome"].fillna("Unknown", inplace=True)
# 7
data["date_of_birth"] = pd.to_datetime(data["date_of_birth"], format="%Y-%m-%d")
# 8
data.loc[data["animal_id"]==data["name"], "name"] = "Unknown"
# 9
data["out_time2"] = np.where(data.datetime.dt.hour < 12, "AM", "PM")
data["datetime"] = data["datetime"].astype("str")
data[["out_date", "out_time1"]] = data["datetime"].str.split(pat=" ", n=1, expand=True)
# 10
data["out_date"] = pd.to_datetime(data["out_date"], format="%Y-%m-%d")
data.drop("datetime", axis=1, inplace=True)
# 11
data.rename(columns={"outcome_type":"out_type",
                     "outcome_subtype":"out_subtype",
                     "sex_upon_outcome":"out_sex",
                     "age_upon_outcome":"out_age"}, inplace=True)
# 12
col1 = data[data["out_age"].isnull()]["out_date"].copy()
col2 = data[data["out_age"].isnull()]["date_of_birth"].copy()
data.loc[data["out_age"].isnull(), "out_age"] = (col1 - col2).astype("str")

col1 = data[data["out_age"]=="NULL"]["out_date"].copy()
col2 = data[data["out_age"]=="NULL"]["date_of_birth"].copy()
data.loc[data["out_age"]=="NULL", "out_age"] = (col1 - col2).astype("str")
# 13
data[["auo1", "auo2"]] = data["out_age"].str.split(n=1, expand=True)
data["auo1"] = data["auo1"].astype("int")
data.loc[data["auo1"]<0, "auo1"] = 0
# 14
conditions = [(data["auo2"]=="day") | (data["auo2"]=="days"),
              (data["auo2"]=="week") | (data["auo2"]=="weeks"),
              (data["auo2"]=="month") | (data["auo2"]=="months"),
              (data["auo2"]=="year") | (data["auo2"]=="years"),]
values = [1/30, 1/(30/7), 1, 12]
data["auo3"] = np.select(conditions, values)
# 15
data["out_age2"] = data["auo1"] * data["auo3"]
data.drop(["auo1", "auo2", "auo3"], axis=1, inplace=True)
# 16
data.sort_values(by="out_date", inplace=True)
# 17
data.to_csv("preprocessed_AnimalCenter-AustinTX_Outcomes.csv", index=False)

print("FINISHED preprocessing AnimalCenter-AustinTX_Outcomes")




#__________preprocess-LOG_Covid-19-Cases-&-Vaccination____________________________________________________________________________________________________
# 0
case_confirmed = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv")
case_deaths = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv")
vacc_vaccinated = pd.read_csv("https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/vaccine_data/us_data/time_series/people_vaccinated_us_timeline.csv")
# 1
data1 = case_confirmed[case_confirmed["FIPS"]==48015].copy()
data2 = case_deaths[case_deaths["FIPS"]==48015].copy()
data3 = vacc_vaccinated[(vacc_vaccinated["Province_State"]=="Texas")].copy()
# 2
data1.drop(["UID","iso2","iso3","code3","FIPS","Province_State","Country_Region","Lat","Long_","Admin2"], axis=1, inplace=True)
data2.drop(["UID","iso2","iso3","code3","FIPS","Province_State","Country_Region","Lat","Long_","Admin2","Population"], axis=1, inplace=True)
# 3
data1.set_index("Combined_Key", inplace=True)
data2.set_index("Combined_Key", inplace=True)
data1 = data1.transpose()
data2 = data2.transpose()
data1.reset_index(inplace=True)
data2.reset_index(inplace=True)
# 4
data1.rename(columns={"index":"date", "Austin, Texas, US":"confirmed"}, inplace=True)
data2.rename(columns={"index":"date", "Austin, Texas, US":"deaths"}, inplace=True)
# 5
data1["date"] = pd.to_datetime(data1["date"], format="%m/%d/%y")
data2["date"] = pd.to_datetime(data2["date"], format="%m/%d/%y")
# 6
data4 = data1.merge(data2, on="date")
# 7
data3.drop(["FIPS","Province_State","Country_Region","Lat","Long_","Combined_Key"] ,axis=1 , inplace=True)
# 8
data3["Date"] = pd.to_datetime(data3["Date"], format="%Y/%m/%d")
# 9
data3["People_Fully_Vaccinated"].fillna(0, inplace=True)
data3["People_Partially_Vaccinated"].fillna(0, inplace=True)
# 10
data3.rename(columns={"Date":"date", "People_Fully_Vaccinated":"fully_vaccinated", "People_Partially_Vaccinated":"partially_vaccinated"}, inplace=True)
# 11
data = data4.merge(data3, how="left", on="date")
# 12
data["fully_vaccinated"].fillna(0, inplace=True)
data["partially_vaccinated"].fillna(0, inplace=True)
# 13
data.to_csv("preprocessed_Covid19-Texas.csv", index=True)

print("FINISHED preprocessing Covid19-Texas")




#__________creating-LOG_AnimalCenter-AustinTX_Animal____________________________________________________________________________________________________
# 0
intake = pd.read_csv("preprocessed_AnimalCenter-AustinTX_Intakes.csv")
outcome = pd.read_csv("preprocessed_AnimalCenter-AustinTX_Outcomes.csv")
# 1
data1 = intake[["animal_id", "name", "animal_type", "breed", "color"]].copy()
data2 = outcome[["animal_id", "name", "animal_type", "breed", "color"]].copy()
# 2
data1.drop_duplicates(inplace=True)
data2.drop_duplicates(inplace=True)
# 3
data = data1.append(data2, ignore_index=True, sort=False)
# 4
data.drop_duplicates(inplace=True)
# 5
data.to_csv("created_AnimalCenter-AustinTX_Animal.csv", index=False)

print("FINISHED creating AnimalCenter-AustinTX_Animal")

#__________creating-LOG_AnimalCenter-AustinTX_Calendar____________________________________________________________________________________________________
# 0
fromm = "10/01/2013"
to = datetime.now().date()
# 1
date = pd.Series(pd.date_range(fromm, to))
# 2
year = date.dt.year
month = date.dt.month
week = date.dt.isocalendar().week
day = date.dt.day
# 3
day_ddd = date.dt.day_name()
day_ddd = day_ddd.str.replace("Monday", "Mon", regex=False)
day_ddd = day_ddd.str.replace("Tuesday", "Tue", regex=False)
day_ddd = day_ddd.str.replace("Wednesday", "Wed", regex=False)
day_ddd = day_ddd.str.replace("Thursday", "Thu", regex=False)
day_ddd = day_ddd.str.replace("Friday", "Fri", regex=False)
day_ddd = day_ddd.str.replace("Saturday", "Sat", regex=False)
day_ddd = day_ddd.str.replace("Sunday", "Sun", regex=False)
# 4
day_of_week = day_ddd.copy()
day_of_week = day_of_week.str.replace("Mon", '2', regex=False)
day_of_week = day_of_week.str.replace("Tue", '3', regex=False)
day_of_week = day_of_week.str.replace("Wed", '4', regex=False)
day_of_week = day_of_week.str.replace("Thu", '5', regex=False)
day_of_week = day_of_week.str.replace("Fri", '6', regex=False)
day_of_week = day_of_week.str.replace("Sat", '7', regex=False)
day_of_week = day_of_week.str.replace("Sun", '0', regex=False)
# 5
month_mmm = date.dt.month_name()
month_mmm = month_mmm.str.replace("January", "Jan", regex=False)
month_mmm = month_mmm.str.replace("February", "Feb", regex=False)
month_mmm = month_mmm.str.replace("March", "Mar", regex=False)
month_mmm = month_mmm.str.replace("April", "Apr", regex=False)

month_mmm = month_mmm.str.replace("June", "Jun", regex=False)
month_mmm = month_mmm.str.replace("July", "Jul", regex=False)
month_mmm = month_mmm.str.replace("August", "Aug", regex=False)
month_mmm = month_mmm.str.replace("September", "Sep", regex=False)
month_mmm = month_mmm.str.replace("October", "Oct", regex=False)
month_mmm = month_mmm.str.replace("November", "Nov", regex=False)
month_mmm = month_mmm.str.replace("December", "Dec", regex=False)
# 6
dict = {
    #"date_id":date_id,
    "date":date,
    "day_of_month":day,
    "day_of_week":day_of_week,
    "ddd_of_week":day_ddd,
    "week_of_year":week,
    "month":month,
    "mmm":month_mmm,
    "year":year
}
# 7
data = pd.DataFrame(data=dict)
# 8
data.to_csv("created_AnimalCenter-AustinTX_Calendar.csv", index=False)

print("FINISHED creating AnimalCenter-AustinTX_Calendar")

#__________creating-LOG_SampleForAnalysis____________________________________________________________________________________________________
# 0
intakes = pd.read_csv("preprocessed_AnimalCenter-AustinTX_Intakes.csv")
outcomes = pd.read_csv("preprocessed_AnimalCenter-AustinTX_Outcomes.csv")
covid19 = pd.read_csv("preprocessed_Covid19-Texas.csv")
# 1
intakes["in_date"] = pd.to_datetime(intakes["in_date"], format="%Y-%m-%d")
outcomes["out_date"] = pd.to_datetime(outcomes["out_date"], format="%Y-%m-%d")
covid19["date"] = pd.to_datetime(covid19["date"], format="%Y-%m-%d")
# 2
intakes = intakes.loc[intakes["in_date"] > datetime(2020, 3, 25), ["in_date", "animal_id"]].copy()
outcomes = outcomes.loc[outcomes["out_date"] > datetime(2020, 3, 25), ["out_date", "animal_id"]].copy()
covid19 = covid19.loc[covid19["confirmed"]>0, ["date", "confirmed", "deaths"]].copy()
# 3
intakes.rename(columns={"in_date":"date", "animal_id":"intakes"}, inplace=True)
outcomes.rename(columns={"out_date":"date", "animal_id":"outcomes"}, inplace=True)
# 4
intakes = intakes.groupby(by="date").count()
outcomes = outcomes.groupby(by="date").count()
data1 = intakes.merge(outcomes, how="left", on="date")
# 5
data = data1.merge(covid19, how="left", on="date")
# 6
data["intakes"].fillna(0, inplace=True)
data["outcomes"].fillna(0, inplace=True)
data["confirmed"].fillna(0, inplace=True)
data["deaths"].fillna(0, inplace=True)

data["intakes"] = data["intakes"].astype("int")
data["outcomes"] = data["outcomes"].astype("int")
data["confirmed"] = data["confirmed"].astype("int")
data["deaths"] = data["deaths"].astype("int")
# 7
data.to_csv("created_SampleForAnalysis.csv", index=False)

print("FINISHED creating SampleForAnalysis")

#__________UPLOAD TO DATA.WORLD____________________________________________________________________________________________________
# client = dw.api_client()

# dataset_key = "dtbuu3700/majorproject"
# client.upload_file(dataset_key=dataset_key, name="preprocessed_AnimalCenter-AustinTX_Intakes.csv")
# client.upload_file(dataset_key=dataset_key, name="preprocessed_AnimalCenter-AustinTX_Outcomes.csv")
# client.upload_file(dataset_key=dataset_key, name="preprocessed_Covid19-Texas.csv")
# client.upload_file(dataset_key=dataset_key, name="created_AnimalCenter-AustinTX_Animal.csv")
# client.upload_file(dataset_key=dataset_key, name="created_AnimalCenter-AustinTX_Calendar.csv")
# client.upload_file(dataset_key=dataset_key, name="created_SampleForAnalysis.csv")

# print("UPLOADED to data.world")
#__________a____________________________________________________________________________________________________
#__________a____________________________________________________________________________________________________
#__________a____________________________________________________________________________________________________
#__________a____________________________________________________________________________________________________