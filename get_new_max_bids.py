import numpy as np
import pandas as pd
import warnings
import pycountry
warnings.filterwarnings(action="ignore")


def get_country_from_ISO(iso_code):
    try:
        return pycountry.countries.get(alpha_2=iso_code).name
    except:
        return ""
	
output_df = pd.read_csv("120719 - 92 Keys/120719 - 92 Keys - Google Ads - 1 Day.csv", skiprows=2)[['Campaign', 'Ad group', "Ad group max. CPV"]]
output_df["Country"] = output_df['Campaign'].str.split("|", expand=True)[2].str.strip()
output_df['Ad group max. CPV'] *= 0.055

for day in [1, 7, 14, 30]:
    google = pd.read_csv("120719 - 92 Keys/120719 - 92 Keys - Google Ads - {} Day.csv".format(day), 
                           skiprows=2)[['Campaign', 'Ad group', "YouTube Earned Views", "Views"]]
    output_df = output_df.merge(google, on=["Campaign", "Ad group"]).rename(columns={"YouTube Earned Views": 
                                                                                       "YouTube Earned Views {} days".format(day), 
                                                                                     'Views': "Ad Views {} days".format(day)})
	
for day in [1, 7, 14, 30]:
    youtube = pd.read_csv("120719 - 92 Keys/120719 - 92 Keys - YouTube - {} Day.csv".format(day))
    google = pd.read_csv("120719 - 92 Keys/120719 - 92 Keys - Google Ads - {} Day.csv".format(day), skiprows=2)
    google["Country"] = google['Campaign'].str.split("|", expand=True)[2].str.strip()
    google['Cost'] *= 0.055
    google['Ad group max. CPV'] *= 0.055
    google['Avg. CPV'] *= 0.055
    google_ads_country_groups = google.groupby("Country").sum().reset_index()
    youtube['Country'] = youtube['Geography'].apply(get_country_from_ISO)
    combined_df_country_groups = google_ads_country_groups.merge(youtube, on="Country", how="left", suffixes=("_Google", "_Youtube"))
    combined_df_country_groups["Country ROAS {} days".format(day)] = combined_df_country_groups['Your estimated revenue (USD)']/combined_df_country_groups['Cost']
    df = combined_df_country_groups[['Country', 'Cost', 'Views_Google', 'YouTube Earned Views', 'Your estimated revenue (USD)', "Country ROAS {} days".format(day)]]
    df["Earned View Value {} days".format(day)] = df['Your estimated revenue (USD)']/df['YouTube Earned Views']
    df["Earned View Cost {} days".format(day)] = df['Cost']/df['YouTube Earned Views']
    output_df = output_df.merge(df[["Country", "Earned View Value {} days".format(day), "Earned View Cost {} days".format(day), 
                                    "Country ROAS {} days".format(day)]], on="Country", how="left")
	
def calculate_max_bid(row):
    standard_weights = {1: .4, 7: .3, 14: .2, 30: .1}
    adjusted_weights = standard_weights.copy()
    minimum_number_views = 10
    type_of_view = "Ad Views"
    for day in [1, 7, 14, 30]:
        if row["{} {} days".format(type_of_view, day)] < minimum_number_views:
            adjusted_weights[day] = 0
    total_weight = np.sum(list(adjusted_weights.values()))
    if total_weight == 0:
        return row['Ad group max. CPV']
    country_ROAS = 0
    earned_view_value_over_cost = 0
    for day in [1, 7, 14, 30]:
        adjusted_weights[day] /= total_weight
        country_ROAS += row["Country ROAS {} days".format(day)] * standard_weights[day]
        earned_view_value_over_cost += ((row["Earned View Value {} days".format(day)]) / (row["Earned View Cost {} days".format(day)])) * adjusted_weights[day]
        
    if (earned_view_value_over_cost >= 1) or (country_ROAS >= 2):
        return row['Ad group max. CPV'] * 1.1
    elif earned_view_value_over_cost <= .5:
        return row['Ad group max. CPV'] * 0.9
    else:
        return row['Ad group max. CPV']
	
output_df["Max Bid"] = output_df.apply(calculate_max_bid, axis=1)

new_max_bid_df = output_df[output_df['Max Bid'] != output_df['Ad group max. CPV']][['Campaign', 'Ad group', 'Max Bid']].round(5)

new_max_bid_df.to_csv("new_max_bid_ads.csv", index=False)