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
	
output_df = pd.read_csv("120719 - 92 Keys/120719 - 92 Keys - Google Ads - {} Day.csv".format(30), skiprows=2)[['Campaign', 'Ad group', "Ad group max. CPV"]]
output_df["Country"] = output_df['Campaign'].str.split("|", expand=True)[2].str.strip()

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
    google_ads_country_groups = google.groupby("Country").sum().reset_index()
    youtube['Country'] = youtube['Geography'].apply(get_country_from_ISO)
    combined_df_country_groups = google_ads_country_groups.merge(youtube, on="Country", how="left", suffixes=("_Google", "_Youtube"))
    combined_df_country_groups["ROAS_{}".format(day)] = combined_df_country_groups['Your estimated revenue (USD)']/combined_df_country_groups['Cost']
    df = combined_df_country_groups[['Country', 'Cost', 'Views_Google', 'YouTube Earned Views', 'Your estimated revenue (USD)', "ROAS_{}".format(day)]]
    df["Earned View Value {} days".format(day)] = df['Your estimated revenue (USD)']/df['YouTube Earned Views']
    output_df = output_df.merge(df[["Country", "Earned View Value {} days".format(day)]], on="Country", how="left")
	
def calculate_max_bid(row):
    weights = {}
    minimum_number_views = 10
    type_of_view = "Ad Views"
    for day in [1, 7, 14, 30]:
        weights[day] = 0
    if row[type_of_view + ' 1 days'] >= minimum_number_views:
        weights[1] = .4
    if row[type_of_view + ' 7 days'] >= minimum_number_views:
        weights[7] = .3
    if row[type_of_view + ' 14 days'] >= minimum_number_views:
        weights[14] = .2
    if row[type_of_view + ' 30 days'] >= minimum_number_views:
        weights[30] = .1
    total_weight = np.sum(list(weights.values()))
    if total_weight == 0:
        return row['Ad group max. CPV']
    new_max_bid = 0
    for day in [1, 7, 14, 30]:
        weights[day] /= total_weight
        new_max_bid += row["Earned View Value {} days".format(day)] * weights[day]
        
        if np.isnan(new_max_bid) or np.isinf(new_max_bid):
            return row['Ad group max. CPV']
    
    return new_max_bid

output_df["Max Bid"] = output_df.apply(calculate_max_bid, axis=1)

new_max_bid_df = output_df[output_df['Max Bid'] != output_df['Ad group max. CPV']][['Campaign', 'Ad group', 'Max Bid']]

new_max_bid_df.to_csv("new_max_bid_ads.csv", index=False)