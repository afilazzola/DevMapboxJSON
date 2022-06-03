## Load libraries
import os
import requests
from io import StringIO
import pandas as pd
from shapely.geometry import Point
import numpy as np


## Read in mapbox data
mapboxTest = pd.read_csv("data//mapboxFiles//seattle.sample.monthly.csv", sep=',')
mapboxTest.head()

## Read in airports test dataset
airportDF = pd.read_csv("data//airports.csv", sep=',')
airportDF.head()

# Drop unnecessary columns
airportDF = airportDF.drop(columns=['id', 'ident', 'elevation_ft','continent',  'scheduled_service', 'gps_code', 'local_code', 'home_link', 'wikipedia_link', 'keywords'])

# Filter to US only airports
USairportDF = airportDF[airportDF["iso_country"] == 'US']

# Filter dataframe to include only large airports
USairportLargeDF = USairportDF[USairportDF["type"] == "large_airport" ]
USairportLargeDF = USairportLargeDF.sort_values(by=["iso_region"])

# Inspect the first few rows of the DataFrame
USairportLargeDF.head()

def coords2points(df_row):
    return Point(df_row['longitude_deg'], df_row['latitude_deg'])

# Apply the coords2points function to each row and store results in a new column 
USairportLargeDF['airport_center'] = USairportLargeDF.apply(coords2points, axis=1)


from functools import partial
import pyproj
from shapely import geometry
from shapely.geometry import Point
from shapely.ops import transform

def aeqd_reproj_buffer(center, radius=1000):
    # Get the latitude, longitude of the center coordinates
    lat = center.y
    long = center.x
    
    # Define the projections
    local_azimuthal_projection = "+proj=aeqd +R=6371000 +units=m +lat_0={} +lon_0={}".format(
        lat, long
    )
    wgs84_to_aeqd = partial(
        pyproj.transform,
        pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
        pyproj.Proj(local_azimuthal_projection),
    )
    aeqd_to_wgs84 = partial(
        pyproj.transform,
        pyproj.Proj(local_azimuthal_projection),
        pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
    )

    # Transform the center coordinates from WGS84 to AEQD
    point_transformed = transform(wgs84_to_aeqd, center)
    buffer = point_transformed.buffer(radius)
    
    # Get the polygon with lat lon coordinates
    circle_poly = transform(aeqd_to_wgs84, buffer)
    
    return  circle_poly


# Map the aeqd_reproj_buffer function to all airport coordinates.
USairportLargeDF["aeqd_reproj_circle"] = USairportLargeDF["airport_center"].apply(aeqd_reproj_buffer)

import mercantile

def generate_quadkeys(circle_poly, zoom):
    return [mercantile.quadkey(x) for x in mercantile.tiles(*circle_poly.bounds, zoom)]



# Create a list of overlapping z18 quadkeys for each airport and add to a new column
USairportLargeDF['z18_quadkeys'] = USairportLargeDF.apply(lambda x: generate_quadkeys(x['aeqd_reproj_circle'], 18),axis=1)

# Create a list of overlapping z7 quadkeys for each airport and add to a new column
USairportLargeDF['z7_quadkeys'] = USairportLargeDF.apply(lambda x:  generate_quadkeys(x['aeqd_reproj_circle'], 7),axis=1)



import os
import requests
from datetime import datetime, timedelta

def download_sample_data_to_df(start_date, end_date, z7_quadkey_list, local_dir, verbose=True):
    """
    Downloads Movement z7 quadkey CSV files to a local dir and reads
    Read the CSV file into a Pandas DataFrame. This DataFrame contains
    **ALL** Z18 quadkey data in this Z7 quadkey.
    
    Args:
        start_date (string): start date as YYYY-MM-DD string.
        end_date (string): end date as YYYY-MM-DD string.
        z7_quadkey_list (list): list of zoom 7 quadkeys as string.
        local_dir (string): local directory to store downloaded sample data.
        verbose (boolean): print download status.
    
    Raises:
        requests.exceptions.RequestException: An exception raised while
            handling your request.
        
    Returns:
        a Pandas DataFrame consists of Z7 quadkey data. DataFrame contains
        **ALL** Z18 quadkey data in this Z7 quadkey.
    
    """

    bucket = "mapbox-movement-uni-toronto-shared"
    
    # Generate range of dates between start and end date in %Y-%m-%d string format
    start = datetime.strptime(start_dt_str, '%Y-%m-%d')
    end = datetime.strptime(end_dt_str, '%Y-%m-%d')
    num_days = int((end - start).days)
    days_range = num_days + 1
    date_range = [(start + timedelta(n)).strftime('%Y-%m-%d') for n in range(days_range)]

    sample_data = []
    for z7_quadkey in z7_quadkey_list:
        for i in range(len(date_range)):
            yr, month, date = date_range[i].split('-')
            url =  f"https://{bucket}.s3.amazonaws.com/v0.2/daily-24h/v0.1.2/US/quadkey/total/2020/{month}/{date}/data/{z7_quadkey}.csv"
    
            if not os.path.isdir(os.path.join("sample_data", month, date)):
                os.makedirs(os.path.join("sample_data", month, date))

            local_path = os.path.join("sample_data", month, date, f'{z7_quadkey}.csv')

            if verbose:
                print (z7_quadkey, month, date)
                print (f'local_path : {local_path}')

            try:
                res = requests.get(url)
                df = pd.read_csv(StringIO(res.text), sep='|')
                convert_dict = {'agg_day_period': 'datetime64[ns]', 'activity_index_total': float, 'geography': str} 
                df = df.astype(convert_dict)
                # Keep leading zeros and save as string
                df['z18_quadkey'] = df.apply(lambda x: x['geography'].zfill(18), axis=1).astype('str')
                df['z7_quadkey'] = df.apply(lambda x: x['geography'][:6].zfill(7), axis=1).astype('str')
                
                sample_data.append(df)
                df.to_csv(local_path, index=False)

            except requests.exceptions.RequestException as e:
                raise SystemExit(e)

            if verbose:
                print (f'Download completed for {z7_quadkey} over date range {start_dt} to {end_dt}')

    return pd.concat(sample_data)


    # Tweak the following set of parameters to include broader time frame or more airports
start_dt_str = "2021-07-01"
end_dt_str = "2021-08-31"

# Find SFO and DEN from the us_airports_df_large DataFrame
sfo = USairportLargeDF[USairportLargeDF["iata_code"] == "SFO"].iloc[0]
den = USairportLargeDF[USairportLargeDF['name'].str.contains("Denver")].iloc[0]

# Add these two airports
airports = [sfo, den]

# Creates a list of z7 quadkeys to download
z7_quadkeys_to_download = []
for airport in airports:
    for z7_quadkey in airport["z7_quadkeys"]:
        z7_quadkeys_to_download.append(z7_quadkey)

# Define a list to append all newly created DataFrames
sample_data_airports = []
for z7 in z7_quadkeys_to_download:
    local_directory = os.path.join(os.getcwd(), f'sample_data_{z7}')
    print ([z7])
    print (local_directory)

    # Run the download script
    sample_data_airports.append(download_sample_data_to_df(start_dt_str, end_dt_str, [z7], local_directory, False))

# Create a DataFrame of all z7 quadkey activity data
sample_data_airports_df = pd.concat(sample_data_airports).sort_values(by=['agg_day_period', 'z18_quadkey'])