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


# https://www.mapbox.com/blog/how-to-utilize-mapbox-movement-data-for-mobility-insights-a-guide-for-analysts-data-scientists-and-developers