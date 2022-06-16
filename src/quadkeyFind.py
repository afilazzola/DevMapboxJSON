## Load libraries
import pandas as pd
from shapely.geometry import Point
from functools import partial
import pyproj
from shapely import geometry
from shapely.ops import transform
import mercantile
import numpy as np

## Read in airports test dataset
cityCoords = pd.DataFrame(
    {"City": ["Toronto"], "State": ["Ontario"], "Country": ["Canada"], "Latitude": [43.6532], "Longitude": [-79.3832]})

def coords2points(df_row):
    return Point(df_row['Longitude'], df_row['Latitude'])

# Apply the coords2points function to each row and store results in a new column 
cityCoords['cityCenter'] = cityCoords.apply(coords2points, axis=1)

def aeqd_reproj_buffer(center, radius=50000): ## 50 km buffer
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
cityCoords["aeqd_reproj_circle"] = cityCoords['cityCenter'].apply(aeqd_reproj_buffer)

def generate_quadkeys(circle_poly, zoom):
    return [mercantile.quadkey(x) for x in mercantile.tiles(*circle_poly.bounds, zoom)]

# Create a list of overlapping z18 quadkeys for each airport and add to a new column
cityCoords['z18_quadkeys'] = cityCoords.apply(lambda x: generate_quadkeys(x['aeqd_reproj_circle'], 18),axis=1)

cityCoords.dtypes

cityCoords['z18_quadkeys'].astype('str').str.split(',') 
cityCoords.to_csv("AllCityQuadkeys.csv")

quadkeyArray = cityCoords["z18_quadkeys"].values

## take first 7 digits for get coarse quadkey
def left(s, amount):
    return s[:amount]

truncatedQuadkey = []
for quadkey in quadkeyArray[0]:
    truncatedQuadkey.append(left(quadkey, 7))

distinctQuadkey = set(truncatedQuadkey)

## Select specific relevant quadkeys
quadkeyList = quadkeyArray[0]
quadkeyList

# write full list of quadkeys to file
with open(r'./data/quadkeyListToronto.txt', 'w') as fp:
    for item in quadkeyList:
        # write each item on a new line
        fp.write("%s\n" % item)
    print('Done')