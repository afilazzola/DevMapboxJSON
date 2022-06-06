#!/bin/bash

for days in {01..31};  
    do for months in {07..08}; 
    do aws s3 cp s3://mapbox-movement-uni-toronto-shared/v0.2/daily-24h/v2.0/CA/quadkey/total/2021/$months/$days/data/0302231.csv data/mapboxFiles/0302231_$days-$months.csv;
    done; done; 


