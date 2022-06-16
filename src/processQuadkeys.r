library(tidyverse)

## Load quadkeys for Toronto
quadkeys <- read.table("data/quadkeyListToronto.txt", sep=",")

## load a quadkey file
quadkeyFiles <- list.files("data//mapboxFiles", pattern=".csv", full.names = TRUE)

quadkeyTemp <- read.table(quadkeyFiles[1], sep="|", header=T)
