#!/usr/bin/python3

# I know, I know, this is "bad form" to have several imports on a line
import os,sys,re
import pandas

# read in file
allWeather = pandas.read_csv('daily_weather.csv',index_col=0)


# output weather with years of interest (2013 to 2016)
allWeather[allWeather.year.isin([2013,2014,2015,2016])].\
    to_csv('daily_weather2013to2016.csv',index=False)

