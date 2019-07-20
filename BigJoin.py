#!/usr/bin/python3

# I know, I know, this is "bad form" to have several imports on a line
import os,sys,re,glob
import pandas

class Joiner:

    def __init__(self):
        self.zipFile = 'ZIP_COUNTY_032013.xlsx'
        # read in preferred crosswalk file, throw out unuseful cols
        # need to use str format for zip and county
        self.zipCorrel = pandas.read_excel(self.zipFile,usecols=[0,1,5],\
                        dtype={'ZIP':str,'COUNTY':str,'TOT_RATIO':float})
        self.preProcWeather()
        self.yearlyJoinDict = {}
        for yearInt in range(2014,2017):
            self.yearly(yearInt)

    def preProcWeather(self):
        self.dailyWeather = pandas.read_csv('daily_weather2013to2016.csv')
        self.dailyWeather['FIPScounty'] = \
                self.dailyWeather['adm2_code'].str.replace('US','').astype(str)
        
    def yearly(self,yearStr):
        _surveyFile = glob.glob(str(yearStr)+'*Survey Data-Wheat .csv')[0]
        _surveyData = pandas.read_csv(_surveyFile,dtype={'zip_code':str})
        print('Processing data from year '+str(yearStr))
        self.yearlyJoinDict[yearStr] = \
                        _surveyData.merge(self.zipCorrel,left_on='zip_code',\
                                          right_on='ZIP',how='left')
        self.yearlyJoinDict[yearStr] = \
                        self.yearlyJoinDict[yearStr].merge(self.dailyWeather,\
                        left_on='COUNTY',right_on='FIPScounty',how='left')

        _naPct = len(self.yearlyJoinDict[yearStr][self.yearlyJoinDict[yearStr]\
                .adm2_code.isnull()])/len(self.yearlyJoinDict[yearStr])
        _dropNABool = input('The percent of null entries, a reflection of '\
                            +'the quality of the join, is '+str(100.0*_naPct)\
                            +'\nDo you want to drop them? Y/N\n')
        if _dropNABool == 'Y':
            self.yearlyJoinDict[yearStr].dropna(subset=['adm2_code'],\
                                                inplace=True)
        elif _dropNABool == 'N':
            print('Keeping all rows, some have empty columns from an '\
                  +'incomplete join')
        else:
            print('Please restart script, and answer "Y" or "N" at previous'\
                  +' prompt.\n Terminating script now!')
            sys.exit()
        self.yearlyJoinDict[yearStr].to_csv('FullJoin'+str(yearStr)+'.csv',\
                        index=False)

if __name__ == '__main__':
    Joiner()
