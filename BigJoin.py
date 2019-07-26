#!/usr/bin/python3

# I know, I know, this is "bad form" to have several imports on a line
import os,sys,re,glob
import pandas
import numpy

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
            # for each year in range, create a joined data table with weather
            # and crop performance data
            self.yearly(yearInt)

    def preProcWeather(self):
        self.dailyWeather = pandas.read_csv('daily_weather2013to2016.csv')
        # modify adm2_code column for join-ability
        self.dailyWeather['FIPScounty'] = \
                self.dailyWeather['adm2_code'].str.replace('US','').astype(str)
        self.dailyWeather['earlYr'] = numpy.where(self.dailyWeather.doy < \
                                                180, True, False)

        # Create new DFs to analyze semi-yearly weather trends
        _aggRainByCounty = self.dailyWeather[self.dailyWeather.earlYr==True]\
                [['FIPScounty','met_p_mm']].groupby(['FIPScounty']).sum()
        _aggRainByCounty.columns = ['aggRain']
        _meanAvgTByCounty = self.dailyWeather[self.dailyWeather.earlYr==True]\
                    [['FIPScounty','met_avg_t']].groupby(['FIPScounty']).mean()
        _meanAvgTByCounty.columns = ['meanAvgT']
        _aggGDDByCounty = self.dailyWeather[self.dailyWeather.earlYr==True]\
                    [['FIPScounty','met_gdd']].groupby(['FIPScounty']).sum()
        _aggGDDByCounty.columns = ['aggGDD']
        _meanMaxVPDbyCounty = self.dailyWeather[self.dailyWeather.earlYr==True]\
                [['FIPScounty','met_max_vpd']].groupby(['FIPScounty']).mean()
        _meanMaxVPDbyCounty.columns = ['meanMaxVPD']
        _meanMaxRHbyCounty = self.dailyWeather[self.dailyWeather.earlYr==True]\
                    [['FIPScounty','met_max_rh']].groupby(['FIPScounty']).mean()
        _meanMaxRHbyCounty.columns = ['meanMaxRH']
        _meanMinRHbyCounty = self.dailyWeather[self.dailyWeather.earlYr==True]\
                    [['FIPScounty','met_min_rh']].groupby(['FIPScounty']).mean()
        _meanMinRHbyCounty.columns = ['meanMinRH']

        '''
        Join all grouped quantities created above into a single DF
        Keep adding to original using indices as keys for join so can 
        reset_index at end
        '''
        _holderGrouped = _aggRainByCounty.merge(_meanAvgTByCounty,left_index =\
                            True,right_index=True,how='outer')
        _holderGrouped = _holderGrouped.merge(_aggGDDByCounty,left_index =\
                            True,right_index=True,how='outer')
        _holderGrouped = _holderGrouped.merge(_meanMaxVPDbyCounty,left_index =\
                            True,right_index=True,how='outer')
        _holderGrouped = _holderGrouped.merge(_meanMaxRHbyCounty,left_index =\
                            True,right_index=True,how='outer')
        _holderGrouped = _holderGrouped.merge(_meanMinRHbyCounty,left_index =\
                            True,right_index=True,how='outer')

        # Merge back into self.dailyWeather (with a reset_index'd holder)
        self.dailyWeather =self.dailyWeather.merge(_holderGrouped.reset_index()\
                                                   ,how='outer',on='FIPScounty')

    def yearly(self,yearStr):
        # create glob the file (needed due to file naming issues)
        _surveyFile = glob.glob(str(yearStr)+'*Survey Data-Wheat .csv')[0]
        # create pd DF from each year's crop health survey data
        _surveyData = pandas.read_csv(_surveyFile,dtype={'zip_code':str})
        print('Processing data from year '+str(yearStr))

        # join to add zip code to county correlation for survey data
        self.yearlyJoinDict[yearStr] = \
                        _surveyData.merge(self.zipCorrel,left_on='zip_code',\
                                          right_on='ZIP',how='left')

        # create DF with all data (this join adds weather data)
        # join is left join to drop any weather data that doesn't have crop
        # health data
        self.yearlyJoinDict[yearStr] = \
                        self.yearlyJoinDict[yearStr].merge(self.dailyWeather,\
                        left_on=['year','COUNTY'], \
                        right_on=['year','FIPScounty'],how='left')

        # find percentage of hanging join -- rows that have crop health data,
        # but no weather data
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
        
        newColList = ['year', 'area', 'zip_code', 'moisture', 'kernel_weight',\
                      'actual_wheat_ash', 'falling_no', 'protein_12', 'ZIP',\
                      'COUNTY', 'FIPScounty', 'aggRain','meanAvgT', 'aggGDD',\
                      'meanMaxVPD', 'meanMaxRH', 'meanMinRH']
        # create DF containing only yearly summary data for each county
        self.yearlyJoinDict[yearStr][newColList].drop_duplicates().to_csv(\
        'YearlySummary'+str(yearStr)+'.csv',index=False)
        

if __name__ == '__main__':
    Joiner()
