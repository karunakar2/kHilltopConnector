import sys
import os
import datetime
from io import StringIO
import json

import requests as req
import requests_cache as reqC

#import xml.etree.ElementTree as eT
import defusedxml.ElementTree as eT

import numpy as np
import pandas as pd

import kHilltopConnector.openDataLinks as openDataLinks

#the actual main function
def main(args):
    print('the module on command line has reduced functionality')
    print('execute without any arguments for help')
    if len(args)<3:
        print('the arguments are site,measurement,myStartDate,myEndDate,daily')
        print('If myStartDate is set false, the data is fetched from begining')
        print('daily takes a boolean value, if true Daily mean variables are used')
    #args are site,measurement,myEndDate,myStartDate=None,daily=True
    # parse arguments using optparse or argparse or what have you
    
    kHK = kHilltopConnector()
    kHK.selectMeasurement = arg[1]
    if arg[2] == 'false':
        msd = None
    if arg[4] == 'false':
        dly = False
    (kHK.fetchData(arg[0],arg[3],msd,daily=dly)).to_csv(str(arg[0])+str(arg[1])+'.csv')
    
    print('Please check the file in the directory where this script is run from')
    
#backup in case main is called
if __name__ == '__main__':
    main(sys.argv[1:])

class kHilltopConnector:

    #all non exposed static variables go here
    _apiRoot = None
    _initialised = False
    
    #all exposed static variables go here
    measurementsList = [str]                #Ref1, constant through out the session

    #what's current
    #selectMeasurement = ''                  #takes a string from Ref1 || defined as property
    siteList = [str]                        #Ref2, sites available against select measurement
    #selectSite = ''                         #takes a string from Ref2 || defined as property
    selectSiteLocation = [float,float]      #[Lat,Long] string notation
    selectSiteMeasurementEndTime = datetime.datetime.fromisoformat('1900-07-01')    #datetime object

    ##this function initiates the class
    def __init__(self,apiUrl='',refreshInterval=15*60,minimalist=False,enableDebug=False):
        #this is the key to let other functions know if we need debugging info
        self.debug = enableDebug
        
        #the caching reduces the number of hits to the origin server
        if refreshInterval > 0:
            reqC.install_cache('hilltop_cache', backend='sqlite', expire_after=refreshInterval)
        
        #read all the existing api URLs
        apiArchive = openDataLinks.apiArchive()
        
        #set the custom api url of the hilltop
        if apiUrl != '' :
            if '.hts' in apiUrl:
                self._apiRoot = apiUrl
            else:
                if apiUrl in apiArchive.keys() :
                    self._apiRoot = apiArchive[apiUrl]['Hilltop']
            if '?' not in self._apiRoot:
                self._apiRoot += '?'
        else:
            print('Api url is required, you can also specify one of following preloaded keys')
            print([x for x in apiArchive.keys() if '-' not in x ])
        
        if self._apiRoot is None:
            self.__myException('No Api Url selected','')
        
        if not minimalist :
            print('The inititialisation takes quite sometime depending on the connection bandwidth')
            #preload the necessary data
            self.measurementsList = []
            _ = self.__getMeasurementList() #get all the measurements
        
            #get the positional information of the observation sites
            _ = self.__getPosInfo()
            
            #set the initialisation variable to true
            self._initialised = True
            
        if self.debug:
            pass
            #print(self.__getMeasurementList())
            #print(self.__getPosInfo())
        
        #self.selectMeasurement = property(self.__get_measurement, self.__set_measurement)
        #self.selectSite = property(self.__get_site, self.__set_site)
        
        #signal all good
        if self._initialised:
            print('kHilltopConnector object is ready')
    
    #General functions and are not available outside
    #---------------------------------------------------------------
    ##this function is to raise all exceptions from this datafetch class
    def __myException(self,err,calFn,url=None):
        fn = sys._getframe(1).f_code.co_name
        if fn == '__init__':
            fn = 'Module'
        if self.debug and url != None:
            print(url)
        raise RuntimeError('kHilltopConnector:',fn,' says ',err)
        return None
    
    ##this function interfaces with the web and gets data
    def __webFetch(self,myWebRequest):
        if self._apiRoot is None:
            return None
        if myWebRequest != '' and isinstance(myWebRequest, str):
            if self.debug:
                print(myWebRequest)
                print('Start online transaction',sys._getframe(1).f_code.co_name)
                
            #r = req.get(myWebRequest)
            #the following method can add params if required params=request_params if auth is needed
            s = req.Session()
            p = req.Request('GET', myWebRequest).prepare()
            r = s.send(p)
            
            if self.debug:
                print('End online transaction')
            #print(r.text,'reply')
            #do sanity checks before returning r
            try:
                root = eT.fromstring(r.content) #returns root directly
                
                namespaces = dict([
                    node for _, node in eT.iterparse(
                        StringIO(r.text), events=['start-ns']
                    )
                ])
                
                return root, namespaces
            except Exception as er:
                self.__myException('xml reply error','API url',url=myWebRequest)
        else :
            self.__myException('Missing url',sys._getframe(1).f_code.co_name)
            return None

    ##this function gets the list of measurements available for acces
    def __getMeasurementList(self):
        if len(self.measurementsList) < 1:
            myWebRequest = self._apiRoot+'Service=Hilltop&Request=MeasurementList'
            root, _ = self.__webFetch(myWebRequest)
            #parse measurement list xml to array
            for child in root.iter('*'):
            #print(child.tag,child.attrib)
                if child.tag == 'Measurement':
                    self.measurementsList.append(child.attrib['Name'])
            if len(self.measurementsList) < 1:
                self.__myException('empty Measurement list returned','',url=myWebRequest)
        else :
            if self.debug:
                print(self.measurementsList)
        return self.measurementsList
    
    #this function gets the position information of the sites from hilltop server
    _allStationLocation = pd.DataFrame() 
    def __getPosInfo(self, bboxString=None):
        if (self._allStationLocation).empty :
            myWebRequest=self._apiRoot+'Service=WFS&Request=GetFeature&TypeName=SiteList'
            if bboxString:
                assert isinstance(myWebRequest, str), self.__myException('bbox should be a string','')
                bbox = '&BBox='+bboxString #-46.48797124,-167.65999182,-44.73293297,168.83236546
                myWebRequest += bbox

            root, namespaces = self.__webFetch(myWebRequest)
            sites = []
            lat = []
            lon = []
            #namespaces = {'gml':"http://www.opengis.net/gml"} # add more as needed
            for item in root.findall('gml:featureMember/SiteList', namespaces):
                #print(item.tag,item.attrib)
                mySite = None
                for child in item:
                    #print(child.tag,'&',child.attrib,'&',child.text)
                    myLoc = []
                    if (child.tag == 'Site'):
                        mySite = child.text
                    if(child.tag == 'Location'):
                        myLoc = (child.find('gml:Point/gml:pos', namespaces)).text.split()
                    if len(myLoc)>0 and mySite != '':
                        sites.append(mySite)
                        lat.append(myLoc[0])
                        lon.append(myLoc[1])

            df={'Site':np.array(sites), 'Latitude':np.array(lat), 'Longitude':np.array(lon)}
            self._allStationLocation = pd.DataFrame(df, columns = ['Site','Latitude','Longitude'])
        
        return self._allStationLocation
        
    #set the lat long of current site
    def __thisSiteLatLong(self,site):
        if site in self._allStationLocation['Site'].values:
            redDf = self._allStationLocation[self._allStationLocation['Site'] == site]
            self.selectSiteLocation = [redDf['Latitude'].values[0],redDf['Longitude'].values[0]]
        else :
            if self.debug:
                temp = (self._allStationLocation[(self._allStationLocation['Site']).str.startswith(site[0])])
                pd.set_option('display.max_rows', len(temp))
                print(temp.head())
                pd.reset_option('display.max_rows')
                print('please report, if you see your station in list but coordinates are not available')
            self.__myException(site+" doesn't seem to have coordinates associated with it",'')
        return None
    
    #this function gets the end time of the measurement, useful when not specified by the user
    def __getSiteEndTime(self,site=None, measurement=None):
        if site == None:
            site = self.selectSite
        if measurement == None:
            measurement = self.selectMeasurement
        
        if site != None and measurement != None :
            myWebRequest=self._apiRoot+"Service=SOS&Request=GetObservation&FeatureOfInterest="+site+"&ObservedProperty="+measurement
            root, namespaces = self.__webFetch(myWebRequest)
            endTime = root.find('./wml2:observationMember/om:OM_Observation/om:resultTime/gml:TimeInstant//gml:timePosition',namespaces).text
            #parse the string to datetime object for consistency
            endTime = datetime.datetime.fromisoformat(endTime)
            self.selectSiteMeasurementEndTime = endTime
            return endTime
        else :
            self.__myException('site and measurement are empty','',url=myWebRequest)
            return None

    ##this function tries fetching all the sites for a given measurement
    def __getSiteListPerMeasurement(self,measurement=None) -> np.dtype.str:
        #this can't be stored as this is specific to each measurement
        #make sure measurement is a string
        assert isinstance(measurement, str), self.__myException(str(measurement)+'is not valid','')
        
        if measurement == None :
                self.__myException('please provide a valid measurement','')
        
        #make sure the requested measurement type exists
        if measurement in self.__getMeasurementList() :
            #get all the sites for requisite measurement type
            myWebRequest =  self._apiRoot + 'Service=Hilltop&Request=SiteList&Measurement='+measurement
            root, _ = self.__webFetch(myWebRequest)
            sites = []
            for child in root.iter('*'):
                #print(child.tag,child.attrib)
                if child.tag == 'Site':
                    sites.append(child.attrib['Name'])
        else:
            self.__myException(measurement+' doesnt exist in catalogue','')
            
        if len(sites) <1 :
            self.__myException(measurement+' sites list returned is empty','',url=myWebRequest)
        else : 
            return sites
            
            
    ###setters and getters------------------------
    #getter and setter for select measurement - to update the site list accordingly
    __measurement = ''
    def __get_measurement(self) -> str:
        return self.__measurement
    def __set_measurement(self, measurement):
        if measurement in self.__getMeasurementList():
            self.__measurement = measurement
            self.siteList = self.__getSiteListPerMeasurement(measurement) #exposed variable
            #return __get_measurement()
        else :
            self.__myException(measurement+' not in the server list','')
            return None
    selectMeasurement = property(__get_measurement, __set_measurement) #moved to init

    #getter and setter for select site - to update location field accordingly.
    __selectSite = ''
    def __get_site(self) -> str:
        return self.__selectSite
    def __set_site(self,site):
        if self.debug:
            print('site for ',self.selectMeasurement)
        if self._initialised:
            if site in self.__getSiteListPerMeasurement(self.selectMeasurement):
                self.__selectSite = site
                self.__thisSiteLatLong(site)
                self.__getSiteEndTime(site)
                #return __get_site()
            else: return None
        else : return None
    selectSite = property(__get_site, __set_site) #see init
    
    
    #---------------------------------------------------------------------
    #Public functions
    #---------------------------------------------------------------------
    
    #Functions to get the maintenance
    #---------------------------------------------------------------------
    def clobberCache(self) -> None:
        try:
            os.remove("hilltop_cache.sqlite")
        except OSError as e: 
            if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
                raise # re-raise exception if a different error occurred
    
    #data fetching functions
    #-------------------------------------------------------------------------
    # let know the return type with ->
    def fetchData(self, site=None, myStartDate=None, myEndDate=None, measurement=None, daily=True, scaleFactor=1) -> pd.DataFrame():
        #update measurment if in case provided
        if measurement != None :
            if measurement in self.__getMeasurementList() :
                self.selectMeasurement = measurement
            else : return None
        else :
            #use the existing measurement to make sure, the site is valid
            measurement = self.selectMeasurement
            
        #make sure site is valid for above measurement
        if site != None :
            assert isinstance(site, str), self.__myException(str(site)+'is not valid','')
            if site in self.siteList :
                self.selectSite = site
            else:
                self.__myException(site+' and '+measurement+' are not connected','')
        else :
            site = self.selectSite
            
        if myEndDate != None:
            assert isinstance(myEndDate,datetime.date), self.__myException(str(myEndDate)+'is not valid','')
        else :
            myEndDate = str((self.__getSiteEndTime()).date())
            
        if myStartDate != None:
            assert isinstance(myStartDate,datetime.date), self.__myException(str(myStartDate)+'is not valid','')
        
        #make sure the site is for said measurement
        #local filling variables
        timeList = []
        obsList = []
        
        #get the observation data for the site
        if myEndDate != None:
            myWebRequest =  self._apiRoot+'Service=Hilltop&Request=GetData&Site='+site+'&Measurement='+measurement+'&To='+str(myEndDate)
            if daily:
                myWebRequest += "&Interval=1 day&method=Average"
            if myStartDate is not None:
                myWebRequest += '&From='+str(myStartDate)

            root, _ = self.__webFetch(myWebRequest)
            for child in root.iter('E'):
                for miter in child.iter('*'):
                    if miter.text != 'nan':
                        if miter.tag == 'T':
                            try:
                                timeList.append(np.datetime64(datetime.datetime.strptime(miter.text,'%Y-%m-%dT%H:%M:%S')))
                            except Exception as er :
                                timeList.append(float('NAN'))
                                print(er)
                                                
                        if miter.tag == 'I1':
                            try:
                                obsList.append(float(miter.text))
                            except Exception as er:
                                obsList.append(float('NAN'))
                                print(er)

            df={'timestamp':np.array(timeList), measurement:np.true_divide(np.array(obsList).astype('float'),scaleFactor)}
            data = pd.DataFrame (df, columns = ['timestamp',measurement])
            return data
        else :
            self.__myException(str(myEndDate)+' end date is not valid','')
        
        
            
            
