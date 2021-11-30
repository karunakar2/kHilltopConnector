import sys
import os
import math

import datetime
from dateutil.parser import parse
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
    
    #debugging flag
    _debugChange = False
    
    #all non exposed static variables go here
    _apiRoot = None
    _initialised = False
    _allStationLocation = pd.DataFrame()
    __measurement = ''
    __selectSite = ''
    
    #all exposed static variables go here
    measurementsList = [str]                #Ref1, constant through out the session

    #what's current
    #selectMeasurement = ''                  #takes a string from Ref1 || defined as property
    siteList = [str]                        #Ref2, sites available against select measurement
    #selectSite = ''                         #takes a string from Ref2 || defined as property
    selectSiteLocation = [float,float]      #[Lat,Long] string notation
    selectSiteMeasurementEndTime = datetime.datetime.fromisoformat('1900-07-01')    #datetime object
    selectSiteGaugings = pd.DataFrame()

    ##this function initiates the class
    def __init__(self,apiUrl='',refreshInterval=15*60,minimalist=False,enableDebug=False):
        #this is the key to let other functions know if we need debugging info
        self.debug = enableDebug
        #print(self._debugChange)
        
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
            
            #get this first, as it often can be smaller and less server time consuming than measlist
            #get the positional information of the observation sites
            _ = self.__getPosInfo()
            
            #preload the necessary data
            self.measurementsList = []
            _ = self.__getMeasurementList() #get all the measurements
            
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
            headers = {
                'Accept': "*/*",
                'User-Agent': 'Mozilla/5.0',
                'From': 'hilltopConnector@karunakar.co.nz'  # This is another valid field
            }
            p = req.Request('GET', myWebRequest, headers=headers).prepare()
            try:
                r = s.send(p)
                r.raise_for_status()
            except req.exceptions.Timeout:
                # Maybe set up for a retry, or continue in a retry loop
                print('request timed out')
                return None, 'erTO'
            except req.exceptions.ConnectionError as errc:
                print ("Error Connecting",errc)
                sys.exit(errc)
                #return None,'erCE'
            except req.exceptions.TooManyRedirects:
                # Tell the user their URL was bad and try a different one
                print('too many redirects')
                return None,None
            except req.exceptions.HTTPError as err:
                #raise SystemExit(err)
                print('unauthorised access', err)
                return None,None
            except req.exceptions.RequestException as err:
                # catastrophic error. bail.
                print(err)
                return None,None
            except Exception as e:
                print(e)
                return None, None
            self._debugChange = 'webFetch'

            if self.debug:
                print('End online transaction')
            #print(r.text,'reply')
            #do sanity checks before returning r
            try:
                root = eT.fromstring(r.content) #returns root directly
                if self.debug:
                    #print(r.content)
                    print(root)
                
                try:
                    namespaces = dict([
                        node for _, node in eT.iterparse(
                            StringIO(r.text), events=['start-ns'])
                        ])
                except Exception as er:
                    print(er)
                    namespaces = None
                
                return root, namespaces
            except Exception as er:
                print(er)
                self.__myException('xml reply error','API url',url=myWebRequest)
        else :
            self.__myException('Missing url',sys._getframe(1).f_code.co_name)
            return None
    
    def __getMeasListAltWay(self):
        print('Going the iterative query way, this is very slow')
        localMeasList = []
        for thisStn in self._allStationLocation['Site']:
            myWebRequest = self._apiRoot+'Service=Hilltop&Request=MeasurementList&Site='+str(thisStn)
            root, _ = self.__webFetch(myWebRequest)
            for child in root.iter('*'):
                #print(child.tag,child.attrib)
                if child.tag == 'Measurement':
                    localMeasList.append(child.attrib['Name'])
        localMeasList = list(dict.fromkeys(localMeasList)) #remove duplicates
        #print(localMeasList)
        return localMeasList        
    
    ##this function gets the list of measurements available for acces
    def __getMeasurementList(self):
        if len(self.measurementsList) < 1:
            myWebRequest = self._apiRoot+'Service=Hilltop&Request=MeasurementList'
            root, _ = self.__webFetch(myWebRequest)
            if root == 'erTO':
                self.measurementsList = (self.__getMeasListAltWay()).copy()
            else :
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
                        lat.append(float(myLoc[0]))
                        lon.append(float(myLoc[1]))

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
    
    #get the guaging site list
    #this function uses latest pandas
    def __guagingSites(self,measurement:'str'='Stage [Gauging Results]'):
        if (measurement == '') or (measurement == None):
            raise 'Expecting to proper measurement to get the gaugings'
        
        apiExt = 'Service=Hilltop&Request=SiteList&Location=Yes&Measurement='+measurement
        apiExt = apiExt.replace(' ','%20')
        slist = pd.read_xml(self._apiRoot+apiExt)
        slist.replace('None',float('nan'),inplace=True)
        slist.dropna(subset=['Name'],inplace=True)
        siteList = slist['Name'].values
        #print(siteList)
        return siteList
    
    def __getGaugings(self,site,sDate=None,eDate=None, measurement:str='Stage [Gauging Results]'):
        if (measurement == '') or (measurement == None):
            raise Exception('Expecting to proper measurement to get the gaugings')
            
        if eDate == None:
            raise Exception('Please specify an end time for gaugings')
        
        apiExt = 'Service=Hilltop&Request=GetData&Measurement='+measurement 
        if sDate != None:
            apiExt += '&From='+str(sDate)
        apiExt += '&To='+str(eDate)+'&Interval=1 hour&method=Average&Site='
        apiExt1 = apiExt + site
        apiExt1 = apiExt1.replace(' ','%20') #%20 is web equivalent of space char
        #print(baseUrl+apiExt1)
        
        #xml needs translation
        #response = requests.get(self._apiRoot+apiExt1)
        #root = ET.fromstring(response.text)
        root,_ = self.__webFetch(self._apiRoot+apiExt1)
        thisData = {}
        myColumns = {}
        myDivisor = {}
        for child in root:
            for thisChild in child:
                if thisChild.tag == 'DataSource':
                    for children in thisChild:
                        temp = list(children.attrib.keys())
                        if len(temp)>0:
                            #print(children.attrib[temp[0]])
                            colKey = children.attrib[temp[0]]
                            if children.tag == 'ItemInfo':
                                for data in children:
                                    if data.tag == 'ItemName':
                                        colVal = data.text #string
                                    if data.tag == 'Divisor':
                                        divVal = float(data.text) #number
                            myColumns[int(colKey)] = colVal
                            myDivisor[int(colKey)] = divVal
                    pass
                if thisChild.tag == 'Data':
                    for children in thisChild:
                        temp = []
                        for data in children:
                            if data.tag == 'T':
                                key = data.text
                            else:
                                temp.append(float(data.text))
                        if len(temp) > 0:
                            thisData[key] = temp
        
        myDivisor[0] = 1 #have to do it for that first and unknown column
        myDf = pd.DataFrame(data=thisData).T
        myDf = myDf.apply(lambda x: x/myDivisor[x.name])
        myDf.rename(columns=myColumns,inplace=True)
        myDf.index.name = 'timestamp'
        myDf.index = pd.to_datetime(myDf.index)
        #display(myDf)
        #myDf.plot(y='Flow',logy=True)
        #plt.show()
        
        self.selectSiteGaugings = myDf
        return myDf
    
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
    def whatsNearest(self,lat:'deg',lon:'deg',searchRadius:'deg len'=0.05)->'Site': #5km approx
        lat = float(lat)
        lon = float(lon)
        if (-90<lat<90) and (-180<=lon<=360):
            redDf = self._allStationLocation[(self._allStationLocation['Latitude'].between(lat-searchRadius,lat+searchRadius))&(self._allStationLocation['Longitude'].between(lon-searchRadius,lon+searchRadius))].copy()
            #calc nearest and sort by distance
            redDf['dist'] = redDf.apply(lambda row:
                                          math.sqrt((float(lat) - float(row.Latitude))**2 
                                          + (float(lon) - float(row.Longitude))**2)
                                          , axis=1)
            redDf.sort_values(by=['dist'],inplace=True)
            redDf.reset_index(drop=True, inplace=True)
            return redDf['Site'][0]
    
    def fetchData(self, site=None, myStartDate=None, myEndDate=None, measurement=None, avgDays = 1, qCode=False, fetchYearsAtATime = 0, scaleFactor=1, drillDown=False) -> pd.DataFrame():
        
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
        
        if myStartDate != None:
            assert isinstance(myStartDate,datetime.date), self.__myException(str(myStartDate)+'is not valid','')
        
        if myEndDate != None:
            #assert isinstance(myEndDate,datetime.date), self.__myException(str(myEndDate)+'is not valid','')
            try:
                if isinstance(myEndDate,datetime.date):
                    temp = myEndDate
                else:
                    temp = parse(myEndDate)
                    myEndDate = temp
                self.selectSiteMeasurementEndTime = temp
            except Exception as er:
                print(er)
                self.__myException(str(myEndDate)+'is not valid','')
                
        else :
            myEndDate = (self.__getSiteEndTime()).date()
        
        if myStartDate == None:
            drillDown = False
        
        #get the observation data for the site
        if myEndDate != None:
            combDf = pd.DataFrame()
            nextIter = True
            eDate = myEndDate
            while nextIter:
                #local filling variables
                timeList = []
                obsList = []
                qualList = []
                try:
                    if myStartDate is not None:
                        if eDate <= myStartDate:
                            break
                    
                    if isinstance(fetchYearsAtATime,int):
                        sDate = eDate.replace(year = eDate.year - fetchYearsAtATime)
                    elif isinstance(fetchYearsAtATime,float):
                        if self.debug:
                            print('years converted to days',365*fetchYearsAtATime)
                        sDate = eDate - datetime.timedelta(days = int(365*fetchYearsAtATime))
                    else:
                        raise Exception('couldnt parse fetchYearAtATime',fetchYearsAtATime)
                    
                    if fetchYearsAtATime == 0:
                        myWebRequest =  self._apiRoot+'Service=Hilltop&Request=GetData&Site='+site+'&Measurement='+measurement+'&To='+str(eDate)+"&Interval="+str(avgDays)+" day&method=Average"
                        if myStartDate is not None:
                            myWebRequest += '&From='+str(myStartDate)
                        nextIter = False
                        
                    else:
                        myWebRequest =  self._apiRoot+'Service=Hilltop&Request=GetData&Site='+site+'&Measurement='+measurement+'&From='+str(sDate)+'&To='+str(eDate)
                        eDate = sDate
                        if qCode :
                            myWebRequest += '&ShowQuality=Yes' 
                            #hilltop doesn't logistically average codes and not available at a sub sampling scale
                        else :
                            myWebRequest += "&Interval="+str(avgDays)+" day&method=Average"

                    root, _ = self.__webFetch(myWebRequest)
                    temp = root.find('Error')
                    if temp != None:
                        print('Error found in Hilltop reply')
                        if not drillDown: #force queryingdown till start date
                            raise Exception(temp.text)
                        else:
                            print('drilling down to start date')
                            pass
                    
                    for child in root.iter('E'):
                        for miter in child.iter('*'):
                            if miter.text != 'nan':
                                if miter.tag == 'T':
                                    try:
                                        timeList.append(np.datetime64(datetime.datetime.strptime(miter.text,'%Y-%m-%dT%H:%M:%S')))
                                    except Exception as er :
                                        timeList.append(float('NAN'))
                                        print('time tag', er)

                                if miter.tag == 'I1':
                                    try:
                                        obsList.append(float(miter.text))
                                    except Exception as er:
                                        obsList.append(float('NAN'))
                                        print('value tag', er)

                                if miter.tag == 'Q1':
                                    try:
                                        qualList.append(int(miter.text))
                                    except Exception as er:
                                        qualList.append(-999)
                                        print('quality code tag',er)
                    
                    df={'timestamp':np.array(timeList),
                        measurement:np.true_divide(np.array(obsList).astype('float'), scaleFactor)}
                    if len(qualList) >0:
                        df['qCode'] = np.array(qualList)
                    data = pd.DataFrame (df, columns = df.keys())

                    if self.debug:
                        print(data.head())
                    
                    if qCode:
                        #[data['timestamp'].dt.date]).agg(
                        data = data.groupby(pd.Grouper(key='timestamp', freq=str(avgDays)+'D')).agg(
                            {measurement:'mean', 'qCode':'min'})[[measurement, 'qCode']].reset_index() 
                            #drop=True isn't desired as it would drop the timestamp column as index
                        if self.debug:
                            print(data.head())
                                        
                    data.drop_duplicates(subset='timestamp', inplace=True)
                    if len(data) > 0:
                        combDf = pd.concat([combDf, data])
                except Exception as er:
                    print(er)
                    nextIter = False
            
            
            #check if we can fetch gaugings for this site
            if measurement == 'Flow [Water Level]' or measurement == 'Flow':
                thisSiteList = self.__guagingSites()
                try:
                    if site in thisSiteList:
                        self.__getGaugings(site=site,eDate=combDf['timestamp'].max())
                        #don't use sDate all gaugings might be valid
                        print('Gauging information available, access via selectSiteGaugings variable')
                    else:
                        print(site,thisSiteList)
                except Exception as er:
                    print(er)
                    pass
            try:
                combDf.sort_values(by=['timestamp'], inplace=True)
                return combDf
            except Exception as er:
                print(combDf.head())
                raise er
                #return None
        else :
            self.__myException(str(myEndDate)+' end date is not valid','')