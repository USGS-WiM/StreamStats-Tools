
#------------------------------------------------------------------------------
#----- PercentOverlayWrapper.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2018 WiM - USGS

#    authors:  Jeremy K. Newson USGS Web Informatics and Mapping
# 
#   purpose:  Wrapper to test and document percentOverlay use
#          
#discussion:  
#       

#region "Comments"
#09.20.2018 jkn - Created
#endregion

#region "Imports"
import traceback
import datetime
import time
import os
import argparse
import arcpy
from arcpy import env
from arcpy.sa import *
import json
from PercentOverlayAgent import *

#endregion

##-------1---------2---------3---------4---------5---------6---------7---------8
##       Main
##-------+---------+---------+---------+---------+---------+---------+---------+
class PercentOverlayWrapper(object):
    #region Constructor
    def __init__(self):
        Results = None
        try:
            mask = None
            features = None
            workingDir = ''


            parser = argparse.ArgumentParser()
            #For project ID
            parser.add_argument("-maskpath", help="specifies pourpoint geojson feature ", type=str, 
                                default = 'D:\WiM\Projects\StreamStats\PercentOverlay\VA20180221204634856000\Layers\GlobalWatershed.shp')
            #Within this EPSG code
            parser.add_argument("-featuresPath", help="specifies nssRegions ", type=str, 
                                default = 'D:\WiM\Projects\StreamStats\PercentOverlay\SS_regionPolys.gdb')
                           
            args = parser.parse_args()
            startTime = time.time()
            self._sm("Start routine")
            
            if(args.maskpath == ""): raise Exception ("Mask is required")   
            workingDir = self.getWorkspace(args.maskpath) 

            mask = args.maskpath

            if(args.featuresPath == ""): raise Exception ("FeaturePath is required")       
            features = args.featuresPath           
            
            with PercentOverlayAgent(workingDir, features) as pcntOverlay:
                if (not pcntOverlay.isInit): raise Exception('PercentOverlayAgent failed to initialize')               
                
                Results = pcntOverlay.Execute(mask)               
            #end with
            if(not Results): raise Exception ("Percent execute Failed")


            self._sm('Finished.  Total time elapsed:'+ str(round((time.time()- startTime)/60, 2))+ 'minutes')


        except:
             tb = traceback.format_exc()
             self._sm( "Error executing wrapper "+tb,"Error")
             Results = {tb}

        finally:
            print(json.dumps(Results))
            
            # Expectedresults
            #[ 
            #  {  
            #    "name":  "Low_Flow_Region_2",  
            #    "code":  "gc737",  
            #    "percent":  100.0,  
            #    "areasqmeter":  222759899.46509716,  
            #    "maskareasqmeter":  222759899.46509716  
            # },
            # {  
            #    "name":  "Statewide_Mean_and_Base_Flow",  
            #    "code":  "gc741",  
            #    "percent":  100.0,  
            #    "areasqmeter":  222759899.46509716,  
            #    "maskareasqmeter":  222759899.46509716  
            # }, 
            #  {  
            #    "name":  "Peak_Flow_Region_3",  
            #    "code":  "gc909",  
            #    "percent":  100.0,  
            #    "areasqmeter":  222759899.46509716,  
            #    "maskareasqmeter":  222759899.46509716  
            # }
            #]
    
    
    def _sm(self,msg,type="INFO", errorID=0):        
        print(msg)
    
    def getWorkspace(self, item):
      '''Return the Geodatabase path from the input table or feature class.
      :param input_table: path to the input table or feature class 
      '''
      workspace = os.path.dirname(item)
      return os.path.dirname(workspace)

if __name__ == '__main__':
    PercentOverlayWrapper()
