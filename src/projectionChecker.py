
#------------------------------------------------------------------------------
#----- projectionChecker.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2017 WIM - USGS

#    authors:  Jeremy K. Newson USGS Web Informatics and Mapping
# 
#   purpose:  Wrapper to delineate watershed using split catchement methods
#          
#discussion:  
#       

#region "Comments"
#11.19.2017 jkn - Created
#endregion

#region "Imports"
import traceback
import datetime
import time
import os
import argparse
import arcpy
import json

#endregion

##-------1---------2---------3---------4---------5---------6---------7---------8
##       Main
##-------+---------+---------+---------+---------+---------+---------+---------+
#http://stackoverflow.com/questions/13653991/passing-quotes-in-process-start-arguments
class SpatialRefWrapper(object):
    #region Constructor
    def __init__(self):
        self.tosr = None
        self.fromsrname = None
        self.availablespatialRef = set();
        try:
            parser = argparse.ArgumentParser()            
            parser.add_argument("-Directory", help="Parent directory", type=str, default="d:\data\ms")   
            parser.add_argument("-FromSR",type=str,help="Name of Spatial Reference to project from", default='NAD_1983_Mississippi_TM')
            parser.add_argument("-ToSR",type=str,help="WKID or file name of Spatial Reference to project to", default=None)#NAD_1983_Transvers_Mercator.prj
            args = parser.parse_args()

            directory = args.Directory
            if directory == '#' or not directory:
                raise Exception('Directory is not supplied')
            if args.FromSR:
                self.fromsrname = args.FromSR
            if args.ToSR:
                self.tosr = arcpy.SpatialReference()
                self.tosr.loadFromString(args.ToSR)#os.path.join(directory,args.ToSR))

            walk = arcpy.da.Walk(directory, datatype=['FeatureClass','RasterDataset','FeatureDataset' ])
            
            for dirpath, dirnames, filenames in walk:
                dirtype = arcpy.Describe(dirpath).datatype
                if(dirtype == 'Coverage' or dirtype == 'FeatureDataset'):
                    self.__defineProjection(dirpath)
                    continue;               
                else:
                    for filename in filenames:
                        item =os.path.join(dirpath, filename); 
                        self.__defineProjection(item)
                    #next filename
            #next directory
            
            print(self.availablespatialRef)

            
        except:
             tb = traceback.format_exc()
             print(tb)

    def __defineProjection(self, item):
        try:

            try:
                sr = arcpy.Describe(item).spatialReference.name
            except:
                print("error getting spatialReference", item)
                return

            if not self.fromsrname: 
                self.availablespatialRef.add(sr)
                return

            if (sr.lower() == self.fromsrname.lower()):
                if(self.tosr != None):
                    try:
                        print("DefineProj to", self.tosr.name)
                        print("BEFORE",arcpy.Describe(item).spatialReference.name)
                        arcpy.DefineProjection_management(item,self.tosr)
                        print("AFTER",arcpy.Describe(item).spatialReference.name)
                        print ("sucessfully Defined", item)
                    except:
                        print("error Defining spatialReference", item)
                else:
                    print (item, sr)
        except :
            tb = traceback.format_exc()
            print(tb)

    
if __name__ == '__main__':
    SpatialRefWrapper()
