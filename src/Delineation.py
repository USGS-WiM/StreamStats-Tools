#------------------------------------------------------------------------------
#----- Delineation.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2014 WiM - USGS

#    authors:  Jeremy K. Newson USGS Wisconsin Internet Mapping
# 
#   purpose:  GlobalDelineation <STABBR> <PourPoint> <GlobalWatershedPoint> <GlobalWatershed>
#          
#discussion:  Adapted from John Guthrie's GetGW12.py Global delineation script
#       

#region "Comments"
#08.19.2015 jkn - fixed hucid sync issue. By removing createfeature in __removePolygonHoles__
#08.07.2015 jkn - modified to store in local projection
#11.05.2014 jkn - Created/ Adapted from John Guthrie's getGW12.py
#endregion

#region "Imports"
import traceback
import os
import arcpy
import shutil
import json
import ArcHydroTools
from arcpy.sa import *
import logging

import xml.dom.minidom
#endregion

class Delineation(object):
    #region Constructor
    def __init__(self, regionID, schemas, xml, workspaceID, state_folder):
        self.Message =""
        self.__schemaPath__ = schemas
        self.__xmlPath__ = xml
        self.__regionID__ = regionID       
        self.__dataFolder__ = state_folder
        self.__templatePath__ = os.path.join(self.__schemaPath__,"Layers")
        self.WorkspaceID = os.path.basename(workspaceID)
        self.__WorkspaceDirectory__ = self.__getDirectory__(workspaceID)
        self.error = ""
        self.isComplete = False

        self.__TempLocation__ = os.path.join(self.__WorkspaceDirectory__, "Temp")

        #set up logging
        logdir = os.path.join(self.__TempLocation__, 'delineation.log')

        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler = logging.FileHandler(logdir)
        self.__logger__ = logging.getLogger('delineation')
        self.__logger__.setLevel(logging.INFO)
        self.__logger__.addHandler(handler)
        handler.setFormatter(formatter)
        self.__sm__("Initialized")       
    #endregion   
        
    #region Methods   
    def Delineate(self, PourPoint):
        xmlPath =''
        datasetPath = ''
        featurePath=''
        templateFeaturePath=''
        sr = None
        GWP = None
        GW = None

        try:
            arcpy.env.overwriteOutput = True
            arcpy.env.workspace = self.__TempLocation__
            arcpy.env.scratchWorkspace = self.__TempLocation__

            templateFeaturePath=os.path.join(self.__templatePath__,'{0}' + self.__regionID__)

            sr = arcpy.Describe(self.__templatePath__).spatialReference
            self.__sm__("Template spatial ref: "+ sr.name)

            self.__sm__("Delineation Started") 
            datasetPath = os.path.join(self.__WorkspaceDirectory__, self.WorkspaceID + '.gdb')
            featurePath = os.path.join(datasetPath,'Layers')
            if not arcpy.Exists(datasetPath):
                self.__sm__("datasetPath: " + datasetPath)
                datasetPath = arcpy.CreateFileGDB_management(self.__WorkspaceDirectory__, self.WorkspaceID +'.gdb')[0]
            if not arcpy.Exists(featurePath):
                featurePath = arcpy.CreateFeatureDataset_management(datasetPath,'Layers', sr)[0]

            self.__sm__("creating workspace environment. "+ datasetPath)
            
            GWP = arcpy.CreateFeatureclass_management(featurePath, "GlobalWatershedPoint", "POINT", 
                                                      templateFeaturePath.format("GlobalWatershedPoint") , "SAME_AS_TEMPLATE", "SAME_AS_TEMPLATE",sr)
            GW = arcpy.CreateFeatureclass_management(featurePath, "GlobalWatershedTemp", "POLYGON", 
                                                     templateFeaturePath.format("GlobalWatershed"), "SAME_AS_TEMPLATE", "SAME_AS_TEMPLATE",sr)
            
            xmlPath = self.__SSXMLPath__("StreamStats{0}.xml".format(self.__regionID__), self.__TempLocation__, self.__TempLocation__)

            arcpy.CheckOutExtension("Spatial")
            self.__sm__("Starting Delineation")

            ArcHydroTools.StreamstatsGlobalWatershedDelineation(PourPoint, GW, GWP, xmlPath , "CLEARFEATURES_NO", self.WorkspaceID)
            self.__sm__(arcpy.GetMessages(),'AHMSG')

            #remove holes  
            self.__removePolygonHoles__(GW,featurePath)
            arcpy.CheckInExtension("Spatial")
            
            self.__sm__("Finished \n")
        except:
            tb = traceback.format_exc()
            self.error = tb
            self.__sm__("Delineation Error "+tb,"ERROR")

        finally:
            #Local cleanup
            del GWP
            del sr
            arcpy.Delete_management(PourPoint)
            arcpy.Delete_management(GW)
            arcpy.ResetEnvironments()
            arcpy.ClearEnvironment("workspace")
            #arcpy.Delete_management(self.__TempLocation__)
    #endregion  
      
    #region Helper Methods
    def __removePolygonHoles__(self, polyFC, path):
        try:
            
            result = arcpy.EliminatePolygonPart_management(polyFC, os.path.join(path,"GlobalWatershed"), "AREA_OR_PERCENT", "90 squaremeters", 1, "ANY")#modified CONTAINED_ONLY

            self.__sm__(arcpy.GetMessages())
            return result
        except:
            tb = traceback.format_exc()
            self.__sm__("Error removing holes "+tb,"ERROR")
            return polyFC       
    def __getDirectory__(self, subDirectory, makeTemp = True):
        try:
            if not os.path.exists(subDirectory): 
                os.makedirs(subDirectory);

            #temp dir
            if makeTemp and not os.path.exists(os.path.join(subDirectory, "Temp")):
                os.makedirs(os.path.join(subDirectory, "Temp"))
 

            return subDirectory
        except:
            x = arcpy.GetMessages()
            return subDirectory
    def __SSXMLPath__(self, xmlFileName, copyToDirectory="#", newTempWorkspace = "#"):
        file = None
        try:
            #move the file to tempDirectory
            xmlFile = self.__xmlPath__
            if copyToDirectory != "#":
                shutil.copy(xmlFile, copyToDirectory)
                xmlFile = os.path.join(copyToDirectory,xmlFileName)

            if newTempWorkspace == "#":
                return xmlFile

            #update tempworkspace
            xmlDoc = xml.dom.minidom.parse(xmlFile)
            archydroPath = os.path.join(self.__dataFolder__, 'archydro')
            bcLayersPath = os.path.join(self.__dataFolder__, 'bc_layers')
            xmlDoc.getElementsByTagName('RASTERDATAPATH')[0].firstChild.data = bcLayersPath
            xmlDoc.getElementsByTagName('VECTORDATAPATH')[0].firstChild.data = os.path.join(archydroPath,"global.gdb")
            xmlDoc.getElementsByTagName('DataPath')[0].firstChild.data = archydroPath
            xmlDoc.getElementsByTagName('GlobalDataPath')[0].firstChild.data = os.path.join(archydroPath,"global.gdb")

            file = open(xmlFile,"wb")
            xmlDoc.writexml(file)

            return xmlFile
        except:
             tb = traceback.format_exc()
             self.__sm__(tb,"ERROR")
        finally:
            if file != None and not file.closed: 
                file.close 
                file = None
    def __sm__(self, msg, type = 'INFO'):
        self.Message += type +':' + msg.replace('_',' ') + '_'

        if type in ('ERROR'): self.__logger__.error(msg)
        else : self.__logger__.info(msg)
    def _buildAHPourpoint(self, ppt, wkid):
            try:
                arcpy.env.workspace = self.__TempLocation__
                arcpy.env.overwriteOutput = False
                #get spatial ref
                wkid = int(wkid)
                sr = arcpy.SpatialReference(wkid)
                pourPoint = {"type":"Point","coordinates":json.loads(ppt)}
                arcpy.env.overwriteOutput = True
                FC = arcpy.CreateFeatureclass_management("in_memory", "FC", "POINT", spatial_reference=sr)
                #add required attributes
                t_fields = (  
                            ("Name", "TEXT"),
                            ("Descript", "TEXT"), 
                            ("SnapOn", "SHORT"),
                            ("SrcType", "SHORT"),
                            ("BATCHDONE", "SHORT") 
                            )
                for t_field in t_fields:  
                    arcpy.AddField_management(FC, *t_field)  

                # Create insert cursor for table
                fields = [i[0] for i in t_fields]
                fields.insert(0,'SHAPE@')

                with arcpy.da.InsertCursor(FC,fields) as cursor:
                    cursor.insertRow([arcpy.AsShape(pourPoint),"ags101","",None,None,None])
                #next cursor

                return FC
            except:
                tb = traceback.format_exc()
                print tb
    #endregion