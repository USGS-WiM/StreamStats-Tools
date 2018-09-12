#------------------------------------------------------------------------------
#----- BasinParameters.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2014 WiM - USGS

#    authors:  Jeremy K. Newson USGS Wisconsin Internet Mapping
# 
#   purpose:  Calculates ss parameters for a given watershed
#          
#discussion:  Adapted from John Guthrie's GetBC7.py basin characteristics script
#

#region "Comments"
#11.12.2014 jkn - Created
#endregion

#region "Imports"
import traceback
import os
import arcpy
import logging
import xml.dom.minidom
import xml.etree.ElementTree as ET
#endregion


##-------1---------2---------3---------4---------5---------6---------7---------8
##       BasinParameters
##-------+---------+---------+---------+---------+---------+---------+---------+

class ParseXML(object):
    #region Constructor
    def __init__(self, stateFolder, regionID, workspaceID, xml): 
        self.RegionID = regionID
        self.isComplete = False
        self.Message =""
        self.__TempLocation__ = os.path.join(workspaceID, "Temp")

        if not os.path.exists(self.__TempLocation__): 
            os.makedirs(self.__TempLocation__)

        logdir = os.path.join(self.__TempLocation__, 'parseXML.log')

        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler = logging.FileHandler(logdir)
        self.__logger__ = logging.getLogger('parseXML')
        self.__logger__.setLevel(logging.INFO)
        self.__logger__.addHandler(handler)
        handler.setFormatter(formatter)
        
        self.__run__(xml, stateFolder)  
            
    #endregion  
         
    #Private Methods
    def __run__(self, xmlPath, stateFolder):
        try:
            self.__sm__('initialized')
            self.__thinXML__(xmlPath, "StreamStatsConfig", 0, "ProgParams") #removes unnecessary nodes from xml
            self.__thinXML__(xmlPath, "ProgParams", 0, "ApFunctions")
            self.__thinXML__(xmlPath, "ApFunctions", 0, {"GlobalPointDelineation", "WshParams"})
            self.__thinXML__(xmlPath, "ApFunction", 0, {"ApFields", "ApLayers", "DataPath", "GlobalDataPath", "SnapToleranceNumCells", "RASTERDATAPATH", "VECTORDATAPATH"})
            self.__thinXML__(xmlPath, "ApFunction", 1, {"ApFields", "ApLayers", "DataPath", "GlobalDataPath", "SnapToleranceNumCells", "RASTERDATAPATH", "VECTORDATAPATH"})
            wshLayers = self.__getWshLayers__(xmlPath) #get layers necessary for basin characteristics
            delinLayers = self.__getDelinLayers__(xmlPath) #get layers necessary for delineation
            layers = wshLayers + delinLayers

            self.__deleteFiles__(stateFolder, layers) #uses layers taken from xml to delete unnecessary files
            self.isComplete = True

            self.__sm__("finished \n")
        except:
            tb = traceback.format_exc() 
            self.__sm__("Error calculating parameters "+tb,"ERROR")
            self.isComplete = False
    def __thinXML__(self, xmlfile, firstNode, i, attr):
        xmlDoc = xml.dom.minidom.parse(xmlfile)
        toRemove = []
        for child in (child for child in xmlDoc.getElementsByTagName(firstNode)[i].childNodes if child.nodeType == 1):
            if child.nodeName not in attr and (not child.getAttribute('Name') or child.getAttribute('Name') not in attr):
                self.__sm__('Remove child: ' + child.nodeName)
                toRemove.extend((child,child.previousSibling))
        parent = xmlDoc.getElementsByTagName(firstNode)[i]
        for child in toRemove:
            parent.removeChild(child)
        file = open(xmlfile,"wb")
        xmlDoc.writexml(file)
    def __getWshLayers__(self, xmlfile):
        wshLayers = []
        xmlDoc = ET.parse(xmlfile)
        for apLayer in xmlDoc.findall(".//ApFunction[@TagName='WshParams']/ApFields/ApField/ApLayers/ApLayer", xmlDoc):
            layerName = apLayer.get('Name').lower()
            layerAlName = apLayer.get('AliasName').lower()
            wshLayers.extend((layerName,layerAlName))
        return wshLayers
    def __getDelinLayers__(self, xmlfile):
        delinLayers = []
        xmlDoc = ET.parse(xmlfile)
        for apLayer in xmlDoc.findall(".//ApFunction[@TagName='GlobalPointDelineation']/ApLayers/ApLayer", xmlDoc):
            layer = apLayer.get('AliasName').lower()
            delinLayers.append(layer)
        return delinLayers
    def __deleteFiles__(self, stateFolder, layers):
        fileDirs = []
        layers.extend(('global.gdb', 'schema', 'streamstats' + self.RegionID, self.RegionID + '_ss.gdb', 'readme'))
        seperator = ';'
        lays = seperator.join(layers)
        self.__sm__('Layers to Keep: ' + lays)

        for root, dirs, files in os.walk(stateFolder):
            for name in files:
                filename = os.path.splitext(name)[0].lower()
                filePath = os.path.join(root,name)
                fileDir = os.path.basename(os.path.dirname(filePath)).lower()
                if all([not fileDir.startswith('0'),fileDir not in layers,filename not in layers,not filename.endswith(".aux"),os.path.dirname(filePath) != os.path.join(stateFolder, "bc_layers","info")]) or name.endswith('.mdb'):
                    #bc_layers\info folder inputs raster tables, affects basin chars for some regions if not there
                    if os.path.dirname(filePath) not in fileDirs:
                        fileDirs.append(os.path.dirname(filePath))
                    os.remove(filePath)
            for folder in dirs:
                if folder.lower().endswith('.gdb'):
                    folderPath = os.path.join(root,folder)
                    arcpy.env.workspace = folderPath
                    if [f for f in arcpy.ListDatasets()] == [] and [f for f in arcpy.ListFiles()] == []:
                        self.__sm__('Deleted .gdb: ' + folderPath)
                        arcpy.Delete_management(folderPath)
                        if [f for f in os.path.dirname(folderPath)] == []:
                            os.rmdir(os.path.dirname(folderPath))
        for dirs in fileDirs:
            if [f for f in os.listdir(dirs)] == []:
                self.__sm__('Deleted directory: ' + dirs)
                os.rmdir(dirs)

    def __sm__(self, msg, type = 'INFO'):
        self.Message += type +':' + msg.replace('_',' ') + '_'

        if type in ('ERROR'): self.__logger__.error(msg)
        else : self.__logger__.info(msg)
    #endregion