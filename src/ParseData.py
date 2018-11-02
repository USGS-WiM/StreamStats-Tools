#------------------------------------------------------------------------------
#----- ParseData.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2018 WIM - USGS

#    authors:  Katrin Jacobsen USGS Web Informatics and Mapping
# 
#   purpose:  Parse through SS regional data using information in SS XML

#region "Comments"
#endregion

#region "Imports"
import traceback
import os
import arcpy
import logging
import xml.dom.minidom
import xml.etree.ElementTree as ET
import numpy
import shutil
#endregion


##-------1---------2---------3---------4---------5---------6---------7---------8
##       ParseData
##-------+---------+---------+---------+---------+---------+---------+---------+

class Main(object):
    #region Constructor
    def __init__(self, stateFolder, regionID, workspaceID, xml, copy_archydro, copy_bc_layers, huc_folders): 
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
        
        self.__run__(xml, stateFolder, self.__TempLocation__, copy_archydro, copy_bc_layers, huc_folders)  
            
    #endregion  
         
    #Private Methods
    def __run__(self, xmlPath, stateFolder, tempLoc, copy_archydro, copy_bc_layers, huc_folders):
        try:
            self.__sm__('initialized')
            if xmlPath:
                arcpy.AddMessage('Parsing xml')
                shutil.copy(xmlPath, tempLoc)
                xmlPath = os.path.join(tempLoc, os.path.basename(xmlPath))
                self.__thinXML__(xmlPath, "StreamStatsConfig", 0, "ProgParams") #removes unnecessary nodes from xml
                self.__thinXML__(xmlPath, "ProgParams", 0, "ApFunctions")
                self.__thinXML__(xmlPath, "ApFunctions", 0, {"GlobalPointDelineation", "WshParams"})
                self.__thinXML__(xmlPath, "ApFunction", 0, {"ApFields", "ApLayers", "DataPath", "GlobalDataPath", "SnapToleranceNumCells", "RASTERDATAPATH", "VECTORDATAPATH"})
                self.__thinXML__(xmlPath, "ApFunction", 1, {"ApFields", "ApLayers", "DataPath", "GlobalDataPath", "SnapToleranceNumCells", "RASTERDATAPATH", "VECTORDATAPATH"})

                if stateFolder:
                    arcpy.AddMessage('Parsing state data')
                    wshLayers = self.__getXMLLayers__(xmlPath, "wsh") #get layers necessary for basin characteristics
                    delinLayers = self.__getXMLLayers__(xmlPath, "delin") #get layers necessary for delineation
                    layers = wshLayers + delinLayers
                    seperator = ';'
                    lays = seperator.join(layers)

                    stateFolder = self.__copydata__(stateFolder, tempLoc, copy_archydro, copy_bc_layers, huc_folders)

                    self.__deleteFiles__(stateFolder, layers) #uses layers taken from xml to delete unnecessary files
                    arcpy.ResetEnvironments()
                    arcpy.ClearEnvironment("workspace")

                    #self.__checkPixelDepth__(stateFolder)
            self.isComplete = True

            self.__sm__("finished \n")
        except:
            tb = traceback.format_exc() 
            self.__sm__("Error parsing data "+tb,"ERROR")
            print tb
            self.isComplete = False
        finally:
            arcpy.ResetEnvironments()
            arcpy.ClearEnvironment("workspace")
            self.__stateFolder__ = stateFolder
            del stateFolder
            self.__xmlPath__ = xmlPath

    def __copydata__(self, stateFolder, tempFolder, copy_archydro, copy_bc_layers, huc_folders):
        # copy state data to temp workspace before cleaning up data
        self.__sm__('Copying data folders')
        state = os.path.basename(stateFolder)
        newStFolder = os.path.join(tempFolder, state)
        if copy_archydro == 'true':
            newFolder = os.path.join(newStFolder, 'archydro')
            shutil.copytree(os.path.join(stateFolder, 'archydro'), newFolder)
        if copy_bc_layers == 'true':
            newFolder = os.path.join(newStFolder, 'bc_layers')
            shutil.copytree(os.path.join(stateFolder, 'bc_layers'), newFolder)
        if huc_folders:
            huc_folders = huc_folders.split(';')
            for huc in huc_folders:
                if '/' in huc: huc = os.path.basename(huc)
                loc = 'archydro/' + os.path.basename(huc)
                newFolder = os.path.join(newStFolder, loc)
                shutil.copytree(os.path.join(stateFolder, loc), newFolder)

        return newStFolder

    def __thinXML__(self, xmlfile, firstNode, i, attr):
        # remove unnecessary nodes from XML
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
    def __getXMLLayers__(self, xmlfile, layerType):
        # parse XML to get layers needed for basin delineation (delin) and characteristics (wsh)
        layers = []
        xmlDoc = ET.parse(xmlfile)
        if layerType == "wsh": xPath = ".//ApFunction[@TagName='WshParams']/ApFields/ApField/ApLayers/ApLayer"
        if layerType == "delin": xPath = ".//ApFunction[@TagName='GlobalPointDelineation']/ApLayers/ApLayer"
        for apLayer in xmlDoc.findall(xPath, xmlDoc):
            if apLayer.get('AliasName') and apLayer.get('AliasName') not in layers:
                layer = apLayer.get('AliasName').lower()
                layers.append(layer)
            if apLayer.get('Name') and apLayer.get('Name') not in layers:
                layer = apLayer.get('Name').lower()
                layers.append(layer)
        return layers
    def __deleteFiles__(self, stateFolder, layers):
        # remove unnecessary files from state folders using layer names parsed from xml
        fileDirs = []
        layers.extend(('global.gdb', 'schema', 'streamstats' + self.RegionID, self.RegionID + '_ss.gdb', 'readme'))
        seperator = ';'
        lays = seperator.join(layers)
        self.__sm__('Layers to Keep: ' + lays)

        for root, dirs, files in os.walk(stateFolder):
            for name in files: #remove unnecessary files
                filename = os.path.splitext(name)[0].lower()
                filePath = os.path.join(root,name)
                fileDir = os.path.basename(os.path.dirname(filePath)).lower()
                if all([not filePath.endswith('.gdb'), not fileDir.endswith('.gdb'),fileDir not in layers,filename not in layers,filename[:-4] not in layers,os.path.dirname(filePath) != os.path.join(stateFolder, "bc_layers","info")]) or name.endswith('.mdb'):
                    if os.path.dirname(filePath) not in fileDirs:
                        fileDirs.append(os.path.dirname(filePath))
                    os.remove(filePath)
            for d in (d for d in dirs if d.lower().endswith('.gdb')):
                upperDir = os.path.basename(os.path.dirname(os.path.join(root, d)))
                if upperDir == "archydro" and not d.lower() == "global.gdb": # remove old global.gdbs
                    try:
                        arcpy.Delete_management(os.path.join(root, d))
                        self.__sm__('deleted: ' + os.path.join(root, d))
                    except:
                        self.__sm__('could not remove ' + d)
                else: # remove unnecessary feature classes from all gdbs
                    folderPath = os.path.join(root,d, 'Layers')
                    arcpy.env.workspace = folderPath
                    fclasses = arcpy.ListFeatureClasses()
                    if fclasses:
                        self.__sm__('feature classes in ' + d + ': ' + str(len(fclasses)))
                        for fc in (fc for fc in fclasses if fc.lower() not in layers):
                            try:
                                arcpy.Delete_management(fc)
                                self.__sm__('removing: ' + fc)
                            except:
                                self.__sm__('Could not remove ' + fc)
                    if not fclasses or len(arcpy.ListFeatureClasses()) == 0:
                        self.__sm__('Deleted .gdb: ' + folderPath)
                        arcpy.Delete_management(folderPath)
            
        for dirs in fileDirs: # delete folders that had files deleted if they are now empty
            if [f for f in os.listdir(dirs)] == []:
                self.__sm__('Deleted directory: ' + dirs)
                os.rmdir(dirs)
    def __checkPixelDepth__(self, dataFolder):
        # read pixel depth and raster max values, for use later if we decide to change the pixel depths
        self.__sm__('checking raster size')
        for root, dirs, files in os.walk(dataFolder):
            for d in [d for d in dirs if d == "bc_layers" or d[0].isdigit()]:
                arcpy.env.workspace = os.path.join(root, d)
                rasterList = arcpy.ListRasters()
                if len(rasterList) > 0:
                    self.__sm__('directory: ' + os.path.join(root, d))
                    self.__sm__('raster list: ' + str(len(rasterList)))

                    for r in rasterList:
                        try:
                            arr = arcpy.RasterToNumPyArray(r, nodata_to_value=-9999)
                            self.__sm__('max value for ' + r + ': ' + str(numpy.amax(arr)))
                            self.__sm__('array dtype: ' + str(arr.dtype))
                        except:
                            tb = traceback.format_exc() 
                            self.__sm__("Error  converting raster to numpy: "+tb + '\n',"ERROR")

    def __sm__(self, msg, type = 'INFO'):
        self.Message += type +':' + msg.replace('_',' ') + '_'

        if type in ('ERROR'): self.__logger__.error(msg)
        else : self.__logger__.info(msg)
    #endregion