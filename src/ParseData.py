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
    def __init__(self, stateFolder, regionID, workspaceID, xml, copy_archydro, copy_bc_layers, huc_folders, copy_global, direction): 
        self.RegionID = regionID
        self.isComplete = False
        self.Message =""
        self.__TempLocation__ = workspaceID

        if not os.path.exists(self.__TempLocation__): 
            os.makedirs(self.__TempLocation__)

        logdir = os.path.join(self.__TempLocation__, 'parse_{0}.log'.format(regionID))

        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler = logging.FileHandler(logdir)
        self.__logger__ = logging.getLogger('parse_{0}'.format(regionID))
        self.__logger__.setLevel(logging.INFO)
        self.__logger__.addHandler(handler)
        handler.setFormatter(formatter)
        
        self.__run__(xml, stateFolder, self.__TempLocation__, copy_archydro, copy_bc_layers, huc_folders, copy_global, direction)  
            
    #endregion  
         
    #Private Methods
    def __run__(self, xmlPath, stateFolder, tempLoc, copy_archydro, copy_bc_layers, huc_folders, copy_global, direction):
        try:
            state = self.RegionID
            self.__sm__('initialized')
            if xmlPath and state.upper() != "MO_STL":
                arcpy.AddMessage('Parsing xml')
                if direction == 'upload':
                    shutil.copy(xmlPath, tempLoc)
                    xmlPath = os.path.join(tempLoc, os.path.basename(xmlPath))
                try:
                    if state == 'HI':
                        self.__thinXML__(xmlPath, "StreamStatsConfig", 0,  {"ProgParams","TemplateView"}) #removes unnecessary nodes from xml
                    else:
                        self.__thinXML__(xmlPath, "StreamStatsConfig", 0, "ProgParams")
                except:
                    self.__thinXML__(xmlPath, "StreamStatsConfig" + state.upper(), 0, "ProgParams")
                self.__thinXML__(xmlPath, "ProgParams", 0, {"ApFunctions", "TempLocation"})
                self.__thinXML__(xmlPath, "ApFunctions", 0, {"GlobalPointDelineation", "WshParams"})
                self.__thinXML__(xmlPath, "ApFunction", 0, {"ApFields", "ApLayers", "DataPath", "GlobalDataPath", "SnapToleranceNumCells", "CleanupThresholdNumCells", "RASTERDATAPATH", "VECTORDATAPATH", "ParameterDelimiter", "NetworkName", "RelationshipName", "FromProjectionFileName", "GlobalParameter"})
                self.__thinXML__(xmlPath, "ApFunction", 1, {"ApFields", "ApLayers", "DataPath", "GlobalDataPath", "SnapToleranceNumCells", "CleanupThresholdNumCells", "RASTERDATAPATH", "VECTORDATAPATH", "ParameterDelimiter", "NetworkName", "RelationshipName", "FromProjectionFileName", "GlobalParameter"})
                self.__replaceProjection__(xmlPath)

                if stateFolder:
                    wshLayers = self.__getXMLLayers__(xmlPath, "wsh") #get layers necessary for basin characteristics
                    delinLayers = self.__getXMLLayers__(xmlPath, "delin") #get layers necessary for delineation
                    layers = wshLayers + delinLayers
                    if direction == 'upload':
                        stateFolder = self.__copydata__(stateFolder, tempLoc, copy_archydro, copy_bc_layers, huc_folders, copy_global)
                    if copy_archydro == 'true' or copy_bc_layers == 'true' or copy_global == 'true' or (huc_folders and huc_folders != ''):
                        arcpy.AddMessage('Parsing state data for: ' + state)
                        self.__deleteFiles__(stateFolder, layers) #uses layers taken from xml to delete unnecessary files
                    arcpy.ResetEnvironments()
                    arcpy.ClearEnvironment("workspace")

                    #self.__checkPixelDepth__(stateFolder)
            self.isComplete = True

            self.__sm__("finished \n")
        except:
            tb = traceback.format_exc() 
            self.__sm__("Error parsing data "+tb,"ERROR")
            arcpy.AddError("Error parsing data "+tb)
            print tb
            self.isComplete = False
        finally:
            arcpy.ResetEnvironments()
            arcpy.ClearEnvironment("workspace")
            self.__stateFolder__ = stateFolder
            del stateFolder
            self.__xmlPath__ = xmlPath

    def __copydata__(self, stateFolder, tempFolder, copy_archydro, copy_bc_layers, huc_folders, copy_global):
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
        if copy_global == 'true':
            loc = 'archydro/global.gdb'
            newFolder = os.path.join(newStFolder, loc)
            shutil.copytree(os.path.join(stateFolder, loc), newFolder)

        return newStFolder

    def __thinXML__(self, xmlfile, firstNode, i, attr):
        # remove unnecessary nodes from XML
        xmlDoc = xml.dom.minidom.parse(xmlfile)
        toRemove = []
        for child in xmlDoc.childNodes:
            if child.nodeType == 8:
                p = child.parentNode
                p.removeChild(child)
        for child in (child for child in xmlDoc.getElementsByTagName(firstNode)[i].childNodes if child.nodeType in [1,8]):
            if child.nodeType == 8 or child.getAttribute('TagName') == 'Flows' or (child.nodeName not in attr and (not child.getAttribute('Name') or child.getAttribute('Name') not in attr)):
                self.__sm__('Remove child: ' + child.nodeName)
                toRemove.extend((child,child.previousSibling))
        parent = xmlDoc.getElementsByTagName(firstNode)[i]
        for child in toRemove:
            parent.removeChild(child)
        file = open(xmlfile,"wb")
        xmlDoc.writexml(file)
    def __replaceProjection__(self, xmlfile):
        xmlDoc = ET.parse(xmlfile)
        xPath = ".//ApFunction[@TagName='WshParams']/ApFields/ApField"
        xPath2 = ".//ApFunction[@TagName='WshParams']/ApFields/ApField/ApLayer/ApFields/ApField"
        root = xmlDoc.getroot()
        for child in root.iter():
            if 'AdditionalParams' in child.attrib and 'PROJECTIONFILENAME' in child.attrib['AdditionalParams']:
                start_sect = child.attrib['AdditionalParams'].split('PROJECTIONFILENAME')[0]
                end_sect = child.attrib['AdditionalParams'].split('PROJECTIONFILENAME')[1].split('schemas')[1]
                self.__sm__('before: ' + child.attrib['AdditionalParams'])
                child.set('AdditionalParams', start_sect + 'PROJECTIONFILENAME=e:\projections' + end_sect)
                self.__sm__('after: ' + child.attrib['AdditionalParams'])
                
        file = open(xmlfile,"wb")
        xmlDoc.write(file)
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
        layers.extend(('global.gdb', 'schema', 'streamstats' + self.RegionID.lower(), self.RegionID.lower() + '_ss.gdb', 'readme', 'xml'))
        # adjusted for NY
        if os.path.basename(stateFolder).lower() == "ny": layers.extend(('cat', 'info'))
        seperator = ';'
        lays = seperator.join(layers)
        self.__sm__('Layers to Keep: ' + lays)

        for root, dirs, files in os.walk(stateFolder):
            for name in files: #remove unnecessary files
                filename = os.path.splitext(name)[0].lower()
                filePath = os.path.join(root,name)
                fileDir = os.path.basename(os.path.dirname(filePath)).lower()
                if name.endswith('.mdb') or name.endswith('.zip') or all([not filePath.endswith('.gdb'), not fileDir.endswith('.gdb'),name.lower() not in layers, fileDir not in layers,filename not in layers,filename[:-4] not in layers,os.path.dirname(filePath) != os.path.join(stateFolder, "bc_layers","info")]):
                    if os.path.dirname(filePath) not in fileDirs:
                        fileDirs.append(os.path.dirname(filePath))
                    self.__sm__('deleted file: ' + filePath)
                    os.remove(filePath)
            for d in (d for d in dirs if d.lower().endswith('.gdb')):
                parentDir = os.path.basename(os.path.dirname(os.path.join(root, d)))
                if parentDir == "archydro" and not d.lower() == "global.gdb": # remove old global.gdbs
                    try:
                        arcpy.Delete_management(os.path.join(root, d))
                        self.__sm__('deleted: ' + os.path.join(root, d))
                    except:
                        self.__sm__('could not remove ' + d)
                elif '_ss.gdb' not in d: # remove unnecessary feature classes from all gdbs
                    folderPath = os.path.join(root,d, 'Layers')
                    arcpy.env.workspace = folderPath
                    fclasses = arcpy.ListFeatureClasses()
                    if fclasses:
                        self.__sm__('feature classes in ' + d + ': ' + str(len(fclasses)))
                        for fc in (fc for fc in fclasses if fc.lower() not in layers):
                            try:
                                arcpy.Delete_management(fc)
                                self.__sm__('removed: ' + fc)
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
                            self.__sm__("Error converting raster to numpy: "+tb + '\n',"ERROR")

    def __sm__(self, msg, type = 'INFO'):
        self.Message += type +':' + msg.replace('_',' ') + '_'

        if type in ('ERROR'): self.__logger__.error(msg)
        else : self.__logger__.info(msg)
    #endregion