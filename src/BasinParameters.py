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
import shutil
import logging
import ArcHydroTools
import xml.dom.minidom
from arcpy import env
import xml.etree.ElementTree as ET
#endregion


##-------1---------2---------3---------4---------5---------6---------7---------8
##       BasinParameters
##-------+---------+---------+---------+---------+---------+---------+---------+

class BasinParameters(object):
    #region Constructor
    def __init__(self, regionID, workspaceID, pList, input_basin): 
        self.RegionID = regionID
        self.WorkspaceID = os.path.basename(workspaceID)
        self.isComplete = False
        self.Message =""    
        self.__MainDirectory__ = workspaceID
        self.__TempLocation__ = os.path.join(self.__MainDirectory__, "Temp")
        self.__xmlPath__ = os.path.join(self.__TempLocation__, "StreamStats"+regionID+".xml")
        self.ParameterList = None

        logdir = os.path.join(self.__TempLocation__, 'parameter.log')

        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler = logging.FileHandler(logdir)
        self.__logger__ = logging.getLogger('parameter')
        self.__logger__.setLevel(logging.INFO)
        self.__logger__.addHandler(handler)
        handler.setFormatter(formatter)
        
         #Test if workspace exists before run   
        if(not self.__workspaceValid__(os.path.join(self.__MainDirectory__, self.WorkspaceID+".gdb","Layers"))):
            return
        
        self.__run__(pList, input_basin)  
            
    #endregion  
         
    #Private Methods
    def __run__(self, parameters, input_basin):
        workspace = ''
        plist = None
        xmlfile =''
        try:
            # Set overwrite option
            arcpy.env.overwriteOutput = True
            arcpy.env.scratchWorkspace = self.__setScratchWorkspace__(os.path.join(self.__MainDirectory__, "Temp"))

            workspace = os.path.join(self.__MainDirectory__, self.WorkspaceID+".gdb","Layers")
            self.__sm__('workspace set: '+self.WorkspaceID)
            outputFile = os.path.join(self.__MainDirectory__, "Temp","parameterFile{0}")

            xmlfile = self.__SSXMLPath__("StreamStats{0}.xml".format(self.RegionID), self.__TempLocation__)
           
            if parameters == '':
                parametersList = self.__allParams__(xmlfile)
                seperator = ';'
                parameters = seperator.join(parametersList)
                self.__sm__('parameters list: ' + parameters)

            arcpy.CheckOutExtension("Spatial")
            self.__sm__("Started calc params")

            if input_basin != "none":
                if arcpy.Exists(input_basin):
                    ArcHydroTools.StreamstatsGlobalParametersServer(input_basin, os.path.join(workspace,"GlobalWatershedPoint"), 
                                                                parameters, outputFile.format(".xml"), outputFile.format(".htm"), 
                                                                xmlfile,"", self.WorkspaceID )
            else:
                ArcHydroTools.StreamstatsGlobalParametersServer(os.path.join(workspace,"GlobalWatershed"), 
                                                                os.path.join(workspace,"GlobalWatershedPoint"), 
                                                                parameters, outputFile.format(".xml"), outputFile.format(".htm"), 
                                                                xmlfile,"", self.WorkspaceID )

            self.__sm__(arcpy.GetMessages(),'AHMSG')
            arcpy.CheckInExtension("Spatial")
            self.__sm__("finished calc params")

            plist = self.__parseParameterXML__(outputFile.format(".xml"))
            if (len(plist) < 1):
                raise Exception("No parameters returned")
           
            self.ParameterList = plist
            self.isComplete = True

            self.__sm__("finished \n")
        except:
            tb = traceback.format_exc() 
            self.__sm__("Error calculating parameters "+tb,"ERROR")
            self.isComplete = False
        finally:
            arcpy.ResetEnvironments()
            arcpy.ClearEnvironment("workspace")
    def __allParams__(self, xmlfile):
        xmlParams = []

        xmlDoc = ET.parse(xmlfile)
        for apField in xmlDoc.findall(".//ApFunction[@TagName='WshParams']/ApFields[@TagName='ApFields']/ApField", xmlDoc):
            param = apField.get('AliasName')
            xmlParams.append(param.lower())
        return xmlParams
    def __parseParameterXML__(self, xmlfile):
        paramList = []
        try:
            self.__sm__("parsing xml")
            xmlDoc = xml.dom.minidom.parse(xmlfile)
            parameters = xmlDoc.getElementsByTagName("PARAMETER")         
            for param in parameters:
                code = param.getAttribute("name")
                value = param.getAttribute("value")
                paramList.append({"code":code,"value":value})
            #next param

            return paramList
        except:
             tb = traceback.format_exc()
             self.__sm__("Error reading parameters "+tb,"ERROR")
    def __getDirectory__(self, subDirectory):
            if os.path.exists(subDirectory): 
                shutil.rmtree(subDirectory)
            os.makedirs(subDirectory);

            return subDirectory
    def __workspaceValid__(self, workspace):
        if not arcpy.Exists(workspace):
            self.__sm__("Workspace " + workspace + " does not exist")
            return False

        if arcpy.TestSchemaLock(workspace):		
            self.__sm__("Workspace " + workspace + " has a schema lock","AHMSG")		
            return False
        self.__sm__("Workspace " + workspace + " is valid")
        return True
    def __setScratchWorkspace__(self, directory):
        if (arcpy.Exists(os.path.join(directory,"scratch.gdb"))):
            arcpy.Delete_management(os.path.join(directory,"scratch.gdb"))
        arcpy.CreateFileGDB_management(directory,'scratch.gdb')
        return os.path.join(directory,"scratch.gdb") 
    def __SSXMLPath__(self, xmlFileName, newTempWorkspace = "#"):
        file = None
        xmlFile =''
        try:
            #return self.__SSXMLPath__("StreamStats{0}.xml".format(self.RegionID),'#',self.__TempLocation__)
            #move the file to tempDirectory
            if os.path.exists(os.path.join(self.__TempLocation__, xmlFileName)):
                xmlFile = os.path.join(self.__TempLocation__, xmlFileName)
                self.__sm__("Using existing xmlFile "+xmlFile);
            else:
            #default location
                xmlFile = os.path.join(self.__xmlPath__,xmlFileName)  
                self.__sm__("Using default xmlFile "+xmlFile);          
                shutil.copy(xmlFile, self.__TempLocation__)
                xmlFile = os.path.join(self.__TempLocation__,xmlFileName)
                self.__sm__("moved default xmlFile to temp "+xmlFile);  
                if newTempWorkspace == "#":
                    return xmlFile

                #update tempworkspace
                xmlDoc = xml.dom.minidom.parse(xmlFile)
                archydroPath = os.path.join(self.__dataFolder__, 'archydro')
                bcLayersPath = os.path.join(self.__dataFolder__, 'bc_layers')
                xmlDoc.getElementsByTagName('RASTERDATAPATH')[0].firstChild.data = bcLayersPath
                xmlDoc.getElementsByTagName('VECTORDATAPATH')[0].firstChild.data = bcLayersPath
                xmlDoc.getElementsByTagName('RasterLocation')[0].firstChild.data = archydroPath
                xmlDoc.getElementsByTagName('VectorLocation')[0].firstChild.data = os.path.join(archydroPath,"global.gdb")
                xmlDoc.getElementsByTagName('RasterLocation')[1].firstChild.data = archydroPath
                xmlDoc.getElementsByTagName('VectorLocation')[1].firstChild.data = os.path.join(archydroPath,"global.gdb")
                xmlDoc.getElementsByTagName('DataPath')[0].firstChild.data = archydroPath
                xmlDoc.getElementsByTagName('GlobalDataPath')[0].firstChild.data = os.path.join(archydroPath,"global.gdb")
                
                xmlDoc.getElementsByTagName('TempLocation')[0].firstChild.data = newTempWorkspace
                file = open(xmlFile,"wb")
                xmlDoc.writexml(file)
                self.__sm__("renamed temp location");  

            return xmlFile
        except:
             tb = traceback.format_exc()
             self.__sm__(tb,"ERROR")
             return os.path.join(self.__xmlPath__,xmlFileName)
        finally:
            if file != None and not file.closed: 
                file.close 
                file = None
    def __sm__(self, msg, type = 'INFO'):
        self.Message += type +':' + msg.replace('_',' ') + '_'

        if type in ('ERROR'): self.__logger__.error(msg)
        else : self.__logger__.info(msg)
    #endregion