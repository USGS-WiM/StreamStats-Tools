import sys, os, subprocess, fnmatch, traceback

import datetime, arcpy, json, logging, pythonaddins
from arcpy import env
from arcpy.sa import *
from Delineation import Delineation as Delineation
from BasinParameters import BasinParameters as BasinParameters
from UpdateS3 import Main as UpdateS3
import time
import json

class Toolbox(object):
    def __init__(self):
        self.label =  "StreamStats Data Tools"
        self.alias  = "ss-tools"

        # List of tool classes associated with this toolbox
        self.tools = [basinDelin, basinParams, updateS3Bucket] 

class updateS3Bucket(object):
    def __init__(self):
        self.label       = "Update S3 Bucket"
        self.description = ""

    def getParameterInfo(self):
        #Define parameter definitions

        log_Note = arcpy.Parameter(
            displayName = "Describe changes made in this update (limit 50 chars)",
            name = "log_Note",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        access_key_id = arcpy.Parameter(
            displayName="Your AWS Access Key ID",
            name="access_key_id",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        access_key = arcpy.Parameter(
            displayName="Your AWS Secret Access Key",
            name="access_key",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        editor_name = arcpy.Parameter(
            displayName="Your name",
            name="editor_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        
        workspace = arcpy.Parameter(
            displayName = "Temporary Workspace",
            name="workspace",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input"
        )
        
        state_folder = arcpy.Parameter(
            displayName="Select input state/region folder",
            name="state_folder",
            datatype="DEFolder",
            parameterType="Optional",
            direction="Input")

        xml_file = arcpy.Parameter(
            displayName="Select xml file",
            name="xml_files",
            datatype="DEFile",
            parameterType="Optional",
            direction="Input")
        
        copy_bc_layers = arcpy.Parameter(
            displayName="Copy 'bc_layers' folder",
            name="copy_data_bc_layers",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")

        copy_archydro = arcpy.Parameter(
            displayName="Copy entire 'archydro' folder",
            name="copy_data_archydro",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")

        copy_global = arcpy.Parameter(
            displayName="Copy 'global.gdb' folder",
            name="copy_global",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        
        huc_folders = arcpy.Parameter(
            displayName="Input huc folders",
            name="huc_folders",
            datatype=["DEFolder", "GPString"],
            parameterType="Optional",
            direction="Input",
            multiValue="True")

        schema_file = arcpy.Parameter(
            displayName="Select schema FGDB file",
            name="schema_files",
            datatype="DEType",
            parameterType="Optional",
            direction="Input")
        
        parameters = [log_Note, access_key_id, access_key, editor_name, workspace, state_folder, xml_file, copy_bc_layers, copy_archydro, copy_global, huc_folders, schema_file]
    
        return parameters

    def isLicensed(self): #optional
        return True

    def updateParameters(self, parameters): #optional

        #if we have an input folder, set checkboxes true
        # if parameters[0].valueAsText:
        #     if parameters[0].altered:
        #         parameters[1].value = "True"
        #         parameters[2].value = "True"

        return

    def updateMessages(self, parameters):
        if parameters[1].altered:
            logNote = parameters[1].valueAsText
            if len(logNote) > 50:
                pythonaddins.MessageBox('Note cannot exceed 50 characters', 'WARNING', 0)
        if not parameters[10].altered:
            parameters[10].value = ''
        if parameters[9].value == True or parameters[10].valueAsText:
            parameters[8].value = False
        return

    def execute(self, parameters, messages):

        messages.addMessage('Running script to update S3')

        updates3 = UpdateS3(parameters) 

class basinDelin(object):
    # region Constructor
    def __init__(self):
        self.label = "Basin Delineation"
        self.description = ""

    def getParameterInfo(self):
        # Define parameter definitions

        state_folder = arcpy.Parameter(
            displayName="Select input state/region folder",
            name="state_folder",
            datatype="DEType",
            parameterType="Required",
            direction="Input")

        schema_file = arcpy.Parameter(
            displayName="Regional schema FGDB file",
            name="schema_file",
            datatype="DEType",
            parameterType="Required",
            direction="Input")
        
        xml_file = arcpy.Parameter(
            displayName="Regional xml",
            name="xml_file",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")

        workspaceID = arcpy.Parameter(
            displayName="Workspace folder",
            name="workspaceID",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")

        pourpoint = arcpy.Parameter(
            displayName="Pour point",
            name="pourpoint",
            datatype="GPString",
            parameterType="Required",
            direction="Input")


        basin_params = arcpy.Parameter(
            displayName="Calculate All Basin Characteristics",
            name="basin_params",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        
        parameters_list = arcpy.Parameter(
            displayName="Characteristics",
            name="parameters_list",
            datatype="GPString",
            parameterType="Optional",
            direction="Input")


        parameters = [state_folder, schema_file, xml_file, workspaceID, pourpoint, basin_params, parameters_list] 
        return parameters
    
    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        if not parameters[5].altered:
            parameters[5].value = '4326'
        parameters[0].value = r'C:\Users\kjacobsen\Documents\wim_projects\ss_data\vt'
        parameters[1].value = r'C:\Users\kjacobsen\Documents\wim_projects\ss_data\vt\VT_ss.gdb'
        parameters[2].value = r'C:\Users\kjacobsen\Documents\wim_projects\ss_data\vt\StreamStatsVT.xml'
        parameters[3].value = r'C:\Users\kjacobsen\Documents\wim_projects\ss_data\workspaces\delin_vt1'
        parameters[4].value = '[-73.15191, 43.08356]'
        return

    def UpdateMessages(self, parameters):
        return

    def execute(self, parameters, messages): 
        state_folder    = parameters[0].valueAsText
        schema_file     = parameters[1].valueAsText
        xml_file        = parameters[2].valueAsText
        workspaceID     = parameters[3].valueAsText
        pourpoint       = parameters[4].valueAsText
        basin_params    = parameters[5].valueAsText
        parameters_list = parameters[6].valueAsText

        arcpy.env.overwriteOutput = True

        stabbr = os.path.basename(state_folder)

        workspace_name = os.path.basename(workspaceID)
        GW_location = os.path.join(workspaceID, workspace_name + '.gdb', 'Layers')
        GW_file = os.path.join(GW_location, 'GlobalWatershed')
        GWP_file = os.path.join(GW_location, 'GlobalWatershedPoint')
        
        def validatePourPoint(ppoint):
            """validatePourPoint(ppoint=None)
                Determines if input pourpoint is a valid json point
            """

            if ppoint.startswith('[') and ppoint.endswith(']'):
                messages.addMessage('Found a valid pourpoint: ' + ppoint)
                return ppoint
            else:
                messages.addErrorMessage('You did not select a valid pourpoint.  Make sure it is contained within square brackets.')
                sys.exit()

        def validateXML(xml):
            """validateStreamStatsXML(xml=None)
                Determines if input xml is a valid streamstats XML file
            """

            #get filename
            filename = xml.replace('\\','/').split('/')[-1]
            #validate xml file
            if fnmatch.fnmatch(filename, 'StreamStats*.xml'):
                messages.addMessage('Found a valid .xml file: ' + filename)
                return xml
            else:
                messages.addErrorMessage('You did not select a valid xml file: ' + filename)
                sys.exit()
                
        def validateSchema(item):
            """validateSchema(item=None)
                Determines if input schema is either a valid .prj file or a valid file geodatabse
            """

            filename = item.replace('\\','/').split('/')[-1]

            #validate prj file
            if os.path.isfile(item) and fnmatch.fnmatch(filename, '*.prj'):
                try:
                    arcpy.SpatialReference(filename)
                except:
                    messages.addErrorMessage('You did not select a valid prj file: ' + filename)
                else:
                    messages.addMessage('Found a valid .prj file: ' + filename)
                    return item

            #validate file gdb
            elif os.path.isdir(item) and filename.find('gdb'):
                try:
                    desc = arcpy.Describe(item)
                    if desc.dataType == 'Workspace':
                        messages.addMessage('Found a valid file geodatabase: ' + filename + ', item: ' + item )
                        return item
                except:
                    messages.addErrorMessage('You did not select a valid file geodatabase: ' + filename)

            else:
                messages.addErrorMessage('You did not select a valid schema: ' + item)
                sys.exit()

        wsID = os.path.basename(workspaceID)
        wshed = os.path.join(workspaceID, wsID + '.gdb', 'Layers', 'GlobalWatershed')
        if arcpy.Exists(wshed):
            val = pythonaddins.MessageBox(wshed + ' already exists.  Would you like to overwrite it?', 'Warning', 4)
            if val == 'No':
                messages.addWarningMessage('Script cancelled due to existing watershed.')
                sys.exit()
        
        messages.addMessage('Delineating Basin')
        schemaCheck = validateSchema(schema_file)
        xmlCheck = validateXML(xml_file)
        ppoint = validatePourPoint(pourpoint)
        try:
            ssdel = Delineation(stabbr, schemaCheck, xmlCheck, workspaceID, state_folder)
            ppoint = ssdel._buildAHPourpoint(ppoint, '4326')
            ssdel.Delineate(ppoint)

        except:
            tb = traceback.format_exc()
            messages.addErrorMessage(tb)

        if arcpy.Exists(GW_file):
            messages.addMessage('Placing on Map')
            mxd = arcpy.mapping.MapDocument("CURRENT")
            df = arcpy.mapping.ListDataFrames(mxd, "*")[0]
            newlayer = arcpy.mapping.Layer(GW_file)
            arcpy.mapping.AddLayer(df, newlayer,"BOTTOM")
        

            if basin_params == "true" or parameters_list:
                if not parameters_list:
                    parameters_list = ''
                try:
                    messages.addMessage('Calculating Basin Characteristics')
                    ssBp = BasinParameters(stabbr, workspaceID, parameters_list, "none")
                
                    if ssBp.isComplete:
                        params = []
                        for parameter in ssBp.ParameterList:
                            params.append(parameter['code'])
                        messages.addMessage("Parameters: " + (',').join(params))
                except:
                    tb = traceback.format_exc()
                    messages.addErrorMessage(tb)

        else:
            if ssdel.error != "":
                messages.addErrorMessage('Delineation Error ' + ssdel.error)
            if "no cataloguing unit" in ssdel.error:
                messages.addErrorMessage('Delination Failed. Please make sure the point is in the given region.  If delineation still fails, try again in another map document or ArcMap session.')
        arcMessages = arcpy.GetMessages()
        if arcMessages.find('ERROR') > -1 or arcMessages.find('Failed') > -1:
            messages.addGPMessages()
        arcpy.ResetEnvironments()
        arcpy.ClearEnvironment("workspace")
        
class basinParams(object):
    # region Constructor
    def __init__(self):
        self.label = "Calculate Basin Characteristics"
        self.description = ""

    def getParameterInfo(self):
        # Define parameter definitions

        state_folder = arcpy.Parameter(
            displayName="Select input state/region folder",
            name="state_folder",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")
        
        parameters_list = arcpy.Parameter(
            displayName="Characteristics",
            name="parameters_list",
            datatype="GPString",
            parameterType="Optional",
            direction="Input")
        
        input_basin = arcpy.Parameter(
            displayName="Input Watershed",
            name="input_basin",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Input")

        parameters = [state_folder, parameters_list, input_basin] 
        return parameters
    
    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def UpdateMessages(self, parameters):
        return

    def execute(self, parameters, messages): 
        state_folder    = parameters[0].valueAsText
        parameters_list = parameters[1].valueAsText
        input_basin     = parameters[2].valueAsText

        arcpy.env.overwriteOutput = True

        if not parameters_list:
            parameters_list = ''
        workspace_gdb_name = os.path.dirname(os.path.dirname(input_basin))
        workspaceID = os.path.dirname(workspace_gdb_name)

        stabbr = os.path.basename(state_folder)

        tempDir = os.path.join(workspaceID, 'Temp')

        try:
            messages.addMessage('Calculating Basin Characteristics')
            ssBp = BasinParameters(stabbr, workspaceID, parameters_list, input_basin)


            if ssBp.isComplete:
                params = []
                for parameter in ssBp.ParameterList:
                    params.append(parameter['code'])
                messages.addMessage("Parameters: " + (',').join(params))

        except:
            tb = traceback.format_exc()
            messages.addErrorMessage(tb)
        finally:
            arcMessages = arcpy.GetMessages()
            if arcMessages.find('ERROR') > -1 or arcMessages.find('Failed') > -1:
                messages.addGPMessages()
            elif arcMessages.find('Raster not found') > -1:
                messages.addWarningMessage('Raster not found for one or more characteristics.  Please make sure the data for each characteristic is in the "bc_layers" folder.')
            elif arcMessages.find('Cataloging Unit') > -1:
                messages.addGPMessages()
                messages.addErrorMessage('Please make sure the basin is in the given region.  If computation still fails, try again in another map document or ArcMap session.')
            arcpy.ResetEnvironments()
            arcpy.ClearEnvironment("workspace")
