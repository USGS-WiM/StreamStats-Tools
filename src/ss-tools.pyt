import sys, os, subprocess, fnmatch, traceback

import datetime, arcpy, json, logging, pythonaddins
from arcpy import env
from arcpy.sa import *
from Delineation import Delineation as Delineation
from BasinParameters import BasinParameters as BasinParameters
import time
import json

class Toolbox(object):
    def __init__(self):
        self.label =  "StreamStats Data Tools"
        self.alias  = "ss-tools"

        # List of tool classes associated with this toolbox
        self.tools = [updateS3Bucket, basinDelin, basinParams] 

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
        
        state_folder = arcpy.Parameter(
            displayName="Select input state/region folder",
            name="state_folder",
            datatype="DEFolder",
            parameterType="Required",
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

        xml_file = arcpy.Parameter(
            displayName="Select xml file",
            name="xml_files",
            datatype="DEFile",
            parameterType="Optional",
            direction="Input")

        schema_file = arcpy.Parameter(
            displayName="Select schema FGDB file",
            name="schema_files",
            datatype="DEType",
            parameterType="Optional",
            direction="Input")
        
        parameters = [log_Note, access_key_id, access_key, editor_name, state_folder, copy_bc_layers, copy_archydro, copy_global, huc_folders, xml_file, schema_file]
    
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

    def updateMessages(self, parameters): #optional
        if parameters[1].altered:
            logNote = parameters[1].valueAsText
            if len(logNote) > 50:
                pythonaddins.MessageBox('Note cannot exceed 50 characters', 'WARNING', 0)
        if not parameters[8].altered:
            parameters[8].value = ''
        if parameters[7].value == True or parameters[8].valueAsText:
            parameters[6].value = False
        return

    def execute(self, parameters, messages):
        logNote        = parameters[0].valueAsText
        accessKeyID    = parameters[1].valueAsText
        accessKey      = parameters[2].valueAsText
        editorName     = parameters[3].valueAsText
        state_folder   = parameters[4].valueAsText
        copy_bc_layers = parameters[5].valueAsText
        copy_archydro  = parameters[6].valueAsText
        copy_global    = parameters[7].valueAsText
        huc_folders    = parameters[8].valueAsText
        xml_file       = parameters[9].valueAsText
        schema_file    = parameters[10].valueAsText
        
        arcpy.env.overwriteOutput = True

        def configureAWSKeyID(AWSKeyID):
            """configureAWSKeyID(AWSKeyID=None)
                Function to configure AWS Access Key ID
            """

            #create AWS CLI command
            cmd="aws configure set aws_access_key_id " + AWSKeyID


            try:
                output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                #messages.addErrorMessage('Configure not successful.  Please make sure you have inserted the correct credentials.')
                messages.addErrorMessage(e.output)
                tb = traceback.format_exc()
                messages.addErrorMessage(tb)
                sys.exit()
            else:
                messages.addMessage('Finished configuring AWS Key ID')
        def configureAWSKey(AWSAccessKey):
            """configureAWSKey(AWSAccessKey=None)
                Function to configure AWS CLI Access Key
            """

            #create AWS CLI command
            cmd="aws configure set aws_secret_access_key " + AWSAccessKey


            try:
                output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                #messages.addErrorMessage('Configure not successful.  Please make sure you have entered the correct credentials.')
                messages.addErrorMessage(e.output)
                tb = traceback.format_exc()
                messages.addErrorMessage(tb)
                sys.exit()
            else:
                messages.addMessage('Finished configuring AWS Secret Access Key')
        def validateStreamStatsXML(xml):
            """validateStreamStatsXML(xml=None)
                Determines if input xml is a valid streamstats XML file
            """

            #get filename
            filename = xml.replace('\\','/').split('/')[-1]

            #validate xml file
            stateabbr = filename.split('.xml')[0].split('StreamStats')[1].upper()
            if fnmatch.fnmatch(filename, 'StreamStats*.xml') and stateabbr in states:
                return True
            else:
                messages.addErrorMessage('You did not select a valid xml file: ' + filename)
                sys.exit()
                
        def validateStreamStatsSchema(item):
            """validateStreamStatsSchema(item=None)
                Determines if input schema is either a valid .prj file or a valid file geodatabse
            """

            filename = item.replace('\\','/').split('/')[-1]
            stateabbr = filename.split('_ss.gdb')[0]

            #validate file gdb
            if os.path.isdir(item) and filename.find('gdb') and stateabbr in states:
                try:
                    desc = arcpy.Describe(item)
                    if desc.dataType == 'Workspace':
                        messages.addMessage('Found a valid file geodatabase: ' + filename + ', item: ' + item )
                        return 'fgdb'
                except:
                    messages.addErrorMessage('You did not select a valid file geodatabase: ' + filename)

            else:
                messages.addErrorMessage('You did not select a valid schema: ' + item)
                sys.exit()

        def validateStreamStatsDataFolder(folder=None,subfolder=None):
            """validateStreamStatsDataFolder(folder=None,subfolder=None)
                Determines if input state/region data folder is valid
            """

            state = os.path.basename(folder).lower()

            #validate state
            if state.upper() not in states:
                messages.addErrorMessage('You did not select a valid state folder: ' + folder)
                sys.exit()
            if os.path.isdir(folder + '/' + subfolder):
                return True
            else:
                messages.addWarningMessage('Subfolder does not exist: ' + subfolder)
        
        def copyS3(source=None,destination=None,args=None, log=False):
            """copyS3(source=None,destination=None,args=None)
                Function to call AWS CLI tools copy source file or folder to and from s3 with error trapping
            """
            if args == None:
                args = ""
            
            if checkS3Bucket(destination) and not log:
                #delete destination folder first
                cmd="aws s3 rm " + destination +  " " + args

                try:
                    output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                except subprocess.CalledProcessError as e:
                    #messages.addErrorMessage('Make sure AWS CLI has been installed, and you have run "aws configure" to store credentials')
                    messages.addErrorMessage(e.output)
                    tb = traceback.format_exc()
                    messages.addErrorMessage(tb)
                    sys.exit()

            #create AWS CLI command
            cmd="aws s3 cp " + source + " " + destination +  " " + args

            messages.addMessage('Copying ' + source + ' to ' + destination + '...')

            try:
                output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                #messages.addErrorMessage('Make sure AWS CLI has been installed, and you have run "aws configure" to store credentials')
                messages.addErrorMessage(e.output)
                tb = traceback.format_exc()
                messages.addErrorMessage(tb)
                sys.exit()

        def checkS3Bucket(fileLocation=None):
            """checkS3Bucket(fileLocation=None)
                function to check for existence of log file in s3 bucket
            """

            cmd = "aws s3 ls " + fileLocation + " | wc -l"
            try:
                output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                if '0' in output:
                    return 'False'
                else:
                    return 'True'

            except subprocess.CalledProcessError as e:
                messages.addErrorMessage(e.output)
                tb = traceback.format_exc()
                messages.addErrorMessage(tb)
                sys.exit()
            else:
                messages.addMessage('Received list of elements in bucket')

        def logData(folder=None,state=None, accessKeyID=None, commands=None):
            logFolder = os.path.join(folder, 'log')
            logdir = os.path.join(logFolder, state.upper() + 'log.txt')
            destFolder = destinationBucket + '/' + state.lower() + '/log'
            destFile = destFolder + '/' + state.upper() + 'log.txt'

            if checkS3Bucket(destFile) == 'True':
                messages.addMessage('Log file found in s3, copying to folder')
                copyS3(destFolder, logFolder, '--recursive', True)
            else:
                messages.addMessage('No log found, creating file')
                if not arcpy.Exists(logFolder):
                    os.makedirs(logFolder)

            formatter = logging.Formatter('%(asctime)s %(message)s')
            handler = logging.FileHandler(logdir)
            logger = logging.getLogger(state + 'log')
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)
            formatter.converter = time.gmtime
            handler.setFormatter(formatter)

            with open(os.path.join(os.path.dirname( __file__ ), 'code.json')) as c:
                codejson = json.load(c)
                version = codejson[0]["version"]

            logger.info('Region: ' + state.upper() + '; Repo version: ' + version + '; User: ' + user_name + '; AWS Key ID: ' + accessKeyID + '; ' + commands + 'Note: ' + logNote)

            copyS3(logFolder, destFolder, '--recursive')

            logging.shutdown()
            arcpy.Delete_management(logdir)

        #start main program
        try:
            configureAWSKeyID(accessKeyID)
            configureAWSKey(accessKey)
            user_name = editorName

        except ImportError:
            messages.addErrorMessage('Error using aws credentials')
            sys.exit()

        destinationBucket = 's3://streamstats-staged-data/test-data'

        states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "CRB","DC", "DE", "DRB", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "RRB", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

        
        commands = 'Items Copied: '

        #check for state folder input
        if (copy_archydro or copy_bc_layers or copy_global or huc_folders) and not state_folder:
            messages.addWarningMessage('Make sure you input a state folder, then try again.')
            sys.exit()
        if state_folder:
            messages.addMessage('Folder: ' + state_folder)

            state = os.path.basename(state_folder).lower()
            messages.addMessage('Processing: ' + state)
                
            if copy_archydro == 'true' and validateStreamStatsDataFolder(state_folder, 'archydro'):
                copyS3(state_folder + '/archydro',destinationBucket + '/' + state.lower() + '/archydro', '--recursive')
                commands += 'archydro, '

            if copy_bc_layers == 'true' and validateStreamStatsDataFolder(state_folder, 'bc_layers'):
                copyS3(state_folder + '/bc_layers',destinationBucket + '/' + state.lower() + '/bc_layers', '--recursive')
                commands += 'bc_layers, '

            global_gdb = os.path.join(state_folder, 'archydro', 'global.gdb')
            if copy_global and os.path.isdir(global_gdb):
                messages.addMessage('Copying global.gdb')
                copyS3(global_gdb, destinationBucket + '/' + state.lower() + '/archydro/global.gdb', '--recursive')
                commands += 'global.gdb, '
            if huc_folders:
                huc_folders = huc_folders.split(';')
                for huc_folder in huc_folders:
                    if '/' in huc_folder: #not the best way to do it
                        huc_folder = huc_folder
                    else:
                        huc_folder = os.path.join(state_folder,'archydro', huc_folder)
                    if os.path.isdir(huc_folder):
                        huc_id = os.path.basename(huc_folder)
                        copyS3(huc_folder, destinationBucket + '/' + state.lower() + '/archydro/' + huc_id, '--recursive')
                        commands += 'huc ' + huc_id + ', '
                    else:
                        messages.addMessage('Huc folder not found: ' + huc_id)

        #check for xml file input
        if xml_file:
            messages.addMessage('Now processing xml')
            if arcpy.Exists(xml_file) and validateStreamStatsXML(xml_file):
                filename = xml_file.replace('\\','/').split('/')[-1]
                state = filename.split('.xml')[0].split('StreamStats')[1]
                copyS3(xml_file, destinationBucket + '/' + state.lower() + '/' + filename, '')
                commands += 'xml, '
            else:
                messages.addWarningMessage("There is no valid .xml file.  File should be named 'Streamstats" + state + ".xml'.")
                sys.exit()

        #check for schema file input
        if schema_file:
            messages.addMessage('Now processing schema')
            schemaType = validateStreamStatsSchema(schema_file)
            rootname = schema_file.replace('\\','/').split('/')[-1]
            state = rootname.split('_ss.gdb')[0].lower()

            if schemaType == 'fgdb':
                copyS3(schema_file, destinationBucket + '/' + state.lower() + '/' + rootname, '--recursive --dryrun')
                commands += 'schema, '

            
        logData(state_folder,state, accessKeyID, commands)

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

        pourpointwkid = arcpy.Parameter(
            displayName="Esri Well Known ID (wkid)",
            name="pourpointwkid",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        output_basin = arcpy.Parameter(
            displayName="Output Watershed",
            name="output_basin",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output")

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


        parameters = [state_folder, schema_file, xml_file, workspaceID, pourpoint, pourpointwkid, output_basin, basin_params, parameters_list] 
        return parameters
    
    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        if not parameters[5].altered:
            parameters[5].value = '4326'
        if parameters[3].altered and not parameters[6].altered:
            workspace_name = os.path.basename(parameters[3].valueAsText)
            parameters[6].value = os.path.join(parameters[3].valueAsText, workspace_name+'.gdb','Watershed')
        if parameters[6].altered:
            basin_dir = os.path.dirname(parameters[6].valueAsText)
            if not basin_dir.endswith('.gdb') or parameters[6].valueAsText.endswith('.shp'):
                try:
                    pythonaddins.MessageBox("Basin must be within a geodatabase, and should not be an '.shp' file.", 'ERROR', 0)
                except Exception, ErrorDesc:
                    logger.error("There was an error setting error message: "+str(ErrorDesc))
        return

    def UpdateMessages(self, parameters):
        return

    def execute(self, parameters, messages): 
        state_folder    = parameters[0].valueAsText
        schema_file     = parameters[1].valueAsText
        xml_file        = parameters[2].valueAsText
        workspaceID     = parameters[3].valueAsText
        pourpoint       = parameters[4].valueAsText
        pourpointwkid   = parameters[5].valueAsText
        output_basin    = parameters[6].valueAsText
        basin_params    = parameters[7].valueAsText
        parameters_list = parameters[8].valueAsText

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

        
        messages.addMessage('Delineating Basin')
        schemaCheck = validateSchema(schema_file)
        xmlCheck = validateXML(xml_file)
        ppoint = validatePourPoint(pourpoint)
        try:
            ssdel = Delineation(stabbr, schemaCheck, xmlCheck, workspaceID, state_folder)
            ppoint = ssdel._buildAHPourpoint(ppoint, pourpointwkid)
            ssdel.Delineate(ppoint)
            


        except:
            tb = traceback.format_exc()
            messages.addErrorMessage(tb)
        
        finally:
            messages.addGPMessages()


        if not parameters_list or not basin_params:
            Output_location = os.path.dirname(output_basin)
            Output_file = os.path.basename(output_basin)
                    
            if arcpy.Exists(GW_file):
                messages.addMessage('Converting to output file')
                arcpy.FeatureClassToFeatureClass_conversion(GW_file, Output_location, Output_file)


        if basin_params or parameters_list:
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
            finally:
                Output_location = os.path.dirname(output_basin)
                Output_file = os.path.basename(output_basin)
                if arcpy.Exists(GW_file):
                    messages.addMessage('Converting to output file')
                    arcpy.FeatureClassToFeatureClass_conversion(GW_file, Output_location, Output_file)
                messages.addGPMessages()

class basinParams(object):
    # region Constructor
    def __init__(self):
        self.label = "Calculate Basin Characteristics"
        self.description = ""

    def getParameterInfo(self):
        # Define parameter definitions

        stabbr = arcpy.Parameter(
            displayName="Abbreviated region name",
            name="stabbr",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        workspaceID = arcpy.Parameter(
            displayName="Workspace folder",
            name="workspaceID",
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

        parameters = [stabbr, workspaceID, parameters_list, input_basin] 
        return parameters
    
    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        if parameters[1].altered and not parameters[3].altered:
            workspace_name = os.path.basename(parameters[1].valueAsText)
            parameters[3].value = os.path.join(parameters[1].valueAsText, workspace_name+'.gdb','Watershed')
        return

    def UpdateMessages(self, parameters):
        return

    def execute(self, parameters, messages): 
        stabbr          = parameters[0].valueAsText
        workspaceID     = parameters[1].valueAsText
        parameters_list = parameters[2].valueAsText
        input_basin     = parameters[3].valueAsText

        arcpy.env.overwriteOutput = True

        if not parameters_list:
            parameters_list = ''
        workspace_name = os.path.basename(workspaceID)
        GW_location = os.path.join(workspaceID, workspace_name + '.gdb', 'Layers')
        GW_file = os.path.join(GW_location, 'GlobalWatershed')


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
            messages.addGPMessages()
