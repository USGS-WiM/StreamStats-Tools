import sys, os, subprocess, fnmatch, traceback

import datetime, arcpy, json, logging, pythonaddins
from arcpy import env
from arcpy.sa import *
from Delineation import Delineation as Delineation
from BasinParameters import BasinParameters as BasinParameters

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

        state_folders = arcpy.Parameter(
            displayName="Select input state/region folder",
            name="state_folders",
            datatype="DEFolder",
            parameterType="Optional",
            direction="Input",
            multiValue=True)

        copy_archydro = arcpy.Parameter(
            displayName="Copy 'archydro' folder",
            name="copy_data_archydro",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")

        copy_bc_layers = arcpy.Parameter(
            displayName="Copy 'bc_layers' folder",
            name="copy_data_bc_layers",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")

        xml_files = arcpy.Parameter(
            displayName="Select input xml",
            name="xml_files",
            datatype="DEFile",
            parameterType="Optional",
            direction="Input",
            multiValue=True)

        schema_files = arcpy.Parameter(
            displayName="Select input schema FGDB or PRJ file",
            name="schema_files",
            datatype="DEType",
            parameterType="Optional",
            direction="Input",
            multiValue=True)
        
        parameters = [state_folders, copy_archydro, copy_bc_layers, xml_files, schema_files]
    
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
        return

    def execute(self, parameters, messages):
        state_folders  = parameters[0].valueAsText
        copy_archydro  = parameters[1].valueAsText
        copy_bc_layers = parameters[2].valueAsText
        xml_files      = parameters[3].valueAsText
        schema_files   = parameters[4].valueAsText
        

        try:
            import secrets
            print 'Secrets file successfully imported'
            aws_access_key_id = secrets.aws_access_key_id
            aws_secret_access_key = secrets.aws_secret_access_key

            #apply keys as environment variables
            #https://stackoverflow.com/questions/36339975/how-to-automate-the-configuring-the-aws-command-line-interface-and-setting-up-pr
            os.environ['aws_access_key_id'] = aws_access_key_id
            os.environ['aws_secret_access_key'] = aws_secret_access_key

        except ImportError:
            messages.addErrorMessage('Secrets file not found')
            sys.exit()

        def validateStreamStatsXML(xml):
            """validateStreamStatsXML(xml=None)
                Determines if input xml is a valid streamstats XML file
            """

            #get filename
            filename = xml.replace('\\','/').split('/')[-1]

            #validate xml file
            stateabbr = filename.split('.xml')[0].split('StreamStats')[1].upper()
            messages.addMessage('stateabbr:' + stateabbr)
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

            #validate prj file
            if os.path.isfile(item) and fnmatch.fnmatch(filename, '*.prj'):
                try:
                    arcpy.SpatialReference(filename)
                except:
                    messages.addErrorMessage('You did not select a valid prj file: ' + filename)
                else:
                    messages.addMessage('Found a valid .prj file: ' + filename)
                    return 'prj'

            #validate file gdb
            elif os.path.isdir(item) and filename.find('gdb'):
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

            state = os.path.basename(os.path.normpath(folder))

            #validate state
            if state.upper() not in states:
                messages.addErrorMessage('You did not select a valid state folder: ' + folder)
                sys.exit()
            if os.path.isdir(folder + '/' + subfolder):
                return True
            else:
                messages.addWarningMessage('Subfolder does not exist: ' + subfolder)

        def copyToS3(source=None,destination=None,args=None):
            """copyToS3(source=None,destination=None,args=None)
                Function to call AWS CLI tools copy source file or folder to s3 with error trapping
            """

            if args == None:
                args = ""

            #create AWS CLI command
            cmd="aws s3 cp " + source + " " + destination +  " " + args

            messages.addMessage('Copying ' + source + ' to amazon s3...')
            messages.addMessage(cmd)

            try:
                output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                #messages.addErrorMessage('Make sure AWS CLI has been installed, and you have run "aws configure" to store credentials')
                messages.addErrorMessage(e.output)
                tb = traceback.format_exc()
                messages.addErrorMessage(tb)
                sys.exit()
            else:
                messages.addMessage('Finished copying')

        #start main program
        destinationBucket = 's3://streamstats-staged-data/KJ'

        states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "CRB","DC", "DE", "DRB", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "RRB", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

        messages.addGPMessages()
        
        #check for state folder input
        if state_folders:
            messages.addMessage('Folder list: ' + state_folders)

            state_folders = state_folders.split(';')

            #first process folders
            for folder in state_folders:
                state = os.path.basename(os.path.normpath(folder))
                messages.addMessage('Processing: ' + state)

                if not copy_archydro and not copy_bc_layers:
                    messages.addWarningMessage('Nothing to do.  Make sure you select at least one "Copy" checkbox')
                    sys.exit()

                if copy_archydro == 'true' and validateStreamStatsDataFolder(folder, 'archydro'):
                    messages.addMessage('Copying archydro folder for: ' + state)
                    copyToS3(folder + '/archydro',destinationBucket + '/data/' + state + '/archydro', '--recursive')

                if copy_bc_layers == 'true' and validateStreamStatsDataFolder(folder, 'bc_layers'):
                    messages.addMessage('Copying bc_layers folder for: ' + state)
                    copyToS3(folder + '/bc_layers',destinationBucket + '/data/' + state + '/bc_layers', '--recursive')

        #check for xml file input
        if xml_files:
            xml_files = xml_files.split(';')

            #first process folders
            for xml in xml_files:
                messages.addMessage('Now processing xml: ' + xml)

                if validateStreamStatsXML(xml) == True:
                    filename = xml.replace('\\','/').split('/')[-1]
                    copyToS3(xml, destinationBucket + '/xml/' + filename, '')

        #check for schema file input
        if schema_files:
            schema_files = schema_files.split(';')

            #first process folders
            for schema in schema_files:

                messages.addMessage('Now processing schema: ' + schema)

                schemaType = validateStreamStatsSchema(schema)
                rootname = schema.replace('\\','/').split('/')[-1]

                if schemaType == 'fgdb':
                    copyToS3(schema, destinationBucket + '/schemas/' + rootname, '--recursive')
                if schemaType == 'prj':
                    copyToS3(schema, destinationBucket + '/schemas/' + rootname)
                     
  

class basinDelin(object):
    # region Constructor
    def __init__(self):
        self.label = "Basin Delineation"
        self.description = ""

    def getParameterInfo(self):
        # Define parameter definitions

        stabbr = arcpy.Parameter(
            displayName="Abbreviated region name",
            name="stabbr",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        schema_file = arcpy.Parameter(
            displayName="Regional schema FGDB or PRJ file",
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
            displayName="Calculate Basin Characteristics",
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


        parameters = [stabbr, schema_file, xml_file, workspaceID, pourpoint, pourpointwkid, output_basin, basin_params, parameters_list] 
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
        stabbr          = parameters[0].valueAsText
        schema_file     = parameters[1].valueAsText
        xml_file        = parameters[2].valueAsText
        workspaceID     = parameters[3].valueAsText
        pourpoint       = parameters[4].valueAsText
        pourpointwkid   = parameters[5].valueAsText
        output_basin    = parameters[6].valueAsText
        basin_params    = parameters[7].valueAsText
        parameters_list = parameters[8].valueAsText

        arcpy.env.overwriteOutput = True

        Results = {}
        workspace_name = os.path.basename(workspaceID)
        GW_location = os.path.join(workspaceID, workspace_name + '.gdb')
        GW_file = os.path.join(GW_location, 'Layers', 'GlobalWatershed')
        GWP_file = os.path.join(GW_location, 'Layers', 'GlobalWatershedPoint')
        
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
            ssdel = Delineation(stabbr, schemaCheck, xmlCheck, workspaceID)
            ppoint = ssdel._buildAHPourpoint(ppoint, pourpointwkid)
            ssdel.Delineate(ppoint)
            

            Results = {
                    "Workspace": ssdel.WorkspaceID,
                    "Message": ssdel.Message.replace("'",'"').replace('\n',' ')
                    }

        except:
            tb = traceback.format_exc()
            messages.addMessage(tb)
            Results = {
                    "error": {"message": tb}
                    }

        finally:
            print "Results="+json.dumps(Results) 

        if not basin_params or basin_params != 'true':
            Output_location = os.path.dirname(output_basin)
            Output_file = os.path.basename(output_basin)
                    
            if arcpy.Exists(GW_file):
                messages.addMessage('Converting to output file')
                arcpy.FeatureClassToFeatureClass_conversion(GW_file, Output_location, Output_file)
                arcpy.Delete_management(GW_file)
                arcpy.Delete_management(GWP_file)


        if basin_params == 'true':
            try:
                messages.addMessage('Calculating Basin Characteristics')
                ssBp = BasinParameters(stabbr, workspaceID, parameters_list, "none")
            

                if ssBp.isComplete:
                    Results = {"Parameters": ssBp.ParameterList, "Message": ssBp.Message.replace("'",'"').replace('\n',' ')}
                else:
                    Results = {"Parameters": [],"Message": ssBp.Message.replace("'",'"').replace('\n',' ')}

                print "Results="+json.dumps(Results)
            except:
                tb = traceback.format_exc()
                messages.addMessage(tb)
                Results = {
                    "error": {"message": tb}
                }
            finally:
                print "Results=" + json.dumps(Results)
                if output_basin:
                    Output_location = os.path.dirname(output_basin)
                    Output_file = os.path.basename(output_basin)

                    if arcpy.Exists(GW_file):
                        messages.addMessage('Converting to output file')
                        arcpy.FeatureClassToFeatureClass_conversion(GW_file, Output_location, Output_file)        

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
            parameterType="Required",
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

        Results = {}
        workspace_name = os.path.basename(workspaceID)
        GW_location = os.path.join(workspaceID, workspace_name + '.gdb')
        GW_file = os.path.join(GW_location, 'Layers', 'GlobalWatershed')


        try:
            messages.addMessage('Calculating Basin Characteristics')
            ssBp = BasinParameters(stabbr, workspaceID, parameters_list, input_basin)
        

            if ssBp.isComplete:
                Results = {"Parameters": ssBp.ParameterList, "Message": ssBp.Message.replace("'",'"').replace('\n',' ')}
            else:
                Results = {"Parameters": [],"Message": ssBp.Message.replace("'",'"').replace('\n',' ')}

            print "Results="+json.dumps(Results)
        except:
            tb = traceback.format_exc()
            messages.addMessage(tb)
            Results = {
                "error": {"message": tb}
            }
        finally:
            print "Results=" + json.dumps(Results)  
