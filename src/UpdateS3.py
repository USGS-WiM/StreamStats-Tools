#------------------------------------------------------------------------------
#----- UpdateS3.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2018 WIM - USGS

#   authors:  Katrin Jacobsen USGS Web Informatics and Mapping
# 
#   purpose:  Uploads SS data to AWS S3 bucket

#region "Comments"
#11.01.2018 kj - Created
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
import sys
import argparse

import sys, os, subprocess, fnmatch, traceback
from ParseData import Main as ParseData
import datetime, arcpy, json, logging
import time
import json
import secrets

class Main(object):

    #region Constructor
    def __init__(self, parameters):
        self.isComplete = False
        self.Message = ""
        workspace = parameters[2].valueAsText if type(parameters[2]).__name__ == 'geoprocessing parameter object' else parameters[2]

        self.__TempLocation__ = os.path.join(workspace, "Temp" + time.strftime('%Y%m%d%H%M%S'))

        if not os.path.exists(self.__TempLocation__): 
            os.makedirs(self.__TempLocation__)

        logdir = os.path.join(self.__TempLocation__, 'updateS3.log')

        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler = logging.FileHandler(logdir)
        self.__logger__ = logging.getLogger('updateS3')
        self.__logger__.setLevel(logging.INFO)
        self.__logger__.addHandler(handler)
        handler.setFormatter(formatter)
        
        self.__run__(parameters, self.__TempLocation__)  
            
    #endregion  

    def __run__(self, parameters, tempLocation):
        self.__sm__('Initialized')
        # adjust for running command line, where the parameters come as normal types, not parameter objects
        logNote        = parameters[0].valueAsText if type(parameters[0]).__name__ == 'geoprocessing parameter object' else parameters[0]
        editorName     = parameters[1].valueAsText if type(parameters[1]).__name__ == 'geoprocessing parameter object' else parameters[1]
        workspace      = parameters[2].valueAsText if type(parameters[2]).__name__ == 'geoprocessing parameter object' else parameters[2]
        state_folder   = parameters[3].valueAsText if type(parameters[3]).__name__ == 'geoprocessing parameter object' else parameters[3]
        xml_file       = parameters[4].valueAsText if type(parameters[4]).__name__ == 'geoprocessing parameter object' else parameters[4]
        copy_bc_layers = parameters[5].valueAsText if type(parameters[5]).__name__ == 'geoprocessing parameter object' else parameters[5]
        copy_archydro  = parameters[6].valueAsText if type(parameters[6]).__name__ == 'geoprocessing parameter object' else parameters[6]
        copy_global    = parameters[7].valueAsText if type(parameters[7]).__name__ == 'geoprocessing parameter object' else parameters[7]
        huc_folders    = parameters[8].valueAsText if type(parameters[8]).__name__ == 'geoprocessing parameter object' else parameters[8]
        schema_file    = parameters[9].valueAsText if type(parameters[9]).__name__ == 'geoprocessing parameter object' else parameters[9]
        
        arcpy.env.overwriteOutput = True


        #start main program
        try:
            self.__configureAWSKeyID__(secrets.accessKeyID)
            self.__configureAWSKey__(secrets.accessKey)
            user_name = editorName

        except ImportError:
            self.__sm__("Error using aws credentials", 'ERROR')
            arcpy.AddError('Error using aws credentials')
            sys.exit()

        destinationBucket = 's3://streamstats-staged-data/test-data'

        with open(os.path.join(os.path.dirname( __file__ ), 'config.json')) as c:
            config = json.load(c)
            self.version = config[0]["version"]
            self.states = config[0]["regions"]

        commands = []
        try:
            if (copy_archydro == 'true' or copy_bc_layers == 'true' or copy_global == 'true' or huc_folders) and not state_folder:
                self.__sm__('ERROR: A state folder is required for one or more functions.', 'ERROR')
                arcpy.AddError('A state folder is required for one or more functions.')
                sys.exit()
            if state_folder and not xml_file:
                self.__sm__('ERROR: An .xml file is required for parsing the data before upload.', 'ERROR')
                arcpy.AddError('An .xml file is required for parsing the data before upload.')
                sys.exit()
            #check for xml file input
            if xml_file:
                self.__sm__('Now processing xml: ' + xml_file)
                filename = xml_file.replace('\\','/').split('/')[-1]
                if 'StreamStats' in filename:
                    state = filename.split('.xml')[0].split('StreamStats')[1].upper()
                else:
                    self.__printXMLError__(filename, '{region abbrevation}')
                checkXML = self.__validateStreamStatsXML__(xml_file)
                if checkXML:
                    parse = ParseData(state_folder, state, tempLocation, xml_file, copy_archydro, copy_bc_layers, huc_folders, copy_global, 'upload')
                    parse_file = parse.__xmlPath__
                    commands.append('xml')
                    if arcpy.Exists(parse_file):
                        self.__copyS3__(parse_file, destinationBucket + '/' + state.lower() + '/' + filename, '')
                else:
                    self.__printXMLError__(filename, state)

            if schema_file:
                self.__sm__('Now processing schema: ' + schema_file)
                schemaType = self.__validateStreamStatsSchema__(schema_file)
                rootname = schema_file.replace('\\','/').split('/')[-1]
                state = rootname.split('_ss.gdb')[0].lower()

                if schemaType == 'fgdb':
                    commands.append('schema')
                    self.__copyS3__(schema_file, destinationBucket + '/' + state.lower() + '/' + rootname, '--recursive')

            if state_folder:
                state = os.path.basename(state_folder).lower()
                self.__sm__('Processing: ' + state)
                arcpy.AddMessage('Processing: ' + state)
                if not parse_file:
                    parse = ParseData(state_folder, state, tempLocation, xml_file, copy_archydro, copy_bc_layers, huc_folders, copy_global, 'upload')
                state_folder = parse.__stateFolder__
                self.__sm__("new state folder: " + state_folder)
                    
                if copy_archydro == 'true' and self.__validateStreamStatsDataFolder__(state_folder, 'archydro'):
                    commands.append('archydro')
                    self.__copyS3__(state_folder + '/archydro',destinationBucket + '/' + state.lower() + '/archydro', '--recursive')

                if copy_bc_layers == 'true' and self.__validateStreamStatsDataFolder__(state_folder, 'bc_layers'):
                    commands.append('bc_layers')
                    self.__copyS3__(state_folder + '/bc_layers',destinationBucket + '/' + state.lower() + '/bc_layers', '--recursive')

                global_gdb = os.path.join(state_folder, 'archydro', 'global.gdb')
                if copy_global == 'true' and os.path.isdir(global_gdb):
                    commands.append('global.gdb')
                    self.__copyS3__(global_gdb, destinationBucket + '/' + state.lower() + '/archydro/global.gdb', '--recursive')
                if huc_folders:
                    huc_folders = huc_folders.split(';')
                    for huc_folder in huc_folders:
                        if '/' in huc_folder: #not the best way to do it
                            huc_folder = huc_folder
                        else:
                            huc_folder = os.path.join(state_folder,'archydro', huc_folder)
                        if os.path.isdir(huc_folder):
                            huc_id = os.path.basename(huc_folder)
                            commands.append('huc ' + huc_id)
                            self.__copyS3__(os.path.join(state_folder, 'archydro', huc_id), destinationBucket + '/' + state.lower() + '/archydro/' + huc_id, '--recursive')
                        else:
                            self.__sm__('Huc folder not found: ' + huc_folder)
                            arcpy.AddError('Huc folder not found: ' + huc_folder)
                #parse.__checkPixelDepth__(state_folder)

            seperator = ','
            commands = seperator.join(commands)
                
            self.__logData__(destinationBucket, tempLocation, state, commands, user_name, logNote)

            self.isComplete = True
            self.__sm__('Finished \n')

        except:
            tb = traceback.format_exc() 
            if 'SystemExit' not in tb:
                self.__sm__("Error uploading data to S3 "+tb,"ERROR")
                print tb
                arcpy.AddError(tb)
            self.isComplete = False

        finally:
            arcpy.ResetEnvironments()

    def __configureAWSKeyID__(self, AWSKeyID):
        """configureAWSKeyID(AWSKeyID=None)
            Function to configure AWS Access Key ID
        """
        #create AWS CLI command
        cmd="aws configure set aws_access_key_id " + AWSKeyID

        try:
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            arcpy.addError('Configure not successful.  Please make sure you have installed the AWS CLI.')
            self.__sm__(e.output, 'ERROR')
            arcpy.AddError(e.output)
            tb = traceback.format_exc()
            self.__sm__(tb, 'ERROR')
            arcpy.AddError(tb)
            sys.exit()
        else:
            self.__sm__('Finished configuring AWS Key ID') 
    def __configureAWSKey__(self, AWSAccessKey):
        """configureAWSKey(AWSAccessKey=None)
            Function to configure AWS CLI Access Key
        """
        #create AWS CLI command
        cmd="aws configure set aws_secret_access_key " + AWSAccessKey

        try:
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            arcpy.addError('Configure not successful.  Please make sure you have installed the AWS CLI.')
            self.__sm__(e.output, 'ERROR')
            arcpy.AddError(e.output)
            tb = traceback.format_exc()
            self.__sm__(tb, 'ERROR')
            arcpy.AddError(tb)
            sys.exit()
        else:
            self.__sm__('Finished configuring AWS Secret Access Key')
    def __validateStreamStatsXML__(self, xml):
        """validateStreamStatsXML(xml=None)
            Determines if input xml is a valid streamstats XML file
        """
        filename = xml.replace('\\','/').split('/')[-1]

        #validate xml file
        stateabbr = filename.split('.xml')[0].split('StreamStats')[1].upper()
        if (fnmatch.fnmatch(filename, 'StreamStats*.xml') and stateabbr in self.states):
            return True
        else:
            self.__printXMLError__(filename, stateabbr)
    
    def __printXMLError__(self, filename, state):
        self.__sm__('You did not select a valid xml file: ' + filename + ". File should be named 'Streamstats" + state + ".xml', and '" + state + "' must be an accepted region.", 'ERROR')
        arcpy.AddError('You did not select a valid xml file: ' + filename + ". File should be named 'Streamstats" + state + ".xml', and '" + state + "' must be an accepted region.")
        sys.exit()

    def __validateStreamStatsSchema__(self, item):
        """validateStreamStatsSchema(item=None)
            Determines if input schema is either a valid .prj file or a valid file geodatabse
        """
        filename = item.replace('\\','/').split('/')[-1]
        stateabbr = filename.split('_ss.gdb')[0].upper()

        #validate file gdb
        if os.path.isdir(item) and filename.find('gdb') and stateabbr in self.states:
            try:
                desc = arcpy.Describe(item)
                if desc.dataType == 'Workspace':
                    self.__sm__('Found a valid file geodatabase: ' + filename + ', item: ' + item)
                    return 'fgdb'
            except:
                self.__sm__('You did not select a valid file geodatabase: ' + filename, 'ERROR')
                arcpy.AddError('You did not select a valid file geodatabase: ' + filename)
                sys.exit()

        else:
            self.__sm__('You did not select a valid schema: ' + item, 'ERROR')
            arcpy.AddError('You did not select a valid schema: ' + item)
            sys.exit()

    def __validateStreamStatsDataFolder__(self, folder=None,subfolder=None):
        """validateStreamStatsDataFolder(folder=None,subfolder=None)
            Determines if input state/region data folder is valid
        """
        state = os.path.basename(folder).lower()

        #validate state
        if state.upper() not in self.states:
            self.__sm__('You did not select a valid state folder: ' + folder, 'ERROR')
            arcpy.AddError('You did not select a valid state folder: ' + folder)
            sys.exit()
        if os.path.isdir(folder + '/' + subfolder):
            return True
        else:
            self.__sm__('Subfolder does not exist: ' + subfolder, 'ERROR')
            arcpy.AddError('Subfolder does not exist: ' + subfolder)

    def __copyS3__(self, source=None,destination=None,args=None, log=False):
        """copyS3(source=None,destination=None,args=None)
            Function to call AWS CLI tools copy source file or folder to and from s3 with error trapping
        """
        if args == None:
            args = ""
        
        if not log and self.__checkS3Bucket__(destination) == "True":
            #delete destination folder first
            cmd="aws s3 rm " + destination +  " " + args

            try:
                output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                tb = traceback.format_exc()
                if 'lock' not in e.output and 'exit status 2' not in tb:
                    self.__sm__(e.output, 'ERROR')
                    arcpy.AddError(e.output)
                    self.__sm__(tb, 'ERROR')
                    arcpy.AddError(tb)
                    sys.exit()

        #create AWS CLI command
        cmd="aws s3 cp " + source + " " + destination +  " " + args
        cmd_print = 'Copying ' + source + ' to ' + destination + '...'
        print cmd_print
        self.__sm__(cmd_print)
        arcpy.AddMessage(cmd_print)

        try:
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            tb = traceback.format_exc()
            if 'lock' not in e.output and 'exit status 2' not in tb:
                print e.output
                arcpy.AddError(e.output)
                print tb
                arcpy.AddError(tb)
                self.__sm__(e.output)
                self.__sm__(tb)
                sys.exit()

    def __checkS3Bucket__(self, fileLocation=None):
        """checkS3Bucket(fileLocation=None)
            function to check for existence of files in s3 bucket
        """
        cmd = "aws s3 ls " + fileLocation
        try:
            output = subprocess.call(cmd, shell=True)
            if output == 1:
                return 'False'
            else:
                return 'True'

        except subprocess.CalledProcessError as e:
            print e.output
            arcpy.AddError(e.output)
            tb = traceback.format_exc()
            print tb
            arcpy.AddError(tb)
            self.__sm__(e.output)
            self.__sm__(tb)
            sys.exit()
        else:
            self.__sm__('Received list of elements in bucket')

    def __logData__(self, destinationBucket, workspace=None,state=None, commands=None, username=None, logNote=None):
        logFolder = os.path.join(workspace, 'log')
        logdir = os.path.join(logFolder, state.upper() + 'log.txt')
        destFolder = destinationBucket + '/' + state.lower() + '/log'
        destFile = destFolder + '/' + state.upper() + 'log.txt'

        if self.__checkS3Bucket__(destFile) == 'True':
            self.__sm__('Log file found in s3, copying to temp folder')
            self.__copyS3__(destFolder, logFolder, '--recursive', True)
        else:
            self.__sm__('No log found in S3, creating file')
            if not arcpy.Exists(logFolder):
                os.makedirs(logFolder)

        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler = logging.FileHandler(logdir)
        logger = logging.getLogger(state + 'log')
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)

        logger.info('Region: ' + state.upper() + '; Repo version: ' + self.version + '; User: ' + username + '; Items Copied: ' + commands + '; Note: ' + logNote)

        self.__copyS3__(logFolder, destFolder, '--recursive')
        logger.removeHandler(handler)
        logging.shutdown()
        arcpy.Delete_management(logFolder)

    def __sm__(self, msg, type = 'INFO'):
        self.Message += type +':' + msg.replace('_',' ') + '_'

        if type in ('ERROR'): self.__logger__.error(msg)
        else : self.__logger__.info(msg)

    
if __name__ == '__main__':
    #add stuff here for args, etc. if using from command line
    """
        logNote        = parameters[0].valueAsText
        editorName     = parameters[1].valueAsText
        workspace      = parameters[2].valueAsText
        state_folder   = parameters[3].valueAsText
        xml_file       = parameters[4].valueAsText
        copy_bc_layers = parameters[5].valueAsText
        copy_archydro  = parameters[6].valueAsText
        copy_global    = parameters[7].valueAsText
        huc_folders    = parameters[8].valueAsText
        schema_file    = parameters[9].valueAsText
    C:\Users\kjacobsen\Desktop\StreamStats-Tools\src\UpdateS3.py(['testing', editorName, workspace, state_folder, xml_file, 'true', 'true', 'false',,schema_file])
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("-logNote", help="describes the purpose of the the upload, and any changes made to the data", type=str, default="Testing")
    parser.add_argument("-editorName", help="specifies the name of the person who edited/is uploading the data", type=str, default="KJ")
    parser.add_argument("-workspace", help="specifies the temporary workspace where the regional data will go before parsing and uploading", type=str, default=r'C:\Users\kjacobsen\Documents\wim_projects\docs\ss-data\test-ri\1')
    parser.add_argument("-state_folder", help="specifies the regional folder containing 'archydro' and bc_layers' folders", type=str, default=r'C:\Users\kjacobsen\Documents\wim_projects\docs\ss-data\test-ri\1\ri')
    parser.add_argument("-xml_file", help="specifies the regional xml file' folders", type=str, default=r'C:\Users\kjacobsen\Documents\wim_projects\docs\ss-data\test-ri\1\ri\StreamStatsRI.xml')
    parser.add_argument("-copy_bc_layers", help="indicates whether to copy the entire bc_layers folder", type=str, default='false')
    parser.add_argument("-copy_archydro", help="indicates whether to copy the entire archydro folder", type=str, default='false')
    parser.add_argument("-copy_global", help="indicates whether to copy the global.gdb", type=str, default='true')
    parser.add_argument("-huc_folders", help="indicates which huc folders to upload", type=str, default='')
    parser.add_argument("-schema_file", help="specifies the location of the regional schema .gdb", type=str, default=r'C:\Users\kjacobsen\Documents\wim_projects\docs\ss-data\test-ri\1\ri\RI_ss.gdb')

    args = parser.parse_args()
    parameters = []
    parameters.extend((args.logNote, args.editorName, args.workspace, args.state_folder, args.xml_file ,args.copy_bc_layers, 
        args.copy_archydro,args.copy_global, args.huc_folders, args.schema_file))
    Main(parameters)    