#------------------------------------------------------------------------------
#----- UpdateS3.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2018 WIM - USGS

#   authors:  Katrin Jacobsen USGS Web Informatics and Mapping
# 
#   purpose:  Pulls SS data from AWS S3 bucket

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

class Main(object):

    #region Constructor
    def __init__(self, parameters):
        self.isComplete = False
        self.Message = ""
        workspace = parameters[3].valueAsText

        self.__TempLocation__ = os.path.join(workspace, "Temp")

        if not os.path.exists(self.__TempLocation__): 
            os.makedirs(self.__TempLocation__)

        logdir = os.path.join(self.__TempLocation__, 'pullFromS3.log')

        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler = logging.FileHandler(logdir)
        self.__logger__ = logging.getLogger('pullFromS3')
        self.__logger__.setLevel(logging.INFO)
        self.__logger__.addHandler(handler)
        handler.setFormatter(formatter)
        
        self.__run__(parameters)  
            
    #endregion  

    def __run__(self, parameters):
        self.__sm__('Initialized') 
        regionID       = parameters[0].valueAsText
        accessKeyID    = parameters[1].valueAsText
        accessKey      = parameters[2].valueAsText
        workspace      = parameters[3].valueAsText
        copy_whole     = parameters[4].valueAsText
        copy_archydro  = parameters[5].valueAsText
        copy_global    = parameters[6].valueAsText
        huc_ids        = parameters[7].valueAsText
        copy_bc_layers = parameters[8].valueAsText
        copy_xml       = parameters[9].valueAsText
        copy_schema    = parameters[10].valueAsText
        
        arcpy.env.overwriteOutput = True


        #start main program
        try:
            self.__configureAWSKeyID__(accessKeyID)
            self.__configureAWSKey__(accessKey)
            
        except ImportError:
            print "Error using aws credentials"
            arcpy.AddError('Error using aws credentials')
            sys.exit()

        destinationBucket = 's3://streamstats-staged-data'

        self.states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "CRB","DC", "DE", "DRB", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MO_STL", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "RRB", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]


        try:
            if not regionID:
                print 'ERROR: A state folder is required for one or more functions.'
                arcpy.AddError('A state folder is required for one or more functions.')
                sys.exit()
            if all([copy_whole == 'false', copy_archydro == 'false', copy_global == 'false', huc_ids == '',  copy_bc_layers == 'false', copy_xml == 'false', copy_schema == 'false']):
                print 'ERROR: Please choose at least one thing to copy'
                arcpy.AddError('Please choose at least one thing to copy')
                sys.exit()
            if regionID.upper() not in self.states and regionID != 'all':
                print 'ERROR: Region ID not found. Please use the region abbreviation, e.g. "AK" for Alaska'
                arcpy.AddError('Region ID not found. Please use the region abbreviation, e.g. "AK" for Alaska')
                sys.exit()
            elif regionID != 'all':
				self.states = [regionID]
            
            for state in self.states:
                state_folder = os.path.join(workspace, state)
                dest_state = destinationBucket + '/data/' + state.lower()
                xml_loc =  state_folder + '/StreamStats' + state.upper() + '.xml'
                dest_xml = dest_state + '/StreamStats' + state.upper() + '.xml'
                self.__sm__('Processing: ' + state)
                arcpy.AddMessage('Processing: ' + state)
                
                if copy_bc_layers == 'true':
                    self.__copyS3__(dest_state + '/bc_layers', state_folder + '/bc_layers', '--recursive')
                    
                if copy_archydro == 'true':
                    arcpy.AddMessage('copy_archydro: ' + copy_archydro);
                    self.__copyS3__(dest_state + '/archydro', state_folder + '/archydro', '--recursive')

                if copy_global == 'true':
                    global_path = '/archydro/global.gdb'
                    self.__copyS3__(dest_state + global_path, state_folder + global_path, '--recursive')
                if huc_ids:
                    huc_ids = huc_ids.split(';')
                    for huc_id in huc_ids:
                        huc_path = '/archydro/' + huc_id
                        self.__copyS3__(dest_state + huc_path, state_folder + huc_path, '--recursive')
                if any([copy_xml == 'true', copy_archydro == 'true', copy_bc_layers == 'true', huc_ids]):
                    self.__copyS3__(dest_xml, xml_loc, '')
                if copy_schema == 'true':
                    schema_path = '/' + state.upper() + '_ss.gdb/'
                    schema_path1 = '/' + state.lower() + '_ss.gdb/'
                    if self.__checkS3Bucket__(dest_state + schema_path) == 'True':
                        self.__copyS3__(dest_state + schema_path, state_folder + schema_path, '--recursive')
                    elif self.__checkS3Bucket__(dest_state + schema_path1) == 'True':
                        self.__copyS3__(dest_state + schema_path1, state_folder + schema_path1, '--recursive')
                if copy_whole == 'true':
                    self.__copyS3__(dest_state + '/', state_folder, '--recursive')
                    ParseData(state_folder, state, workspace, xml_loc , 'true', 'true', huc_ids, 'pull')
                else:
                    ParseData(state_folder, state, workspace, xml_loc , copy_archydro, copy_bc_layers, huc_ids, 'pull')

            self.isComplete = True
            self.__sm__('Finished \n')

        except:
            tb = traceback.format_exc() 
            self.__sm__("Error uploading data to S3 "+tb,"ERROR")
            print tb
            arcpy.AddError(tb)
            self.isComplete = False

    def __configureAWSKeyID__(self, AWSKeyID):
                """configureAWSKeyID(AWSKeyID=None)
                    Function to configure AWS Access Key ID
                """
                #create AWS CLI command
                cmd="aws configure set aws_access_key_id " + AWSKeyID

                try:
                    output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                except subprocess.CalledProcessError as e:
                    #messages.addErrorMessage('Configure not successful.  Please make sure you have inserted the correct credentials.')
                    print e.output
                    arcpy.AddError(e.output)
                    tb = traceback.format_exc()
                    print tb
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
            #messages.addErrorMessage('Configure not successful.  Please make sure you have entered the correct credentials.')
            print e.output
            arcpy.AddError(e.output)
            tb = traceback.format_exc()
            print tb
            arcpy.AddError(tb)
            sys.exit()
        else:
            self.__sm__('Finished configuring AWS Secret Access Key')

    def __copyS3__(self, source=None,destination=None,args=None, log=False):
        """copyS3(source=None,destination=None,args=None)
            Function to call AWS CLI tools copy source file or folder to and from s3 with error trapping
        """
        if args == None:
            args = ""
        
        if self.__checkS3Bucket__(source) == 'True':
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
                    arcpy.AddError('Make sure AWS CLI has been installed')
                    print e.output
                    arcpy.AddError(e.output)
                    print tb
                    arcpy.AddError(tb)
                    self.__sm__(e.output)
                    self.__sm__(tb)
        else:
            print source + ' not found'
            self.__sm__(source + ' not found')
            arcpy.AddMessage(source + ' not found')


    def __checkS3Bucket__(self, fileLocation=None):
        """checkS3Bucket(fileLocation=None)
            function to check for existence of files in s3 bucket
        """
        cmd = "aws s3 ls " + fileLocation + " | wc -l"
        try:
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            if '0' in output:
                return 'False'
            else:
                return 'True'

        except subprocess.CalledProcessError as e:
            print e.output
            arcpy.AddError(e.output)
            tb = traceback.format_exc()
            print tb
            arcpy.AddError(tb)
            sys.exit()
        else:
            self.__sm__('Received list of elements in bucket')


    def __sm__(self, msg, type = 'INFO'):
        self.Message += type +':' + msg.replace('_',' ') + '_'

        if type in ('ERROR'): self.__logger__.error(msg)
        else : self.__logger__.info(msg)

if __name__ == '__main__':
    #add stuff here for args, etc. if using from command line
    """
    C:\Users\kjacobsen\Desktop\StreamStats-Tools\src\PullFromS3.py(['ct', accessKeyID, accessKey, editorName, workspace, state_folder, xml_file, 'true', 'true', 'false',,schema_file])
     C:\Python27\ArcGISx6410.5\python.exe PullFromS3.py(['ct','test','test',r'C\Users\kjacobsen\Documents\wim_projects\ss_data\test-pull\1','true','false','false','','false','false','false])
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("-region_id", help="specifies the regional folder to pull from S3", type=str, default='all')
    parser.add_argument("-accessKeyID", help="specifies the AWS Access Key ID for the person pulling the data", type=str, default='xxxxxxxxxxxxxxxxxxxx')
    parser.add_argument("-accessKey", help="specifies the AWS Secret Access Key of the person pulling the data", type=str, default='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
    parser.add_argument("-workspace", help="specifies the workspace folder where the regional data will go", type=str, default=r'')
    parser.add_argument("-copy_whole", help="indicates whether to copy the entire regional folder", type=str, default='true')
    parser.add_argument("-copy_whole_archydro", help="indicates whether to copy the entire archydro folder", type=str, default='false')
    parser.add_argument("-copy_global", help="indicates whether to copy the global.gdb", type=str, default='false')
    parser.add_argument("-huc_folders", help="indicates which huc folders to copy", type=str, default='')
    parser.add_argument("-copy_bc_layers", help="indicates whether to copy the entire bc_layers folder", type=str, default='false')
    parser.add_argument("-copy_xml", help="indicates whether to copy the region's .xml file", type=str, default='false')
    parser.add_argument("-copy_schema", help="indicates whether to copy the region's schema folder", type=str, default='false')
    
    args = parser.parse_args()
    parameters = []
    parameters.extend((args.region_id, args.accessKeyID, args.accessKey, args.workspace, args.copy_whole, args.copy_whole_archydro, args.copy_global, 
        args.huc_folders,args.copy_bc_layers, args.copy_xml, args.copy_schema))
    Main(parameters)