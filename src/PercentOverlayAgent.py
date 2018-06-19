#------------------------------------------------------------------------------
#----- PercentOverlayAgent.py --------------------------------------------------
#------------------------------------------------------------------------------
#
#  copyright:  2018 WiM - USGS
#
#    authors:  Jeremy K. Newson - USGS Web Informatics and Mapping (WiM)
#              
#    purpose:  Computes the percent area of overlayed regions.
#
#
#      dates:   16 JAN 2018 jkn - Created, adapted from PercentOverlayRESTSOE
#               02 FEB 2018 jkn - fixed issue where mask and infeature where inverted in spatial Overlay method
#
#------------------------------------------------------------------------------
import traceback
import os
import arcpy
import tempfile
from arcpy import env
import shutil

class PercentOverlayAgent(object):

    #region Constructor and Dispose
    def __init__(self, workspacePath, regions):
        self.Workspace = ""
        self.Regions = None
        self.isInit = False
        self.Mask = None

        arcpy.ResetEnvironments() 
        self._WorkspaceDirectory = workspacePath
        self._TempLocation = tempfile.mkdtemp(dir=self._WorkspaceDirectory)
        
        arcpy.env.workspace = self._TempLocation 
        arcpy.env.overwriteOutput = True
        
        self._initialize(regions)   
        
        arcpy.env.workspace = self._TempLocation 
        arcpy.env.overwriteOutput = True
        self._sm("initialized Percent Overlay Agent")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        #clean up
        self.Workspace = ""
        self.Regions = None
        shutil.rmtree(self._TempLocation, True)
        arcpy.ResetEnvironments()
        arcpy.ClearEnvironment("workspace")
    #endregion   

    #region Methods
    def Execute(self, inmask):
        temp = None
        pArray = []
        try:
            # Describe a feature class using arcpy.da.Describe
            desc = arcpy.Describe(inmask)
            if (inmask == None or desc.shapeType != "Polygon"): raise Exception("Invalid maskType")
            mask = inmask
            maskArea = self._getAreaSqMeter(mask)
            for region in self.Regions:
                sr = arcpy.Describe(region).spatialReference
                features = self._spatialOverlay(region,mask,'INTERSECT')
                if features == None: continue                   
                
                with arcpy.da.SearchCursor(features, ["SHAPE@","Name", "GRIDCODE"]) as source_curs:
                    for row in source_curs:
                        featurePolygon = self._clip(row[0],mask)
                        area = featurePolygon.area * sr.metersPerUnit * sr.metersPerUnit
                        localrounded = round(area/maskArea,2)*100
                        feature = row[0]
                        pArray.append({  
                                        "name":  row[1],  
                                        "code":  row[2],  
                                        "percent":  localrounded,  
                                        "areasqmeter":  area,  
                                        "maskareasqmeter":  maskArea  
                                        })

            #next region
            return pArray

        except:
            tb = traceback.format_exc()
            self._sm("Trace Error "+tb, "ERROR")
        finally:
            #local cleanup
            temp = None

    #endregion

    #region Helper Methods
    def _initialize(self, ingdb):
        try:
            #check if regions and workspace are valid
            if not arcpy.Exists(ingdb): raise Exception("Regions dataset does not exist")
            self.Regions= self._getPolygonFeaturesInGDB(ingdb)
            self.isInit = True
        except:
            self.isInit = False
    def _getPolygonFeaturesInGDB(self, ingdb):
        #list all Feature Classes in a geodatabase, including inside Feature Datasets
        arcpy.env.workspace = ingdb
        print 'Processing ', arcpy.env.workspace

        fcs = []
        for fds in arcpy.ListDatasets('','feature') + ['']:
            for fc in arcpy.ListFeatureClasses('*','POLYGON',fds):
                #yield os.path.join(fds, fc)
                fcs.append(os.path.join(arcpy.env.workspace,fds, fc))
        return fcs
    def _projectFeature(self, inFeature, sr):
        #http://joshwerts.com/blog/2015/09/10/arcpy-dot-project-in-memory-featureclass/
        inSR = None
        out_projected_fc = None
        path =""
        name =""
        source_curs = None
        ins_curs = None
        row = None
        try:
            inSR = arcpy.Describe(inFeature).spatialReference
            if (inSR.name == sr.name): return inFeature

            name = arcpy.Describe(inFeature).name +"_proj"
            
            out_projected_fc = arcpy.management.CreateFeatureclass(self._TempLocation, name,
                                                arcpy.Describe(inFeature).shapeType,
                                                template=inFeature,
                                                spatial_reference=sr)

            # specify copy of all fields from source to destination
            fields = ["Shape@"]

            # project source geometries on the fly while inserting to destination featureclass
            with arcpy.da.SearchCursor(inFeature, fields, spatial_reference=sr) as source_curs,\
                arcpy.da.InsertCursor(out_projected_fc, fields) as ins_curs:
                for row in source_curs:
                    ins_curs.insertRow(row)
                #next
            #end with

            return out_projected_fc
        except:
            tb = traceback.format_exc()
            raise Exception("Failed to project feature " +tb)
        finally:
            inSR = None
            out_projected_fc= None
            path =""
            name =""
            if source_curs != None: del source_curs
            if ins_curs != None: del ins_curs
            if row != None: del row
    def _spatialOverlay(self, inFeature, maskfeature, matchOption = "COMPLETELY_CONTAINS"):
        mask = None
        try:
            sr = arcpy.Describe(inFeature).spatialReference
            mask = self._projectFeature(maskfeature,sr) 
            out_projected_fc = os.path.join(self._TempLocation, "ovrlytmpso")
            feature = arcpy.SpatialJoin_analysis(inFeature, maskfeature, out_projected_fc,'JOIN_ONE_TO_MANY', 'KEEP_COMMON', None, matchOption)
            if(arcpy.management.GetCount(feature)[0] == "0"): return None
            return feature

        except:
             tb = traceback.format_exc()
             self._sm(tb,"Error",152)
        finally:
            mask = None 
            #do not release
            out_projected_fc = None      
    def _getAreaSqMeter(self, inFeature):
        AreaValue = -999
        try:
            sr = arcpy.Describe(inFeature).spatialReference
            cursor = arcpy.da.SearchCursor(inFeature, "SHAPE@")
            for row in cursor:
                AreaValue = row[0].area * sr.metersPerUnit * sr.metersPerUnit 
                break
            return AreaValue if (AreaValue > 0) else None
        except:
            tb = traceback.format_exc()
            self._sm("Error computing area "+tb,"ERROR")
            return None 
    def _clip (self, geom, mask ):
        
        try:
            sr = arcpy.Describe(mask).spatialReference
            if(geom.spatialReference.name != sr.name):
                item = geom.projectAs(sr)
            else:
                item = geom

            with arcpy.da.SearchCursor(mask, ["SHAPE@"]) as source_curs:
                for row in source_curs:
                    returnItem = item.intersect(row[0],4)
                #next row
            #end with

            return returnItem
        except:
            tb = traceback.format_exc()
            self._sm("clip error "+tb, "ERROR")

    def _sm(self, msg,type="INFO", errorID=0):        
        print(msg)        
        #arcpy.AddMessage(msg)
