# -*- coding: UTF-8 -*-
''' Metadata, Copyright, License: 
------------------------------------------------------------------------
Name:       lidarlas.py
Purpose:    This module contians worker functions for processing and
            classifying LAS datasets in ArcGIS Pro
Usage:      [Usage]
Author:     Whitacre, James V. | whitacrej@carnegiemnh.org
Created:    2020/01/13
Modified:   2020/01/13
Version:    1.0.0
Copyright:  Copyright 2020 Carnegie Institute, Carnegie Museum of
            Natural History, Powdermill Nature Reserve
Licence:    Licensed under the Apache License, Version 2.0 (the
            "License"); you may not use this file except in compliance
            with the License. You may obtain a copy of the License at
            http://www.apache.org/licenses/LICENSE-2.0
            Unless required by applicable law or agreed to in writing,
            software distributed under the License is distributed on an
            "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
            either express or implied. See the License for the specific
            language governing permissions and limitations under the
            License.​
------------------------------------------------------------------------
'''

''' Import Modules '''
import arcpy
import os


''' Functions '''
# Change Las Class Codes
def ChangeLasClassCodes(in_las_dataset, class_codes, compute_stats='COMPUTE_STATS', extent='DEFAULT', boundary=None, process_entire_files='PROCESS_EXTENT'):
    arcpy.ddd.ChangeLasClassCodes(in_las_dataset, class_codes, compute_stats, extent, boundary, process_entire_files)
    return in_las_dataset

# Classify LAS Building
def ClassifyLasBuilding(in_las_dataset, min_height='2 Meters', min_area='10 SquareMeters', compute_stats='COMPUTE_STATS', extent='DEFAULT', boundary=None, process_entire_files='PROCESS_EXTENT', point_spacing=None, reuse_building='RECLASSIFY_BUILDING', photogrammetric_data='NOT_PHOTOGRAMMETRIC_DATA'):
    arcpy.ddd.ClassifyLasBuilding(in_las_dataset, min_height, min_area, compute_stats, extent, boundary, process_entire_files, point_spacing, reuse_building, photogrammetric_data)
    return in_las_dataset

# Classify LAS By Height
def ClassifyLasByHeight(in_las_dataset, ground_source, height_classification, noise='NONE', compute_stats='COMPUTE_STATS', extent='DEFAULT', boundary=None, process_entire_files='PROCESS_EXTENT'):
    arcpy.ddd.ClassifyLasByHeight(in_las_dataset, ground_source, height_classification, noise, compute_stats, extent, process_entire_files, boundary)
    return in_las_dataset

# Classify LAS Ground
def ClassifyLasGround(in_las_dataset, method, reuse_ground='RECLASSIFY_GROUND', dem_resolution=None, compute_stats='COMPUTE_STATS', extent='DEFAULT', boundary=None, process_entire_files='PROCESS_EXTENT'):
    arcpy.ddd.ClassifyLasGround(in_las_dataset, method, reuse_ground, dem_resolution, compute_stats, extent, boundary, process_entire_files)
    return in_las_dataset

# Classify LAS Noise
def ClassifyLasNoise(in_las_dataset, method='ISOLATION', edit_las='CLASSIFY', withheld='NO_WITHHELD', ground=None, low_z=None, high_z=None, max_neighbors=None, step_width=None, step_height=None, out_feature_class=None, compute_stats='COMPUTE_STATS', extent='DEFAULT', process_entire_files='PROCESS_EXTENT'):
    arcpy.ddd.ClassifyLasNoise(in_las_dataset, method, edit_las, withheld, compute_stats, ground, low_z, high_z, max_neighbors, step_width, step_height, extent, process_entire_files, out_feature_class)
    return in_las_dataset

# Set LAS Class Codes Using Features
def SetLasClassCodesUsingFeaturesFiltered(in_las_dataset, feature_class, class_code_filter=None, compute_stats='COMPUTE_STATS'):
    if class_code_filter:
        # Create a unique LAS Dataset Layer name
        las_layer = 'FilterLAS'
        i = 0
        while arcpy.Exists(las_layer):
            i += 1
            las_layer = f'{las_layer}_{i}'
        # Create filtered LAS Dataset Layer
        arcpy.management.MakeLasDatasetLayer(in_las_dataset, las_layer, class_code_filter.replace(' ', '').replace(',', ';'))
    else:
        las_layer = in_las_dataset
    arcpy.ddd.SetLasClassCodesUsingFeatures(las_layer, feature_class, compute_stats)
    if class_code_filter:
        arcpy.management.Delete(las_layer)
    return in_las_dataset

# LAS Point Statistics As Raster
def LasPointStatsAsRaster(in_las_dataset, out_raster, method='POINT_COUNT', sampling_type='CELLSIZE', sampling_value=1, class_code_filter=None):
    if class_code_filter:
        # Create a unique LAS Dataset Layer name
        las_layer = 'LAS_Stats_Temp'
        i = 0
        while arcpy.Exists(las_layer):
            i += 1
            las_layer = f'{las_layer}_{i}'
        # Create filtered LAS Dataset Layer
        arcpy.management.MakeLasDatasetLayer(in_las_dataset, las_layer, class_code_filter.replace(' ', '').replace(',', ';'))
    else:
        las_layer = in_las_dataset
    arcpy.management.LasPointStatsAsRaster(las_layer, out_raster, method, sampling_type, sampling_value)
    if class_code_filter:
        arcpy.management.Delete(las_layer)
    return out_raster

# Reclass Smooth
def ReclassSmooth(in_raster, out_raster):
    reclass = f'{os.path.splitext(in_raster)[0]}_Reclass.tif'
    arcpy.ddd.Reclassify(in_raster, 'Value', ';NODATA 0', reclass, 'DATA')
    focal = arcpy.sa.FocalStatistics(reclass, 'Rectangle 3 3 CELL', 'MEAN')
    focal.save(out_raster)
    arcpy.management.Delete(reclass)
    return out_raster
