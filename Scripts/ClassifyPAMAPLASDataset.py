# -*- coding: UTF-8 -*-

''' Metadata, Copyright, License: 
------------------------------------------------------------------------
Name:       ClassifyPAMAPLASDataset.py
Purpose:    Classifies PAMAP LAS datasets to conform to the LAS file
            specification version 1.4.

            Any output raster will align to the horizontal (XY)
            coordinate system. The input LAS Dataset should contain a
            vertical (Z) coordinate system or the vertical units must be
            set in the LAS Dataset layer properties. By setting the
            output spatial reference vertical coordinate system, the
            output elevation values will automatically be converted to
            the units of the output vertical coordinate system if
            different from the input LAS dataset.
Usage:      [Usage]
Author:     Whitacre, James V. | whitacrej@carnegiemnh.org
Created:    2020/01/13
Modified:   2020/02/10
Version:    1.0.0
Copyright:  Copyright 2020 Carnegie Institute, Carnegie Museum of
            Natural History, Powdermill Nature Reserve
Licence:    This code may not be distributed.
------------------------------------------------------------------------
'''

''' Import Modules '''
import arcpy
from datetime import datetime
import lidarlas
import multiprocessing
from multiprocessing import Pool, cpu_count
import os
import shlex
import sys
import winsound


''' Parameters '''
# Required Parameters
# Input PAMAP LAS Files (Folder, File; Validation: Create Error Message if files are not LAS files or no LAS files exist in the folder)
input_files = arcpy.GetParameterAsText(0).split(';')
# Include Subfolders (Boolean; Optional; Filter: True = RECURSION, False = NO_RECURSION; Validation: Disable at initialization, Enable if a folder is in Input LAS Files)
folder_recursion = arcpy.GetParameterAsText(1)
# LAS File(s) Coordinate System (Spatial Reference; Optional; Validation: Add coordinates system from first LAS file or first LAS file in Folder)
input_las_sr = arcpy.GetParameter(2)
# Compute Statistics (Boolean; Optional; Filter: True = COMPUTE_STATS, False = NO_COMPUTE_STATS; Default: true)
compute_stats = arcpy.GetParameterAsText(3)

# Group: Change Classification Options
# Change Original Class Codes (Value Table: Current Class > Long | New Class > Long | Synthetic > String | KeyPoint > String | Withheld > String | Overlap > String | Class Code Filter > String; Optional; Filter: Current Class, New Class > Range 0 - 255 | Synthetic, KeyPoint, Withheld, Overlap > NO_CHANGE, SET, CLEAR; Default: 8 8 NO_CHANGE SET NO_CHANGE NO_CHANGE)
change_original_codes = arcpy.GetParameterAsText(4)
# Classification Feature Classes (Value Table: Features > Feature Layer | Buffer Distance > Double | New Class > Long | Synthetic > String | KeyPoint > String | Withheld > String | Overlap > String | Class Code Filter > String; Optional; Filter: Feature > Point, Polyline, Polygon | New Class > Range 0 - 255 | Synthetic, KeyPoint, Withheld, Overlap > NO_CHANGE, SET, CLEAR; Validation: Create Error Message if Class Code Filter contians text other than integer between 0 and 255 or a comma)
feature_classes = arcpy.GetParameterAsText(5).split(';') if arcpy.GetParameterAsText(5) else []

# Group: Ground Classification Options
# Ground Detection Method (String; Optional; Filter: STANDARD, CONSERVATIVE, AGGRESSIVE)
ground_method = arcpy.GetParameterAsText(6)
# Reuse Existing Ground (Boolean; Optional; Filter: True = REUSE_GROUND, False = RECLASSIFY_GROUND; Default: true; Validation: Enable if Ground Detection Method is not empty)
reuse_ground = arcpy.GetParameterAsText(7)
# DEM Resolution (Linear Unit; Optional; Validation: Enable if Ground Detection Method is not empty)
dem_resolution = arcpy.GetParameterAsText(8)

# Group: Building Classification Options
# Classify Building Points (String; Optional; Filter: RECLASSIFY_BUILDING, REUSE_BUILDING)
classify_buildings = arcpy.GetParameterAsText(9)
# Minimum Rooftop Height (Linear Unit; Optional; Filter: Meters, Feet; Validation: Enable if Classify Building Points is not empty, Set default value of 2 Meters (6.562 Feet) based on input spatial reference, Create Error Message if Minimum Rooftop Height is enabled and empty)
min_roof_height = arcpy.GetParameterAsText(10)
# Minimum Building Area (Areal Unit; Optional; Filter: Square Meters, Square Feet; Validation: Enable if Classify Building Points is not empty, Set default value of 10 Square Meters (107.639 Square Feet) based on input spatial reference, Create Error Message if Minimum Building Area is enabled and empty)
min_building_area = arcpy.GetParameterAsText(11)
# Is Photogrammetric Data (Boolean; Optional; Filter: True = PHOTOGRAMMETRIC_DATA, False = NOT_PHOTOGRAMMETRIC_DATA;Validation: Enable if Classify Building Points is not empty)
photogrammetric_data = arcpy.GetParameterAsText(12)

# Group: Vegetation Classification Options
# Vegetation Classification Method (String; Optional; Filter: EXISTING_VEGETATION, NEW_VEGETATION, ALL_VEGETATION)
vegetation_method = arcpy.GetParameterAsText(13)
# Exisiting Vegetation Classification Codes (Long, Multiple values; Optional; Filter: Range 0 - 255; Validation: Enable if Vegetation Classification Method is EXISTING_VEGETATION or ALL_VEGETATION, Create Error Message if Exisiting Vegetation Classification Codes is enabled and empty)
exist_veg_codes = arcpy.GetParameterAsText(14).split(';')
# Height Classification (Value Table: Class Code > Long | Height > Double; ; Filter: Class Code > Range 0 - 255; Validataion: Enable if Vegetation Classification Method is not empty, Set default values based on input spatial reference unit of '1 0.5;3 2;4 5;5 65' for meter and '1 1.64;3 6.562;4 16.404;5 213.255' for feet, Create Error Message if Height Classification is enabled and empty)
height_classification = arcpy.GetParameterAsText(15)
# Vegetation Area Cell Size (Double; Optional; Validation: Enable if Vegetation Classification Method is not empty, Set default values based on input spatial reference unit of 5 for meter and 16.404 for feet, Create Error Message if Vegetation Area Cell Size is enabled and empty AND if Classify Building Points is not empty OR Output Vegetation Areas Feature Class is not empty)
vegetation_cell_size = arcpy.GetParameter(16)

# Group: Noise Classification Options
# Noise Classification Method (String; Optional; Filter: ALL_NOISE, HIGH_NOISE, NONE; Default: ALL_NOISE)
noise_method = arcpy.GetParameterAsText(17)
# Noise Height From Ground (Double; Optional; Validation: Enable if Noise Classification is ALL_NOISE or HIGH_NOISE, Set default value based on input spatial reference unit of 65 for meter or 213.255 for feet, Create Error Message if Height Classification is enabled and empty)
noise_height = arcpy.GetParameterAsText(18)
# Noise Neighborhood Point Limit (Long; Optional; Validation: Enable if Noise Classification is ALL_NOISE, Set default value as 25, Create Error Message if Height Classification is enabled and empty)
max_neighbors = arcpy.GetParameterAsText(19)
# Noise Neighborhood Width (Linear Unit; Optional; Filter: Meters, Feet; Validation: Enable if Noise Classification is ALL_NOISE, Set default value based on input spatial reference unit of 10 Meters or 32.808 for feet, Create Error Message if Height Classification is enabled and empty)
step_width = arcpy.GetParameterAsText(20)
# Noise Neighborhood Height (Linear Unit; Optional; Filter: Meters, Feet; Validation: Enable if Noise Classification is ALL_NOISE, Set default value based on input spatial reference unit of 10 Meters or 32.808 for feet, Create Error Message if Height Classification is enabled and empty)
step_height = arcpy.GetParameterAsText(21)

# Group: Processing Extent
# Processing Extent (Extent; Optional; Default: DEFAULT)
extent = arcpy.GetParameterAsText(22)
# Processing Boundary (Feature Set; Optional)
boundary = arcpy.GetParameterAsText(23)
# Process Entire LAS Files That Intersect Extent (Boolean; Optional; Filter: True = PROCESS_ENTIRE_FILES, False = PROCESS_EXTENT )
process_entire_files = arcpy.GetParameterAsText(24)

# Group: Output Options
# Output LAS Dataset (LAS Dataset; Optional; Output)
out_las_dataset = arcpy.GetParameterAsText(25)


''' Script '''
# Variables
global_args = [compute_stats, extent, boundary, process_entire_files]
scratch_folder = arcpy.env.scratchFolder
scratch_gdb = arcpy.env.scratchGDB

# Set Multiprocessing Execution Space
multiprocessing.set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))

# Create List of All Input LAS Files
input_las_files = []

for i in input_files:
    i_desc = arcpy.Describe(i)
    if i_desc.dataType == 'Folder':
        if folder_recursion == 'NO_RECURSION' or not folder_recursion:
            input_las_files.extend([child.catalogPath for child in i_desc.children if child.dataType == 'LasDataset'])
        else:
            walk = arcpy.da.Walk(i, datatype='LasDataset')
            input_las_files.extend([os.path.join(dirpath, filename) for dirpath, dirnames, filenames in walk for filename in filenames if os.path.splitext(filename)[1] == '.las'])
    elif i_desc.dataType == 'LasDataset':
        input_las_files.append(i)

# Get CPU Count
cpu_ct = cpu_count() if len(input_las_files) > cpu_count() else len(input_las_files)
arcpy.AddMessage(f'Total CPU Count: {cpu_count()} | File Count: {len(input_las_files)} | CPUs Used: {cpu_ct}')

# Create a temporary ArcGIS LAS Dataset of all LAS files
lasd_temp = arcpy.CreateUniqueName(os.path.join(scratch_folder, 'LASD_Temp.lasd'))
arcpy.management.CreateLasDataset(input_files, lasd_temp, folder_recursion, None, input_las_sr, compute_stats, 'RELATIVE_PATHS', 'NO_FILES')

# Change Original Class Codes
if change_original_codes:
    arcpy.SetProgressorLabel('Changing Original Class Codes...')
    change_codes_arg = [change_original_codes]
    change_codes_args = [[las_file] + change_codes_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ChangeLasClassCodes, change_codes_args)

# Reclassify Exisiting Vegetation Points
if exist_veg_codes and vegetation_method in ['EXISTING_VEGETATION', 'ALL_VEGETATION']:
    arcpy.SetProgressorLabel('Reclassifying Exisitng Vegetation Points...')
    # Change Unclassified and Unassigned Point Codes
    change_codes_arg = ['0 30 NO_CHANGE NO_CHANGE NO_CHANGE NO_CHANGE;1 31 NO_CHANGE NO_CHANGE NO_CHANGE NO_CHANGE']
    change_codes_args = [[las_file] + change_codes_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ChangeLasClassCodes, change_codes_args)

    # Change Existing Vegetation Points to Unclassified
    change_codes_arg = [';'.join([f'{veg_code} 0 NO_CHANGE NO_CHANGE NO_CHANGE NO_CHANGE' for veg_code in exist_veg_codes])]
    change_codes_args = [[las_file] + change_codes_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ChangeLasClassCodes, change_codes_args)

    # Classify Existing Vegetation to Vegetation Levels by Height 
    height_arg = ['GROUND', height_classification, noise_method]
    height_args = [[las_file] + height_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ClassifyLasByHeight, height_args)

    # Change Back Unclassified and Unassigned Point Codes and Flag Noise Points
    change_codes_arg = ['30 0 NO_CHANGE NO_CHANGE NO_CHANGE NO_CHANGE;31 1 NO_CHANGE NO_CHANGE NO_CHANGE NO_CHANGE;7 7 NO_CHANGE NO_CHANGE SET NO_CHANGE;18 18 NO_CHANGE NO_CHANGE SET NO_CHANGE']
    change_codes_args = [[las_file] + change_codes_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ChangeLasClassCodes, change_codes_args)

# Classify Ground Points
if ground_method == 'AGGRESSIVE':
    arcpy.SetProgressorLabel(f'Classifying Ground Points: Standard Method...')
    classify_ground_arg = ['STANDARD', reuse_ground, dem_resolution]
    classify_ground_args = [[las_file] + classify_ground_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ClassifyLasGround, classify_ground_args)

if ground_method:
    arcpy.SetProgressorLabel(f'Classifying Ground Points: {ground_method.capitalize()} Method...')
    classify_ground_arg = [ground_method, reuse_ground, dem_resolution]
    classify_ground_args = [[las_file] + classify_ground_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ClassifyLasGround, classify_ground_args)

# Classify Points by Features
if feature_classes:
    # Iterate through each feature class
    for row in feature_classes:
        # Split the row by quotes and spaces
        row_split = shlex.split(row)
        # Create Describe object of feature class
        fc_desc = arcpy.Describe(row_split[0])
        # Convert feature layers to catalog path and split feature classes from class code
        feature_class_arg = [f"'{fc_desc.catalogPath}' {' '.join(row_split[1:6])}"]
        # Create class code argument
        class_code_arg = [row_split[7]]
        classify_features_args = [[las_file] + feature_class_arg + class_code_arg + [compute_stats] for las_file in input_las_files]
        # Classify Points by Feature
        arcpy.SetProgressorLabel(f'Classifying Points by {fc_desc.baseName} Features...')
        with Pool(processes=cpu_ct) as pool:
            results = pool.starmap(lidarlas.SetLasClassCodesUsingFeaturesFiltered, classify_features_args)

# Classify Noise Points
# Classify High Noise Points
if noise_height:
    arcpy.SetProgressorLabel('Classifying High Noise Points...')
    height_arg = ['GROUND', f'1 {noise_height}', 'HIGH_NOISE']
    height_args = [[las_file] + height_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ClassifyLasByHeight, height_args)

# Classify Other Noise Points
if noise_method == 'ALL_NOISE':
    arcpy.SetProgressorLabel('Classifying Other Noise Points...')
    noise_arg = ['ISOLATION', 'CLASSIFY', 'WITHHELD', None, None, None, max_neighbors, step_width, step_height, None, compute_stats, extent, process_entire_files]
    noise_args = [[las_file] + noise_arg for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ClassifyLasNoise, noise_args)

# Classify Building Points (Not Multiprocessing; it's fast)
if classify_buildings:
    arcpy.SetProgressorLabel('Classifying Building Points...')
    arcpy.ddd.ClassifyLasBuilding(lasd_temp, min_roof_height, min_building_area, compute_stats, extent, boundary, process_entire_files, None, classify_buildings, photogrammetric_data)

# Classify New Vegetation Points
if vegetation_method in ['NEW_VEGETATION', 'ALL_VEGETATION']:
    arcpy.SetProgressorLabel('Classifying New Vegetation Points...')
    # Classify Unclassified and Unassigned Points to Vegetation Levels
    height_arg = ['GROUND', height_classification, noise_method]
    height_args = [[las_file] + height_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ClassifyLasByHeight, height_args)

# Refine Vegetation and Building Areas
if classify_buildings and vegetation_cell_size:
    arcpy.SetProgressorLabel('Refining Building and Vegetation Points...')
    # Create All, Vegetation, and Building Points Count Rasters
    arcpy.SetProgressorLabel('Refining Building and Vegetation Points: Counting Points...')
    all_point_count = arcpy.CreateUniqueName(os.path.join(scratch_folder, 'All_Point_Count.tif'))
    veg_codes = '3;4;5'
    veg_point_count = arcpy.CreateUniqueName(os.path.join(scratch_folder, 'Veg_Point_Count.tif'))
    building_codes = '6'
    building_point_count = arcpy.CreateUniqueName(os.path.join(scratch_folder, 'Building_Point_Count.tif'))

    las_stats_args = [[lasd_temp, all_point_count, 'POINT_COUNT', 'CELLSIZE', vegetation_cell_size], [lasd_temp, veg_point_count, 'POINT_COUNT', 'CELLSIZE', vegetation_cell_size, veg_codes], [lasd_temp, building_point_count, 'POINT_COUNT', 'CELLSIZE', vegetation_cell_size, building_codes]]
    
    with Pool(processes=4) as pool:
        results = pool.starmap(lidarlas.LasPointStatsAsRaster, las_stats_args)

    # Reclassify and Smooth All, Vegetation, and Building Points Count Rasters
    arcpy.SetProgressorLabel('Refining Building and Vegetation Points: Smoothing Rasters...')

    all_point_count_smooth = arcpy.CreateUniqueName(os.path.join(scratch_folder, 'All_Point_Count_Smooth.tif'))
    veg_point_count_smooth = arcpy.CreateUniqueName(os.path.join(scratch_folder, 'Veg_Point_Count_Smooth.tif'))
    building_point_count_smooth = arcpy.CreateUniqueName(os.path.join(scratch_folder, 'Building_Point_Count_Smooth.tif'))

    reclass_smooth_args = [[all_point_count, all_point_count_smooth], [veg_point_count, veg_point_count_smooth], [building_point_count, building_point_count_smooth]]
    
    with Pool(processes=4) as pool:
        results = pool.starmap(lidarlas.ReclassSmooth, reclass_smooth_args)

    # Calcaulte Vegetated Weighted Density (Not Multiprocessing)
    arcpy.SetProgressorLabel('Refining Building and Vegetation Points: Calcaulting Density...')
    veg_density = arcpy.sa.Minus(arcpy.sa.Divide(veg_point_count_smooth, all_point_count_smooth), arcpy.sa.Divide(building_point_count_smooth, all_point_count_smooth))
    veg_density.save(arcpy.CreateUniqueName(os.path.join(scratch_gdb, 'Veg_Density_Weighted')))

    # Create Vegetated Areas (Not Multiprocessing)
    arcpy.SetProgressorLabel('Refining Building and Vegetation Points: Creating Vegetated Areas...')
    veg_countours = arcpy.CreateUniqueName(os.path.join(scratch_gdb, 'Veg_Contours'))
    # Create Contour Polygons of Vegetated Weighted Density
    arcpy.sa.Contour(veg_density, veg_countours, 2, 0.25, 1, 'CONTOUR_POLYGON', None)
    # Select Vegetated Areas
    veg_areas = arcpy.CreateUniqueName(os.path.join(scratch_gdb, 'Vegetated_Areas'))
    arcpy.analysis.Select(veg_countours, veg_areas, 'ContourMax > 0.25')

    # Classify Building Points as Unclassified by Vegetated Area Features
    arcpy.SetProgressorLabel('Refining Building and Vegetation Points: Reclassifying Building Points in Vegetated Areas...')
    classify_features_arg = [f'{veg_areas} 0 0 NO_CHANGE NO_CHANGE NO_CHANGE NO_CHANGE', building_codes, compute_stats]
    classify_features_args = [[las_file] + classify_features_arg for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.SetLasClassCodesUsingFeaturesFiltered, classify_features_args)

    # Classify Unclassified and Unassigned Points to Vegetation Levels
    arcpy.SetProgressorLabel('Refining Building and Vegetation Points: Building Points to Vegetation...')
    height_arg = ['GROUND', height_classification, noise_method]
    height_args = [[las_file] + height_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ClassifyLasByHeight, height_args)

# Flag Noise Points as Withheld
if noise_method:
    arcpy.SetProgressorLabel('Flagging Noise Points as Withheld...')
    change_codes_arg = ['7 7 NO_CHANGE NO_CHANGE SET NO_CHANGE;18 18 NO_CHANGE NO_CHANGE SET NO_CHANGE']
    change_codes_args = [[las_file] + change_codes_arg + global_args for las_file in input_las_files]
    with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ChangeLasClassCodes, change_codes_args)

arcpy.SetProgressorLabel('Changing Never Classified Points to Unassigned...')
change_codes_arg = ['0 1 NO_CHANGE NO_CHANGE SET NO_CHANGE']
change_codes_args = [[las_file] + change_codes_arg + global_args for las_file in input_las_files]
with Pool(processes=cpu_ct) as pool:
        results = pool.starmap(lidarlas.ChangeLasClassCodes, change_codes_args)

# Create Output LAS Dataset
if out_las_dataset:
    arcpy.management.CreateLasDataset(input_files, out_las_dataset, folder_recursion, None, input_las_sr, compute_stats, 'RELATIVE_PATHS', 'NO_FILES')

# Clean up
folder_walk = arcpy.da.Walk(scratch_folder)
gdb_walk = arcpy.da.Walk(scratch_gdb)
clean_up = [os.path.join(dirpath, filename) for dirpath, dirnames, filenames in folder_walk for filename in filenames] + [os.path.join(dirpath, filename) for dirpath, dirnames, filenames in gdb_walk for filename in filenames]
for dataset in clean_up:
    arcpy.management.Delete(dataset)

winsound.PlaySound(r'C:\Windows\media\Ring08.wav', winsound.SND_FILENAME)