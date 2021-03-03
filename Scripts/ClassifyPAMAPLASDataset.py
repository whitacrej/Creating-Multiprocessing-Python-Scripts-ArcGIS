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
# Classification Feature Classes (Value Table: Features > Feature Layer | Buffer Distance > Double | New Class > Long | Synthetic > String | KeyPoint > String | Withheld > String | Overlap > String | Class Code Filter > String; Optional; Filter: Feature > Point, Polyline, Polygon | New Class > Range 0 - 255 | Synthetic, KeyPoint, Withheld, Overlap > NO_CHANGE, SET, CLEAR; Validation: Create Error Message if Class Code Filter contians text other than integer between 0 and 255 or a comma)
feature_classes = arcpy.GetParameterAsText(4).split(';')

# Group: Ground Classification Options
# Ground Detection Method (String; Optional; Filter: STANDARD, CONSERVATIVE, AGGRESSIVE)
ground_method = arcpy.GetParameterAsText(5)
# Reuse Existing Ground (Boolean; Optional; Filter: True = REUSE_GROUND, False = RECLASSIFY_GROUND; Default: true; Validation: Enable if Ground Detection Method is not empty)
reuse_ground = arcpy.GetParameterAsText(6)
# DEM Resolution (Linear Unit; Optional; Validation: Enable if Ground Detection Method is not empty)
dem_resolution = arcpy.GetParameterAsText(7)

# Group: Processing Extent
# Processing Extent (Extent; Optional; Default: DEFAULT)
extent = arcpy.GetParameterAsText(8)
# Processing Boundary (Feature Set; Optional)
boundary = arcpy.GetParameterAsText(9)
# Process Entire LAS Files That Intersect Extent (Boolean; Optional; Filter: True = PROCESS_ENTIRE_FILES, False = PROCESS_EXTENT )
process_entire_files = arcpy.GetParameterAsText(10)

# Group: Output Options
# Output LAS Dataset (LAS Dataset; Optional; Output)
out_las_dataset = arcpy.GetParameterAsText(11)


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