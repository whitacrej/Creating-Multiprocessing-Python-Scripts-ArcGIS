import arcpy
import os

class ToolValidator(object):
    """Class for validating a tool's parameter values and controlling
    the behavior of the tool's dialog."""

    def __init__(self):
        """Setup arcpy and the list of tool parameters.""" 
        self.params = arcpy.GetParameterInfo()

    def initializeParameters(self):
        """Refine the properties of a tool's parameters. This method is 
        called when the tool is opened."""
        # Include Subfolders: Disable at initialization
        self.params[1].enabled = False
        # Ground Classification Options: Disable options at initialization
        self.params[7].enabled = False
        self.params[8].enabled = False
        # Building Classification Options: Disable options at initialization
        self.params[10].enabled = False
        self.params[11].enabled = False
        self.params[12].enabled = False
        # Vegetation Classification Options
        self.params[14].enabled = False
        self.params[15].enabled = False
        self.params[16].enabled = False
        # Noise Classification Options
        self.params[19].enabled = False
        self.params[20].enabled = False
        self.params[21].enabled = False
        self.params[21].enabled = False

    def updateParameters(self):
        """Modify the values and properties of parameters before internal
        validation is performed. This method is called whenever a parameter
        has been changed."""

        if self.params[0].value:
            # Include Subfolders: Enable if a folder is in Input LAS Files
            input_types = [arcpy.Describe(i).dataType for i in self.params[0].valueAsText.split(';')]
            if 'Folder' in input_types:
                self.params[1].enabled = True
            else:
                self.params[1].enabled = False

            # Create Describe object for first value in Input Files
            input_las = self.params[0].valueAsText.split(';')[0]
            if arcpy.Describe(input_las).dataType == 'Folder':
                if self.params[1].valueAsText == 'NO_RECURSION' or not self.params[1].value:
                    las_files = [child.catalogPath for child in arcpy.Describe(input_las).children if child.dataType == 'LasDataset']
                else:
                    walk = arcpy.da.Walk(input_las, datatype='LasDataset')
                    las_files = [os.path.join(dirpath, filename) for dirpath, dirnames, filenames in walk for filename in filenames if os.path.splitext(filename)[1] == '.las']
                if las_files:
                    input_las = las_files[0]
                else:
                    input_las = None
            # LAS File(s) Coordinate System: Add coordinates system from first LAS file or first LAS file in Folder    
            if input_las and arcpy.Describe(input_las).spatialReference:
                in_sr = arcpy.Describe(input_las).spatialReference
            if not self.params[2].altered:
                self.params[2].value = in_sr
        
            # Ground Classification Options: Enable if Ground Detection Method is not empty
            if self.params[6].value:
                self.params[7].enabled = True
                self.params[8].enabled = True
            else:
                self.params[7].enabled = False
                self.params[8].enabled = False

            # Building Classification Options: Enable if Classify Building Points is not empty
            if self.params[9].value:
                self.params[10].enabled = True
                self.params[11].enabled = True
                self.params[12].enabled = True
            else:
                self.params[10].enabled = False
                self.params[11].enabled = False
                self.params[12].enabled = False

            # Determine Coordinate System Units
            if self.params[2].value:
                in_sr = self.params[2].value
                if in_sr.type == 'Projected':
                    if (in_sr.VCS and in_sr.VCS.linearUnitName in ['Meter']) or in_sr.linearUnitName in ['Meter']:
                        linear_unit = 'Meters'
                        area_unit = 'Square Meters'
                    elif (in_sr.VCS and in_sr.VCS.linearUnitName in ['Foot', 'Foot_US']) or in_sr.linearUnitName in ['Foot', 'Foot_US']:
                        linear_unit = 'Feet'
                        area_unit = 'Square Feet'

            # Minimum Rooftop Height: Set default value of 2 Meters (6.562 Feet) based on input spatial reference
            if self.params[10].enabled == True and not self.params[10].altered:
                if linear_unit == 'Meters':
                    self.params[10].value = '2 Meters'
                elif linear_unit == 'Feet':
                    self.params[10].value = '6.562 Feet'
            
            # Minimum Building Area: Set default value of 10 Square Meters (107.639 Square Feet) based on input spatial reference
            if self.params[11].enabled == True and not self.params[11].altered:
                if area_unit == 'Square Meters':
                    self.params[11].value = '10 SquareMeters'
                elif area_unit == 'Square Feet':
                    self.params[11].value = '107.639 SquareFeet'
            
            # Vegetation Validation
            # Exisiting Vegetation Classification Codes: Enable if Vegetation Classification Method is EXISTING_VEGETATION or ALL_VEGETATION
            if self.params[13].valueAsText in ['EXISTING_VEGETATION', 'ALL_VEGETATION']:
                self.params[14].enabled = True
            else:
                self.params[14].enabled = False

            # Height Classification: Enable if Vegetation Classification Method is not empty, Set default values based on input spatial reference unit of '1 0.5;3 2;4 5;5 65' for meter and '1 1.64;3 6.562;4 16.404;5 213.255' for feet
            #  Vegetation Area Cell Size: Enable if Vegetation Classification Method is not empty
            if self.params[13].value:
                self.params[15].enabled = True
                self.params[16].enabled = True
                if self.params[2].value and not self.params[15].altered:
                    if linear_unit == 'Meters':
                        self.params[15].value = '1 0.5;3 2;4 5;5 65'
                    elif linear_unit == 'Feet':
                        self.params[15].value = '1 1.64;3 6.562;4 16.404;5 213.255'
                if self.params[2].value and not self.params[16].altered:
                    if linear_unit == 'Meters':
                        self.params[16].value = 5
                    elif linear_unit == 'Feet':
                        self.params[16].value = 16.404
            else:
                self.params[15].enabled = False
                self.params[16].enabled = False

            # Noise Height From Ground: Enable if Noise Classification is ALL_NOISE or HIGH_NOISE, Set default value based on input spatial reference unit of 65 for meter or 213.255 for feet
            if self.params[17].valueAsText in ['ALL_NOISE', 'HIGH_NOISE']:
                self.params[18].enabled = True
                if self.params[2].value and not self.params[18].altered:
                    if linear_unit == 'Meters':
                        self.params[18].value = 65
                    elif linear_unit == 'Feet':
                        self.params[18].value = 213.255
            else:
                self.params[18].enabled = False

            # Noise Validation
            if self.params[17].valueAsText == 'ALL_NOISE':
                self.params[19].enabled = True
                self.params[20].enabled = True
                self.params[21].enabled = True
                # Noise Neighborhood Point Limit: Enable if Noise Classification is ALL_NOISE, Set default value as 25
                if not self.params[19].altered:
                    self.params[19].value = 25
                # Noise Noise Neighborhood Width and Height: Enable if Noise Classification is ALL_NOISE, Set default value based on input spatial reference unit of 10 Meters or 32.808 for feet
                if self.params[2].value and not self.params[20].altered and not self.params[21].altered:
                    if linear_unit == 'Meters':
                        self.params[20].value = self.params[21].value = '10 Meters'
                    elif linear_unit == 'Feet':
                        self.params[20].value = self.params[21].value = '32.808 Feet'
            else:
                self.params[19].enabled = False
                self.params[20].enabled = False
                self.params[21].enabled = False

    def updateMessages(self):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        # LAS Class Codes: Create Error Message if files are not LAS files or no LAS files exist in the input folders
        if self.params[0].value:
            input_files = self.params[0].valueAsText.split(';')
            for i in input_files:
                i_desc = arcpy.Describe(i)
                if i_desc.dataType == 'Folder':
                    if self.params[1].valueAsText == 'NO_RECURSION' or not self.params[1].value:
                        las_files = [child.catalogPath for child in i_desc.children if child.dataType == 'LasDataset']
                    else:
                        walk = arcpy.da.Walk(i, datatype='LasDataset')
                        las_files = [os.path.join(dirpath, filename) for dirpath, dirnames, filenames in walk for filename in filenames]
                    if not las_files:
                        self.params[0].setErrorMessage(f'Folder {i} does not contain LAS files. Remove folder from Input Files.')
                elif i_desc.dataType != 'LasDataset':
                    self.params[0].setIDMessage('ERROR', 814)

        # Classification Feature Classes: Create Error Message if Class Code Filter contians text other than integer between 0 and 255 or a comma
        if self.params[5].value:
            class_code_filters = [row[-1] for row in self.params[5].value]
            classify_features_error = []
            for f in class_code_filters:
                # Check for non-integers
                if f and not all(i.strip().isdigit() for i in f.split(',')):
                    classify_features_error.append(f"Class Code Filter '{f}' must contain all integers delimited by a comma (,).")
                # Check for integers between 0 and 255
                elif f and not all(0 <= int(i.strip()) <= 255 for i in f.split(',')):
                    classify_features_error.append(f"Class Code Filter '{f}' must contain all integers less than 255.")
            # Add error message if errors exist
            if classify_features_error:
                self.params[5].setErrorMessage('\n'.join(classify_features_error))

        # Value Required Messages
        value_required = 'Value required.'
        # Minimum Rooftop Height: Create Error Message if Minimum Rooftop Height is enabled and empty
        if self.params[10].enabled and not self.params[10].value:
            self.params[10].setErrorMessage(value_required)

        # Minimum Building Area: Create Error Message if Minimum Building Area is enabled and empty
        if self.params[11].enabled and not self.params[11].value:
            self.params[11].setErrorMessage(value_required)

        # Exisiting Vegetation Classification Codes: Create Error Message if Exisiting Vegetation Classification Codes is enabled and empty
        if self.params[14].enabled and not self.params[14].value:
            self.params[14].setErrorMessage(value_required)

        # Height Classification: Create Error Message if Height Classification is enabled and empty
        if self.params[15].enabled and not self.params[15].value:
            self.params[15].setErrorMessage(value_required)
        
        #  Vegetation Area Cell Size: Create Error Message if Vegetation Area Cell Size is enabled and empty if Classify Building Points is not empty or Output Vegetation Areas Feature Class is not empty
        if self.params[16].enabled and not self.params[16].value and self.params[9].value:
            self.params[16].setErrorMessage(value_required)

        # Noise Neighborhood Width: Create Error Message if Noise Height From Ground Limit is enabled and empty
        if self.params[18].enabled and not self.params[18].value:
            self.params[18].setErrorMessage(value_required)
        
        # Noise Neighborhood Point Limit: Create Error Message if Noise Neighborhood Point Limit is enabled and empty
        if self.params[19].enabled and not self.params[19].value:
            self.params[19].setErrorMessage(value_required)
        
        # Noise Neighborhood Width: Create Error Message if Noise Neighborhood Width is enabled and empty
        if self.params[20].enabled and not self.params[20].value:
            self.params[20].setErrorMessage(value_required)
        
        # Noise Neighborhood Width: Create Error Message if Noise Neighborhood Width is enabled and empty
        if self.params[21].enabled and not self.params[21].value:
            self.params[21].setErrorMessage(value_required)

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        # Check if 3D and Spatial Analyst licenses are available
        try:
            if arcpy.CheckExtension('3D') != 'Available' or arcpy.CheckExtension('Spatial') != 'Available':
                raise Exception
        except Exception:
            return False  # tool cannot be executed
        return True
