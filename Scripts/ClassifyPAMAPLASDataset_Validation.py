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
        self.params[6].enabled = False
        self.params[7].enabled = False

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
            if self.params[5].value:
                self.params[6].enabled = True
                self.params[7].enabled = True
            else:
                self.params[6].enabled = False
                self.params[7].enabled = False

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

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        # Check if 3D and Spatial Analyst licenses are available
        try:
            if arcpy.CheckExtension('3D') != 'Available' or arcpy.CheckExtension('Spatial') != 'Available':
                raise Exception
        except Exception:
            return False  # tool cannot be executed
        return True
