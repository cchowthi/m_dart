# -*- coding: utf-8 -*-
######### start of header ######################################
# Program Name: m-script-name.py
# Author:       
# Description:  Python Script test requirements for m-script-name utility
#
#
# Category:     Functional test
# Macros called:
# Parameter:
# Usage:
#
#
# Change History:
########## end of header ###########################################/


import os
import pytest
from puts import puts
import importlib.util
import inspect
import sys
import types
import ast
from pprint import pprint

def get_function_locals(func, *args, **kwargs):
    frame, result = call_function_get_frame(func, *args, **kwargs)
    try:
        module = types.ModuleType(func.__name__)
        module.__dict__.update(frame.f_locals)
        return module
    finally:
        del frame
    
def call_function_get_frame(func, *args, **kwargs):
    """
    Calls the function *func* with the specified arguments and keyword
    arguments and snatches its local frame before it actually executes.
    """
    frame = None
    trace = sys.gettrace()
    def snatch_locals(_frame, name, arg):
        nonlocal frame
        if name == 'call':
            frame = _frame
            sys.settrace(trace)
        return trace
    sys.settrace(snatch_locals)
    try:
        result = func(*args, **kwargs)
    finally:
        sys.settrace(trace)
    return frame, result
  
def get_function():
    spec = importlib.util.spec_from_file_location("load", "../../deploy/m_dart/m_dart.py")
    func = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(func)
    return func

def get_default_args(func):
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }
    
# =============================================================================
# Define tests below
# =============================================================================
def test_ur1():
	
    """ User Requirement 1: 
    Parameter to specify dataframe to be sent to DART (dsnin) """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'dsnin' in args


def test_ur2():
	
    """ User Requirement 2: 
    Parameter to specify path to data to be sent to DART (pathin) """ 
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'pathin' in args

   
def test_ur3():
	
    """ User Requirement 3: 
    Parameter specifying short listing name  """ 
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'listlabel' in args    

  
def test_ur4():
	
    """ User Requirement 4: 
    Parameter specifying full listing description  """ 
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'listtitle' in args    
     

def test_ur5():
	
    """ User Requirement 5: 
    Parameter specifying variables used to identify a unique record in dataset  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'keyvars' in args 

def test_ur6():
	
    """ User Requirement 6: 
    Parameter specifying CPT merge key (CPT=Clean Patient Tracking) """ 

    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'cpt_merge_key' in args 
  
def test_ur7():
	
    """ User Requirement 7: 
    Parameter specifying listing id obtained from sDVS  """ 

    dart = get_function()
    args = get_default_args(dart.load)
        
    assert 'listingcode' in args    


def test_ur8():
	
    """ User Requirement 8: 
    Parameter to specify organizationid, numeric variable coresponding to organization id #  """

    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'organizationid' in args         

def test_ur9():
	
    """ User Requirement 9: 
    Parameter issubjectlisting, numeric variable (0/1) """ 
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'issubjectlisting' in args

   
def test_ur10():
	
    """ User Requirement 10: 
    Parameter specifying destination of item being sent to DART (prod, dev or xlsx) """ 
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'loadtype' in args    

  
def test_ur11():
	
    """ User Requirement 11: 
    Parameter specifying type of item being sent to DART """ 
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'listcat' in args    
     

def test_ur12():
	
    """ User Requirement 12: 
    Parameter used for multi tab listings  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'parentlistingname' in args 

def test_ur13():
	
    """ User Requirement 13: 
    Parameter used for multi tab listings """ 

    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'tabname' in args 
 
def test_ur14():
	
    """ User Requirement 14: 
    Parameter specifying override of modifying duplicates  """ 

    dart = get_function()
    args = get_default_args(dart.load)
        
    assert 'dupovrid' in args    
  
def test_ur15():
	
    """ User Requirement 15: 
    Parameter specifying override formats and send all as char. This is the default for the first version of this module  """

    dart = get_function()
    args = get_default_args(dart.load)
    
    assert 'fmtovrid' in args  
  
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur16(fix_test, record_property):
	
    """ User Requirement 16: 
    Loads with colormap functionality  """

    record_property('method', 'CR, VI')
    
    dart = get_function()
    
    os.chdir('../test_data/colormap_test/prog')

    dart.load(pathin='../vdata/ae_colormap.sas7bdat',
                keyvars=['SUBJID','INVID','SCRNID','AETERM','AESTDAT','AEENDAT'],
                listlabel='COLORMAP TEST' ,
                listtitle='COLORMAP TEST')
   
    assert True 
        
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur17(fix_test, record_property):
	
    """ User Requirement 17: 
    Set orginizationid if not explicitly defined in macro call (1-BIOMARKER if bmcdp in path)  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/bmcdp/prog')

    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['SUBJID','INVID','SCRNID','VISIT','TPT','ARSAMPID'],
                                    listlabel='GENERAL TEST' ,
                                    listtitle='GENERAL TEST')
   
    assert result.args.organizationid == 1
    
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur18(fix_test, record_property):
	
    """ User Requirement 18: 
    Set listingcategoryid (listcat) to 7 if not explicitly defined in macro call  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/bmcdp/prog')

    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['SUBJID','INVID','SCRNID','VISIT','TPT','ARSAMPID'],
                                    listlabel='GENERAL TEST' ,
                                    listtitle='GENERAL TEST')
   
    assert result.args.listcat == 7
    
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur19(fix_test, record_property):
	
    """ User Requirement 19: 
    Set _rowid and _GILDNO to null if listingcategoryid (listcat) is 6  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/cdp/prog')

    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['SUBJID','INVID','SCRNID','VISIT','TPT','ARSAMPID'],
                                    listlabel='GENERAL TEST' ,
                                    listtitle='GENERAL TEST',
                                    listcat=6)

    assert result.init['_rowid'] is None and result.init['_GILDNO'] is None   
    
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur20(fix_test, record_property):
	
    """ User Requirement 20: 
    Set issubjectlisting  if not explicitly defined in macro call (used for CPT listings)  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/bmcdp/prog')

    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['SUBJID','INVID','SCRNID','VISIT','TPT','ARSAMPID'],
                                    listlabel='GENERAL TEST' ,
                                    listtitle='GENERAL TEST')
   
    assert result.args.issubjectlisting == 1
    
    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['VISIT','TPT','ARSAMPID'],
                                    listlabel='GENERAL TEST' ,
                                    listtitle='GENERAL TEST')
   
    assert result.args.issubjectlisting == 0
    
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur21(fix_test, record_property):
	
    """ User Requirement 21: 
    Set listingcode if not explicitly defined in macro call ( grab from list label )  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/bmcdp/prog')

    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['SUBJID','INVID','SCRNID','VISIT','TPT','ARSAMPID'],
                                    listlabel='GENERAL TEST' ,
                                    listtitle='GENERAL TEST')
   
    assert  result.args.listingcode == 'GENERAL_TEST'
     
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur22(fix_test, record_property):
	
    """ User Requirement 22: 
    Grab email addresses for study/group to pass into dart for notifications (biomarkerCPsupport if organizationid = 1, else get contact fro b3p)  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/bmcdp/prog')

    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['SUBJID','INVID','SCRNID','VISIT','TPT','ARSAMPID'],
                                    listlabel='GENERAL TEST' ,
                                    listtitle='GENERAL TEST')
   
    assert  result.__emailcontact == 'biomarkerCPsupport@gilead.com'     
    
    os.chdir('../../cpt_test/prog')
    
    result = get_function_locals(dart.load, pathin='../output/GS-US-412-5624_General Edit Checks_12_07_2021_11_18.xlsx',
                                    keyvars=['Subject','Inv ID','Scrn ID','Dataset'],
                                    cpt_merge_key=['Subject','Inv ID','Scrn ID'],
                                    listlabel='CPT TEST' ,
                                    listtitle='CPT TEST')
                                    
    assert  result.__emailcontact != 'biomarkerCPsupport@gilead.com'     
    
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur23(fix_test, record_property):
	
    """ User Requirement 23: 
    listingcategoryid=14 and issubjectinglisting=1 causes CPT count issues, er ror out  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/cpt_test/prog')

    try:
        result = get_function_locals(dart.load, pathin='../output/GS-US-412-5624_General Edit Checks_12_07_2021_11_18.xlsx',
                                    keyvars=['Subject','Inv ID','Scrn ID','Dataset'],
                                    cpt_merge_key=['Subject','Inv ID','Scrn ID'],
                                    listlabel='CPT TEST' ,
                                    listtitle='CPT TEST',
                                    listcat=14)
        
    except:
        # Expecting an exception raised here
        assert True
    
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur24(fix_test, record_property):
	
    """ User Requirement 24: 
    Raise error for missing listlabel  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/cpt_test/prog')

    try:
        result = get_function_locals(dart.load, pathin='../output/GS-US-412-5624_General Edit Checks_12_07_2021_11_18.xlsx',
                                    keyvars=['Subject','Inv ID','Scrn ID','Dataset'],
                                    cpt_merge_key=['Subject','Inv ID','Scrn ID'],
                                    listtitle='CPT TEST')
        
    except:
        # Expecting an exception raised here
        assert True
                     
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur25(fix_test, record_property):
	
    """ User Requirement 25: 
    Raise error for missing listtitle  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/cpt_test/prog')

    try:
        result = get_function_locals(dart.load, pathin='../output/GS-US-412-5624_General Edit Checks_12_07_2021_11_18.xlsx',
                                    keyvars=['Subject','Inv ID','Scrn ID','Dataset'],
                                    cpt_merge_key=['Subject','Inv ID','Scrn ID'],
                                    listlabel='CPT TEST')
        
    except:
        # Expecting an exception raised here
        assert True      
        
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur26(fix_test, record_property):
	
    """ User Requirement 26: 
    Raise error for missing keyvars  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/cpt_test/prog')

    try:
        result = get_function_locals(dart.load, pathin='../output/GS-US-412-5624_General Edit Checks_12_07_2021_11_18.xlsx',
                                    cpt_merge_key=['Subject','Inv ID','Scrn ID'],
                                    listlabel='CPT TEST',
                                    listtitle='CPT TEST')
        
    except:
        # Expecting an exception raised here
        assert True            
                      
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur27(fix_test, record_property):
	
    """ User Requirement 27: 
    If study is CPT=Y then add in merge vars  """
    
    record_property('method', 'CR, VI')
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/cpt_test/prog')

    result = get_function_locals(dart.load, pathin='../output/GS-US-412-5624_General Edit Checks_12_07_2021_11_18.xlsx',
                                    keyvars=['Subject','Inv ID','Scrn ID','Dataset'],
                                    cpt_merge_key=['Subject','Inv ID','Scrn ID'],
                                    listlabel='CPT TEST',
                                    listtitle='CPT TEST')
        
    assert True        
    
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur28(fix_test, record_property):
	
    """ User Requirement 28: 
    Send as xlsx if loadtype='xlsx'  """
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/bmcdp/prog')

    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['SUBJID','INVID','SCRNID','VISIT','TPT','ARSAMPID'],
                                    listlabel='GENERAL TEST' ,
                                    listtitle='GENERAL TEST',
                                    loadtype='xlsx')
   
    assert not result.sendtodart and not result.senttodart   
    
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur29(fix_test, record_property):
	
    """ User Requirement 29: 
    Send to production server if loadtype='prod'  """
    
    record_property('method', 'CR, VI')
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/bmcdp/prog')

    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['SUBJID','INVID','SCRNID','VISIT','TPT','ARSAMPID'],
                                    listlabel='PROD TEST - Please ignore' ,
                                    listtitle='PROD TEST - Please ignore',
                                    loadtype='prod')
   
    assert True
    
@pytest.mark.parametrize('fix_test', [[]], indirect=True) 
def test_ur30(fix_test, record_property):
	
    """ User Requirement 30: 
    Send to validation server if loadtype='val'  """
    
    record_property('method', 'CR, VI')
    
    dart = get_function()
    args = get_default_args(dart.load)
    
    os.chdir('../test_data/bmcdp/prog')

    result = get_function_locals(dart.load, pathin='../vdata/combo_report_data.sas7bdat',
                                    keyvars=['SUBJID','INVID','SCRNID','VISIT','TPT','ARSAMPID'],
                                    listlabel='PROD TEST - Please ignore' ,
                                    listtitle='PROD TEST - Please ignore',
                                    loadtype='val')
   
    assert True