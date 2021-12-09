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

# =============================================================================
# Define tests below
# =============================================================================
@pytest.mark.parametrize('fix_test',
                          [['path/to/file1',
                           'path/to/file2']],
                          indirect=True)
def test_ur1(fix_test):

                          [['test_ur1.stdout','/path/to/file2']], # (1) files to remove during setup/teardown
                          indirect=True)    
def test_ur1(fix_test):
	
    """ User Requirement 1: 
    Description here  """ # (2) User Requirement Description
      
    os.chdir(r'/path/to/execute/command/from') # (3) Directory to execute command from
    
    cmd='../../../../../../m-script-name -args' # (4) Command

    puts.check_output_to_file(cmd, "test_ur1.stdout") 
    
    assert puts.puts_diff_pdf('output.pdf','../../../../../expected_results/expected.pdf') # (5) Test assertion(s)
