#!/biometrics/global/gpythonvenv/bin/python3
# -*- coding: utf-8 -*-

##############################################################################
#Program:		latestDate function
#Coded on: 		12JAN2021 by AJ Johansson 
#Description:	sorts by date and removes the newest from the list
#							
##############################################################################
import m_dart as dart
import pandas as pd


dart.load(pathin='../vdata/ae_colormap.sas7bdat',
            keyvars=['SUBJID','INVID','SCRNID','AETERM','AESTDAT','AEENDAT'],
            listlabel='COLORMAP TEST 01' ,
            listtitle='COLORMAP TEST 01')
