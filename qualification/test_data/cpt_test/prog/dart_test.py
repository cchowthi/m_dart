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


dart.load(pathin='../output/GS-US-412-5624_General Edit Checks_12_07_2021_11_18.xlsx',
            keyvars=['Subject','Inv ID','Scrn ID','Dataset'],
            cpt_merge_key=['Subject','Inv ID','Scrn ID'],
            listlabel='CPT TEST 01' ,
            listtitle='CPT TEST 01')

quit()
df = pd.read_pickle("ae_cm_mh_1582.pkl")  
                
status = dart.load(
    dsnin=df,
    listlabel="Vinh Test 3",
    listtitle="Vinh Test 3",
    keyvars=['Investigator ID', 'Screen ID', 'Subject ID', 'Event Form', 'Event Position', 'Event', 'Medication', 'Flag'],
    cpt_merge_key=['Investigator ID', 'Screen ID', 'Subject ID']
)



