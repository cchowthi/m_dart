# -*- coding: utf-8 -*-
######### start of header ######################################
# Program Name: m-dart.py
# Author:       Christal Chowthi (cchowthi)
# Description:  Send reports to dart
# 
#
# Category:     
# Macros called: 
# Parameter:                
# Usage:         
#                                   
#                                                                       
# Change History: 
#       2021-12-15  cchowthi    Original Programming
#
########## end of header ###########################################/

import m_passw
import pandas as pd
import numpy as np
import argparse
import sys
import re 
import os
import datetime
import json
import pyodbc
import m_config_cc as m_config
import pathlib

# Define colors
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class collection(object):    
    def __init__(self, my_dict):         
        for key in my_dict:
            setattr(self, key, my_dict[key])
                                
def load(   dsnin=None,             #Dataframe to load            
            pathin='',              #Path to dataset including extension if not passing dataframe
            listlabel='',           #Quick label of listing, will be displayed in dart
            listtitle='',           #Longer title description of listing, displayed in dart
            keyvars=[],             #list of variables (space delimieted) in input dataset that create unique sort without duplicates, used for comment merging in dart                                     
            cpt_merge_key=[],       #merge key variables for CPT, also indicates that the listing is a subject listing
            listingcode='',         #code provided from sDVS', default='')
            organizationid=None,    #numeric code for describing org: N/A for CP, 1 for Biomarker, 2 for Safety
            issubjectlisting=None,  #Subject related listing
            loadtype='dev',         #Load to prod, dev, or xlsx sheet
            listcat=7,              #Listing category
            parentlistingname=None, #Used for multitab listings, references parent listing
            tabname=None,           #Used for multitab listings, tabname to dispaly for sheet in dart
            dupovrid=False,         #Override duplicate changing of records
            fmtovrid=True,          #Override formats (send all to dart as char with the exception of )
            ):
    # =============================================================================
    # Load Parameters 
    # =============================================================================           
    sendtodart = True
    senttodart = False
    args = collection(locals())

    if args.pathin:
        if '/' in args.pathin:
            __dsdir = re.sub(r'^(.*?\/?)([^\/]+)\.([^\/\.]+$)',r'\1',args.pathin)
            __dsname = re.sub(r'^(.*?\/?)([^\/]+)\.([^\/\.]+$)',r'\2',args.pathin)
            __dstyp = re.sub(r'^(.*?\/?)([^\/]+)\.([^\/\.]+$)',r'\3',args.pathin)
        else:
            __dsdir = os.getcwd()
            __dsname = re.sub(r'([^\/]+)\.([^\/\.]+$)',r'\1',args.pathin)
            __dstyp = re.sub(r'([^\/]+)\.([^\/\.]+$)',r'\2',args.pathin)  

        __dspath = __dsdir + '/' + __dsname + '.' + __dstyp    
        # =========================================================================
        # Read in data to be loaded
        # =========================================================================
        if __dstyp == 'sas7bdat':    
            ds = pd.read_sas(__dspath, encoding='iso-8859-1')
        elif __dstyp == 'csv':
            ds = pd.read_csv(__dspath)
        elif __dstyp =='xls':
            ds = pd.read_excel(__dspath)
        elif __dstyp == 'xlsx':
            ds = pd.read_excel(__dspath, engine='openpyxl')
        else:
            raise Exception("Unsupported file type")   
    else:
        ds = args.dsnin        
    
    __b3pcont = "/biometrics/global/mgarea/data/b3pcont.sas7bdat"
    __b3pprot = "/biometrics/global/mgarea/data/b3pprot.sas7bdat"
    __emailcontact = ''
    
    init = m_config.return_init()
    __CUT_ID = None
           
    # =============================================================================
    # Business Checking to Ensure Neccessary Parameters are Populated/Populate Fields Based on Logic
    # =============================================================================        
    ### Sort Keys Alphabetically to match dart migration sort    
    args.keyvars.sort()

    ### Set orginizationid if not explicitly defined in macro call (CP,SAFETY,BIOMARKER) 
    if not args.organizationid:
        if 'BMCDP' in os.getcwd().upper():
            args.organizationid = 1
    else:
        args.organizationid = int(args.organizationid)
      
    ### Set listingcategoryid (listcat)  if not explicitly defined in macro call (special handling of listings (coding,echecks,etc.) 
    args.listcat = int(args.listcat)
    if args.listcat==6:
        init['_rowid'] = None
        init['_GILDNO'] = None

    ### Set issubjectlisting  if not explicitly defined in macro call (used for CPT listings) ***/    
    if args.issubjectlisting is None:
        if any(x in [y.upper() for y in args.keyvars] for x in ['SCRNID','SCRNNUM','SUBJID','SUBJECT_ID','SUBJECT','SCRN ID']):
            args.issubjectlisting=True
        else:
            args.issubjectlisting=False
    args.issubjectlisting = int(args.issubjectlisting)
    
    ### Set listingcode if not explicitly defined in macro call ( grab from list label )
    if args.listingcode == '':
        args.listingcode=args.listlabel.replace(' ','_')
    
    ### Grab email addresses for study/group to pass into dart for notifications 
    ### Get CPT status
    if args.organizationid == 1:
        ###Biomarker listings use group inbox as email addy 
        __emailcontact='biomarkerCPsupport@gilead.com'

    if not __emailcontact:
        df = pd.read_sas(__b3pcont, encoding='iso-8859-1')
        df = df[df['GILDA'] == init['_GILDNO']]
        if not df.empty:
            if not args.loadtype == 'dev':
                if args.organizationid != 1:
                    __emailcontact = df.iloc[0]['PCP'] + ',' + os.environ.get('USER') + '@gilead.com'
            else:
                __emailcontact = os.environ.get('USER') + '@gilead.com'
                
    ### Get CPT status
    df = pd.read_sas(__b3pprot, encoding='iso-8859-1')
    df = df[df['PROTOCOLNAME'] == init['_GILDNO']]
    if not df.empty:
        if int(df.iloc[0]['ISCPTSTUDY']) == 1:
            init['CPT'] = True
        else:
            init['CPT'] = False
         
    ### Additional Business Logic 
    ### listingcategoryid=14 and issubjectinglisting=1 causes CPT count issues, er ror out 
    if args.listcat==14 and args.issubjectlisting==1:
        raise Exception(bcolors.FAIL + "Parameter listcat ID is " + str(args.listcat) + " AND issubjectlisting is " + str(args.issubjectlisting) + ": CPT Issues will occur, no data sent to DART." + bcolors.ENDC)

    ### Raise error for missing parameters
    if args.listlabel == '':
        raise Exception(bcolors.FAIL + "Parameter LISTLABEL is blank: no data sent to DART." + bcolors.ENDC)
    if args.listtitle == '':
        raise Exception(bcolors.FAIL + "Parameter LISTTITLE is blank: no data sent to DART." + bcolors.ENDC)
    if args.keyvars == []:
        raise Exception(bcolors.FAIL + "Parameter KEYVARS is blank: no data sent to DART." + bcolors.ENDC)

    # =============================================================================
    # Setup CPT Variables and conditions
    # =============================================================================   
    ### Check if being run in CPT directory, used to load CPT data **/
    P = pathlib.Path(os.getcwd() + '/../rawdata_cpt')
    rawdata_cpt = str(P.resolve())
    if os.path.exists(rawdata_cpt):
        ### Set prefix to load CPT tables
        __prfix='C_'
        ### Load cut_id from dataset
        df = pd.read_sas(rawdata_cpt + '/vw_dataset_detail.sas7bdat', encoding='iso-8859-1')
        if df:
            if not df.empty:
                __CUT_ID = int(''.join(df['CUT_ID'].unique()))
                ds['datacutid'] = __CUT_ID

    ### If study is CPT=Y then add in merge vars
    if init['CPT']:
        if len(args.cpt_merge_key) > 0:
            _inv, _scrn, _subj, _oth = '', '', '', '' 
            for _v in args.cpt_merge_key:
                if _v.upper().replace(" ", "") in ['SITENUMBER', 'INVID', 'INVESTIGATORID']:
                    _inv = _v
                elif _v.upper().replace(" ", "") in ['SCRNID', 'SCRNNUM', 'SCREENID']:
                    _scrn = _v
                elif _v.upper().replace(" ", "") in ['SUBJID', 'SUBJECT_ID', 'SUBJECTID', 'SUBJECT']:
                    _subj = _v
                else:
                    _oth += _v + ' '
            _oth = [_ for _ in _oth.strip().split(' ')]
            mergev = list(filter(None, ['_y', '_z'] + _oth))
            keepv = [_inv, _scrn, _subj, 'SUBJECTID']
            P = pathlib.Path(os.getcwd() + '/../rawdata')
            rawdata = str(P.resolve())
            if os.path.exists(rawdata_cpt):
                df = pd.read_sas(rawdata+ '/dm.sas7bdat', encoding='iso-8859-1')
                df.sort_values(by=['INVID', 'SCRNID', 'SUBJID']).drop_duplicates(subset=['INVID', 'SCRNID', 'SUBJID'])
                df.rename({'INVID': _inv, 'SCRNID': _scrn, 'SUBJID': _subj}, axis=1, inplace=True)
                df = df[keepv]
                # Perform merge in pecking order (1. dm.subjid == input.subjid, 2. dm.invid == input.invid + dm.scrnid == input.scrnid)
                ds = pd.concat([pd.merge(ds[~ds[_subj].isnull()].assign(_x=ds[~ds[_subj].isnull()][_subj].astype(int)), 
                                         df[~df[_subj].isnull()].assign(_x=df[~df[_subj].isnull()][_subj].astype(int)), 
                                         on='_x', how='left', suffixes=('', '_x')), # Merge on subjid (where not missing subjid in input)
                                pd.merge(ds[ds[_subj].isnull()].assign(_y=ds[ds[_subj].isnull()][_inv].astype(int), _z=ds[ds[_subj].isnull()][_scrn].astype(int)), 
                                         df.assign(_y=df[_inv].astype(int), _z=df[_scrn].astype(int)), 
                                         on=mergev, how='left', suffixes=('', '_y'))]) # Merge on invid, scrnid (where missing subjid in input)

                ds.drop(ds.filter(regex='_[xyz]$').columns.tolist(),axis=1, inplace=True)
    else:
        __prfix='C_'
        __CUT_ID = None

    # =============================================================================
    # Check if listing is loading to PROD, DEV, OR XLSX Output
    # =============================================================================
    ### Check to see if study level macro var &g_testing exists, if it does use it
    if 'g_testing' in init:
        if not args.loadtype:
            if init['g_testing'] in [1, 2]:
                args.loadtype == 'prod'
            if init['g_testing'] == 3:
                ds.to_excel('./' + __dsname + '.xlsx', index=False)
                sendtodart = False
        if (args.loadtype == '' or args.loadtype == 'UAT') and init['g_testing'] == 4:
            curtim = datetime.datetime.now().strftime("%Y%m%d_%H:%M:%S")
            ds.to_pickle("./__dart_" + curtim + ".pkl")
            sendtodart = False  
    print()
    print('Preparing to load with the following parameters...')
    
    for key, val in vars(args).items():
        if key == 'dsnin' and val is not None:
            print('\t' + bcolors.HEADER + 'dsnin (preview):' + bcolors.ENDC + ':' + bcolors.UNDERLINE + ', '.join(val.columns.tolist()) + bcolors.ENDC)
        else:  
            print('\t' + bcolors.HEADER + key + bcolors.ENDC + ':' + bcolors.UNDERLINE + str(val) + bcolors.ENDC)
    print('\t' + bcolors.HEADER + 'emailcontact:' + bcolors.ENDC + ':' + bcolors.UNDERLINE + __emailcontact + bcolors.ENDC)
    print('\t' + bcolors.HEADER + 'rowid:' + bcolors.ENDC + ':' + bcolors.UNDERLINE + str(init['_rowid']) + bcolors.ENDC)
    print('\t' + bcolors.HEADER + 'gildno:' + bcolors.ENDC + ':' + bcolors.UNDERLINE + str(init['_GILDNO']) + bcolors.ENDC)
    print()
    
    # =============================================================================
    # Connect to DART Server
    # =============================================================================
    if args.loadtype == 'dev':
        db1 = m_passw.connect('D_D_ETL', debug=True)
        __etl_crsr = db1.cursor()
        db2 = m_passw.connect('D_D_UPD', debug=True)
        __upd_crsr = db2.cursor()
    elif args.loadtype == 'prod':
        db1 = m_passw.connect('D_P_ETL', debug=True)
        __etl_crsr = db1.cursor()
        db2 = m_passw.connect('D_P_UPD', debug=True)
        __upd_crsr = db2.cursor()
    elif args.loadtype == 'val':
        db1 = m_passw.connect('D_V_ETL', debug=True)
        __etl_crsr = db1.cursor()
        db2 = m_passw.connect('D_V_UPD', debug=True)
        __upd_crsr = db2.cursor()
    elif args.loadtype == 'xlsx':
        ds.to_excel('./' + __dsname + '.xlsx', index=False)
        sendtodart = False
        
    # =============================================================================
    # Generate dataloadid for sending to DART Server
    # ============================================================================= 
    if sendtodart:
        def map_sql_type(df_col):
            fmt = ds.dtypes[df_col] 
            outfmt = 'char'
            if df_col == 'SYS_CLMN_HGLT':
                outfmt = 'colormap'
            if not fmtovrid:   
                if fmt.name == 'object':
                    outfmt = 'char'
                elif 'float' in fmt.name:
                    outfmt = 'float'
                elif 'int' in fmt.name:
                    outfmt = 'int'
                elif 'bool' in fmt.name:
                    outfmt = 'bool'
                elif 'datetime' in fmt.name:
                    dt = pd.to_datetime(ds[df_col].dropna())
                    if len(dt) > 0:
                        if not (dt.dt.floor('D') == dt).all():
                            outfmt = 'datetime'
                        elif (dt.dt.date == pd.Timestamp('now').date()).all():
                            outfmt = 'time'
                        else:
                            outfmt = 'date'
            return outfmt
            
        __json_cnfg= "[" + ",".join('{"ColumnName":"' + x + '","ColumnHeader":"' + x + '","DataType":"' + map_sql_type(x) + '","ColumnOrder":' + str(i+1) + '}' 
                                        for i, x in enumerate(ds.columns)) + "]"
            
        ### Create LoadStartTime
        __updt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print('Creating Data Load ID..') 
        sql = 'EXECUTE etl.CreateDataLoad ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?'  
        vals = (init['_rowid'],         # GildaProtocolRowId (char)
                init['_GILDNO'],        # ProtocolNumber (char)
                args.listlabel,         # ListingName (char)
                args.tabname,           # TabName (char)
                args.listtitle,         # ListingDescription (char)
                args.listingcode,       # ListingCode (char)
                args.parentlistingname, # ParentListingName (char)
                __updt,                 # LoadStartTime (char)
                __CUT_ID,               # DataCutId (int)
                args.issubjectlisting,  # IsSubjectListing (bit)
                args.listcat,           # ListingCategoryId (int)
                args.organizationid,    # OrganizationId (int)
                __json_cnfg,            # JsonColumnConfig (char)
                __emailcontact,         # NotificationTo (char)
                " ")                    # ErrorReason (char)
        
         ### Execute and fetch dataloadid      
        __etl_crsr.execute(sql, vals) 
        __dataloadid = __etl_crsr.fetchval()
        
        # =============================================================================
        # Generate listing and send to DART Server      
        # =============================================================================
        __listingdetail = []
        
        ### Allow override of duplicates
        if not args.dupovrid:
            ds.set_index(args.keyvars, inplace = True)
            dups_at = ds.index[ds.index.duplicated()].values
            ds.loc[dups_at, "Duplicate"] = True
            ds.reset_index(inplace = True)
            for index, row in ds.loc[ds["Duplicate"] == True].iterrows():
                for key in args.keyvars:
                    ds[key] = ds[key].astype(str)
                    ds.at[index, key] = ds.at[index, key] + '_' + str(index)

        for index, row in ds.iterrows():                                                                                
            __listingdetail.append([row.dropna().to_json(date_format='iso'),                # JSONDATA
                                    row[args.keyvars].dropna().to_json(date_format='iso'),  # JSONKEY
                                    None,                                                   # DARTSUBJECTID
                                    __dataloadid])                                          # DATALOADID

        ### Insert into etl.ListingDetail
        print('Loading ListingDetail info into SQL table..') 
        __etl_crsr.execute('SELECT * FROM etl.ListingDetail')
        __etl_crsr.executemany("INSERT INTO etl.ListingDetail (JsonData, JsonKey, DartSubjectId, dataloadid) VALUES (?, ?, ?, ?)", __listingdetail)
        db1.commit()
        db1.close() 
        
        ### Create LoadEndTime
        __updt = datetime.datetime.now()
        
        ### Update etl.DataLoad 
        print('Updating DART metadata with load end time')     
        __upd_crsr.execute("update etl.DataLoad set loadendtime=? where dataloadid=?", __updt, __dataloadid)
        db2.commit()         
        db2.close() 
        print()
        
        senttodart = True