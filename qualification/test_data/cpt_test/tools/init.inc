/* start of header *************************************************************************************************************

Program Name:      init.inc

Program Author:    Joe Meskey (jmeskey)

Program Purpose:   Initialization File                                                        
                     - Creates macro variables used by global macros                                                 
                     - Automatically copied to tools directory & localized by FAB when new tools folders are created                                                                                   
                     - Programmer needs to provide data transfer date &dataver and last 4 digits of &sno (as applicable)                     
                     - Can be further customized by Programmer                                                                                                                  
                     - Template location: \\gsasdata\global\biostat\v4.19.01\folder_templates 
                                                                                                                                    
*************************************************************************************************************** end of header */

&debug %put Begin "init.inc" file processing. ;

/* macro variables used by the standard macros */
%let toolver  = v4.19.01;                    /* Global Macro Version */ 
%let dataver  = %str(ddmmmyyyy);             /* Data Transfer Date (updated/customized by Programmer) */
%let sno      = 412-5624;                    /* Study # (partially updated by FAB; Programmer provides last 4 digits) */
%let project  = GS-6207;                     /* GS # (updated by FAB) */
%let protocol = GS-US-412-5624;               /* Protocol # (updated by FAB) */
%let prtstudy = %str(Study GS-US-412-5624);  /* Protocol # for Output Header (updated by FAB, customizable by Programmer) */
%let hdrmode  = R;                           /* Header Justification (default & standard is R, can be changed to L) */
%let runmode  = BLINDED;                     /* Run Mode (changed to blank by Programmer if study is open-label/unblinded) */                                        
%let caution  = Y;                           /* Caution Flag (changed to blank by Programmer when data & programs are final) */
%let _subj_keyvars = scrnid;    /* Declare the unique key identifier variables for subjects in the study */
   
/* Check if CPT is active */
data _null_;
    set mgardata.b3pprot;
    where protocolname="&protocol";
    call symputx('CPT',ifc(iscptstudy=1,'Y','N'));
run;   

/* Programmer: create task/release specific macro variables below  */
%let cpreport = %str(&progpath./../outputs/reports);  


/* Set SAS options, overwrite system defaults  */
options mprint source ls=255 nocenter;  * source2 symbolgen;
     
&debug %put End "init.inc" file processing. ;
/* Do not change anything below this */
%let _GILDNO  = GS-US-412-5624;
%let _STFLDNM = s4125624;
%let _rowid   = 1-1ZWRVH;
/* Do not change anything above this */
