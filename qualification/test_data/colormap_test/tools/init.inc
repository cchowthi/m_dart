/**************************************************************************\
*  SAVED AS:        init.inc                                               *
*                                                                          *
*  CODED ON:        25OCT2016 Michael G. Neece 	                           *
*                                                                          *
*                                                                          *
\**************************************************************************/

*** Tools ***;
	%let tools	= ../tools ;		/* Only valid setting is "../tools" (without quotes */
	%let toolver	= v4.19.01 ;		/* BDE and BAE version. */

*** Options ***;
  	options NOQUOTELENMAX varlenchk=nowarn MPRINT nofmterr;	** if variables are long, no log messages appear as issues **;
libname vdata "/biometrics/projects/p223/s2231015/bmcdp/vdata" ;
    libname pacbi_si "/biometrics/projects/p223/s2231015/bmcdp/source/pacbio_si/current" ;
libname pacbi_ar "/biometrics/projects/p223/s2231015/bmcdp/source/pacbio_ar/current" ;
        libname metgf_ar "/biometrics/projects/p223/s2231015/bmcdp/source/metabolon_ar/current" ;
libname metgf_si "/biometrics/projects/p223/s2231015/bmcdp/source/metabolongfr_si/current" ;
libname nordi_si "/biometrics/projects/p223/s2231015/bmcdp/source/nordic_si/current" ;
libname nordi_ar "/biometrics/projects/p223/s2231015/bmcdp/source/nordic_ar/current" ;

/* Do not change anything below this */
%let _GILDNO=GS-US-223-1015;
%let _STFLDNM=s2231015;   	
%let sno = 223-1015;
%let _rowid=1-7PF3W;
%let _subj_keyvars= invid subjid scrnid; 
/* Do not change anything above this */

