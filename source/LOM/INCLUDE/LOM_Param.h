/******************************************************************************/
/*                                                                            */
/*    Copyright (c) 1990-2016, KAIST                                          */
/*    All rights reserved.                                                    */
/*                                                                            */
/*    Redistribution and use in source and binary forms, with or without      */
/*    modification, are permitted provided that the following conditions      */
/*    are met:                                                                */
/*                                                                            */
/*    1. Redistributions of source code must retain the above copyright       */
/*       notice, this list of conditions and the following disclaimer.        */
/*                                                                            */
/*    2. Redistributions in binary form must reproduce the above copyright    */
/*       notice, this list of conditions and the following disclaimer in      */
/*       the documentation and/or other materials provided with the           */
/*       distribution.                                                        */
/*                                                                            */
/*    3. Neither the name of the copyright holder nor the names of its        */
/*       contributors may be used to endorse or promote products derived      */
/*       from this software without specific prior written permission.        */
/*                                                                            */
/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
/*    POSSIBILITY OF SUCH DAMAGE.                                             */
/*                                                                            */
/******************************************************************************/
/******************************************************************************/
/*                                                                            */
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
/*                                                                            */
/*    Developed by Professor Kyu-Young Whang et al.                           */
/*                                                                            */
/*    Advanced Information Technology Research Center (AITrc)                 */
/*    Korea Advanced Institute of Science and Technology (KAIST)              */
/*                                                                            */
/*    e-mail: odysseus.oosql@gmail.com                                        */
/*                                                                            */
/*    Bibliography:                                                           */
/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
/*        Demonstration Award.                                                */
/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
/*        Storage Structure Using Subindexes and Large Objects for Tight      */
/*        Coupling of Information Retrieval with Database Management          */
/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
/*        (1999)).                                                            */
/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
/*        J., "Tightly-Coupled Spatial Database Features in the               */
/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
/*                                                                            */
/******************************************************************************/

#ifndef _LOM_PARAM_H
#define _LOM_PARAM_H

#include "param.h"		/* include cosmos configuration parameter file */

/************************************************************/
/* VOLUMN-RELATED PARAMETERS                                */
/************************************************************/
/* maximum number of volumn to mount */
#define CATALOG_MAXNUMOFVOLS				MAXNUMOFVOLS   /* maximum number of volumes */

/************************************************************/
/* CLIENT/SERVER-RELATED PARAMETERS                         */
/************************************************************/

/* time-out millisecond */
#define LOM_MAXTIMEOUT          			60 * 24 * 60

/* the number of trial of connection to a server */
#define LOM_MAXTRIAL            			60 * 1000

/* encoding length for fetching colummn info. */
#define LOM_FETCHCOLINFO_ENCODING_LENGTH()  (sizeof(Two) + sizeof(Four)*3)

#define LOM_COLINFO_ENCODING_LENGTH(dataLength) (sizeof(Two) + sizeof(One) + sizeof(Four)*3 + dataLength)

/* the initial number of threads in a server */
#define LOM_INIT_NUM_OF_THREADS 			MAXTHREADS


/************************************************************/
/* SCHEMA-RELATED PARAMETERS                                */
/************************************************************/

/* maximum class name */
#define LOM_MAXCLASSNAME        			MAXRELNAME

/* maximum index name */
#define LOM_MAXINDEXNAME        			LOM_MAXCLASSNAME

/* maximum method name */
#define LOM_MAXMETHODNAME					MAXKEYLEN/2

/* maximum function name */
#define LOM_MAXFUNCTIONNAME					LOM_MAXMETHODNAME

/* maximum path length */
#define LOM_MAXPATHLENGTH       			1024

/* maximum # of arguments */
#define LOM_MAXNARGUMENT					36

#define LOM_MAXARGUMENTNAME					LOM_MAXCLASSNAME

/* maximum relationship name */
#define LOM_MAXRELATIONSHIPNAME				LOM_MAXCLASSNAME

#define LOM_MAXSUPERCLASSID     			LOM_MAXCLASSNAME
#define LOM_MAXSUBCLASSID       			LOM_MAXCLASSNAME

/* maximum # of superclasses for single class */
#define LOM_MAXSUPERCLASSNUM 				12

/* maximum attribute name */
#define LOM_MAXATTRNAME						LOM_MAXCLASSNAME

/* maximum # of attributes */
#define LOM_MAXNUMOFATTRIBUTE   			MAXNUMOFCOLS

/* maximum # of indexes */
#define LOM_MAXNUMOFINDEX   				MAXNUMOFINDEXES

/* maximum type name */
#define LOM_MAXTYPENAME                     LOM_MAXCLASSNAME

/* maximum routine name */
#define LOM_MAXROUTINENAME                  LOM_MAXMETHODNAME

/* maximum # of methods */
#define LOM_MAXNUMOFMETHOD                  12

/* maximum transform group name */
#define LOM_MAXGROUPNAME                    LOM_MAXATTRNAME

/* maximum # of transform groups and elements */
#define LOM_MAXNUMOFTRANSFORM               12

/* maximum specific name */
#define LOM_MAXSPECIFICNAME                 LOM_MAXTABLENAME

#define LOM_MAXUDTPATHLEN                   32

/************************************************************/
/* TEXT-RELATED PARAMETERS                                  */
/************************************************************/

/* posting buffer size for fetching posting list from disk */
#define LOM_PAGESIZE					1024 * 4
#define LOM_DEFAULTPOSTINGBUFFERSIZE 	LOM_PAGESIZE*12*10

/* maximum # of text-type attributes in a class */
#define LOM_MAXNUMOFTEXTCOLUMN  			LOM_MAXNUMOFATTRIBUTE/2

#define LOM_MAXOBJECTNAME       			LOM_MAXFUNCTIONNAME             
#define LOM_MAXFILTERNAME       			LOM_MAXFUNCTIONNAME
#define LOM_MAXDIRPATH          			LOM_MAXPATHLENGTH
#define LOM_MAXFILTERFILEPATHNAME       	LOM_MAXPATHLENGTH
#define LOM_MAXFILTERFUNCTIONNAME       	LOM_MAXFUNCTIONNAME
#define LOM_MAXKEYWORDEXTRACTORNAME     	LOM_MAXFUNCTIONNAME
#define LOM_MAXKEYWORDEXTRACTORFILEPATHNAME LOM_MAXPATHLENGTH
#define LOM_MAXKEYWORDEXTRACTORFUNCTIONNAME LOM_MAXFUNCTIONNAME
#define LOM_MAXSTEMIZERNAME     			LOM_MAXFUNCTIONNAME
#define LOM_MAXSTEMIZERFILEPATHNAME     	LOM_MAXPATHLENGTH
#define LOM_MAXSTEMIZERFUNCTIONNAME     	LOM_MAXFUNCTIONNAME

/* Keyword extractor related */
#define LOM_MAXKEYWORDSIZE      			128			/* maximum size of keyword */

#define LOM_MAXFILEBUFFER 					1024 * 4	/* maximum buffer size for writing temorary file */
#define LOM_MAXLINESIZE 					1000		/* maximum size of one line length */

#define LOM_MAXPOSITIONLENGTH				1024 * 10  

#define MAX_NUM_EMBEDDEDATTRIBUTES			24

/************************************************************/
/* MISC. PARAMETERS                                         */
/************************************************************/

#define LOM_MAXARRAYSIZE					LONG_MAX
#define ODMG_REF_SIZE						36

#define TEXT_IN_DB							0
#define TEXT_IN_FILE						1
#define TEXT_IN_MEMORY						2
#define TEXT_DONE							1

#define TEXT_TEMP_PATH						"ODYS_TEMP_PATH"
#define INIT_NUMOF_DOCID_POINTER			341		/* (Four)((1024 * 4)/sizeof(TupleID)) */

#define LOM_NUMOFDBPRIMITIVETYPE			100	

#define LOM_CATALOGCLASSID_START			500 

#define LOM_MAXDLLFUNCPTRS					20	

#define LOM_NO_COMPRESSION_LENGTH		    256
#define LOM_COMPRESSION_MIN_LENGTH			1024
#define LOM_COMPRESSION_MIN_LEVEL			1	
#define LOM_COMPRESSION_MAX_LEVEL			9
#endif
