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

#ifndef __CHECK_DATASYNTAX_H__
#define __CHECK_DATASYNTAX_H__

#define eNOERROR_WITH_DISPLAY				1

#undef  MAXNAMELEN
#define MAXNAMELEN							50	
#define MAXLINELEN							102400
#define MAXCLASSNUM							20
#define MAXATTRNUM							128
#define MAXPOOLLEN							204800
#define MAXCARDINALITY						1024
#define MAXPOINTS							4096
#define MAXALLOWEDERRORNUM					20

/* define etc */
#define NOTOPENEDCLASS						(long)-100
#define NOTSPECIFIED						-1

/* error code */
#define eCANNOTFINDCLASSREF_CHECKER			-9500
#define eINVALIDINSTANCENO_CHECKER			-9501	
#define eUNDEFINEDCOMMAND_CHECKER			-9502
#define eCANNOTCLOSESETCOLUMN_CHECKER		-9503
#define eNOSUCHCLASS_CHECKER				-9504
#define eNOSUCHATTRIBUTE_CHECKER			-9505
#define eINSUFFICIENTDATA_CHECKER			-9506
#define eSETALREADYOPENED_CHECKER			-9507
#define eSETALREADYCLOSED_CHECKER			-9508
#define eINVALIDUSEOFBACKSLASH_CHECKER		-9509
#define eLINETRUNCATED_CHECKER				-9510
#define eSINGLEQUOTEINDATA_CHECKER			-9511
#define eDOUBLEQUOTEINDATA_CHECKER			-9512
#define eINVALIDNUMBERFORMAT_CHECKER		-9513
#define eINVALIDUSEOFSINGLEQUOTE_CHECKER 	-9514
#define eINVALIDUSEOFDOUBLEQUOTE_CHECKER	-9515
#define eINCOMPLETEDATALINE_CHECKER			-9516
#define eINCOMPATIBLEATTRTYPE_CHECKER		-9517
#define eSPATIALATTRMUSTBEFIRST_CHECKER     -9518

typedef enum {
	OPEN=0,
	CLOSED,
	SINGLE_QUOTE,
	DOUBLE_QUOTE,
	BACKSLASH,
	REF_OBJECT
} readState;

typedef struct {
	Two colNo;
	Two type;
	Four start;
	Four length;
	Four size;
	Boolean isNull;
	Two complexType;
	Four nElements;
	VarArray elementSize;
} ColListEntry;

typedef struct {
	char		className[MAXNAMELEN];
	Four        classID;
	Four		ocn; 
	Four		scanID; 
	VarArray	colList;
	Four        attrNum;
} ClassListEntry;

#define CHECKER_CLASSLIST			((ClassListEntry*)checker_classList.ptr)
#define CHECKER_COLLIST(x)			((ColListEntry*)CHECKER_CLASSLIST[x].colList.ptr)
#define CHECKER_DATAPOOL			((char*)checker_readData.ptr)
#define CHECKER_ELEMSIZE(x,y)		(CHECKER_COLLIST(x)[y].elementSize)
#define CHECKER_ELEMSIZEARRAY(x,y)	((Four*)CHECKER_COLLIST(x)[y].elementSize.ptr)

#define DISPLAY_ERROR_MESSAGE(x,y,z) \
	checker_PrintError(x,y,z)

#define RESET_DATA_ACCUMULATOR \
{\
	checker_prevColNo     = -1; \
	checker_prevElementNo = -1; \
}

#define SKIP_TO_NEXT_DATA_RECORD(x,y,z,w) \
	checker_skipToNextDataRecord(x,y,z,w)

/* function prototype */
Four checker_Init(LOM_Handle*);
Four checker_ValidateRefObject(LOM_Handle*, char*, Four);
Four checker_InspectSourceData(LOM_Handle*, Four, char*, Boolean);
Four checker_InspectDataFile(LOM_Handle*, Four, FILE*, Boolean, FILE*);
Four checker_ReadLine(char *, Four, FILE *);
Four checker_GenerateClassInfo(LOM_Handle*, Four, Two, char*, Two*);
Four checker_GetColumnInfo(Two classNo, catalog_SysAttributesOverlay*, Two, char*, Two*);
Four checker_AccumReadData(LOM_Handle*, char*, Boolean, Two, Two, Four, Boolean, FILE*);
Four checker_Finalize(LOM_Handle*);
void checker_PrintError(Four, Four, char*);
void checker_skipToNextDataRecord(FILE*, char*, Two, Two);

#endif /* __CHECK_DATASYNTAX_H__ */
