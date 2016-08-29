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

#ifndef __LOADDB_H__
#define __LOADDB_H__

#include <sys/stat.h>
#include <map>
using namespace std;

#define MAXNAMELEN		50	
#define MAXLINELEN		102400
#define MAXCLASSNUM		20
#define MAXATTRNUM		128
#define MAXPOOLLEN		204800
#define MAXCARDINALITY	1024
#define MAXPOINTS		4096

/* define etc */
#define NOTOPENEDCLASS	(long)-100
#define TEMPCLASS		(MAXCLASSNUM-1)
#define NOTSPECIFIED	-1 
#define TMPRELNAME		"Tmptbl"
#define TMPIDXNAME		"class-inst"

/* error code */
#define eUNDEFINEDSYNTAX				-9001
#define eNOSUCHCLASS					-9002
#define eNOSUCHATTRIBUTE				-9003
#define eNOSUCHINSTANCE					-9004
#define eNOTENOUGHMEMORY				-9005
#define eBADPARAMETER_LOADER			-9006
#define eBADTEXTINDEXMODE_LOADER		-9007
#define eSPATIALATTRMUSTBEFIRST			-9008
#define eTOOLARGEPOINTSAREUSED			-9009
#define eINSUFFICIENTDATA				-9010

typedef enum {
	OPEN=0,
	CLOSED,
	SINGLE_QUOTE,
	DOUBLE_QUOTE,
	BACKSLASH
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
	Two type;
	Two complexType;
} AttrTypeInfoEntry;

typedef struct {
	char		className[MAXNAMELEN];
	Four        classID;
	Four		ocn; 
	Four		scanID; 
	VarArray	colList;
	VarArray	attrTypeInfo;
	Four		colNoToCollistMap[MAXATTRNUM];
	Boolean		colNoToCollistMapMaden;
	Four		colNoToDataCollistMap[MAXATTRNUM];
	Boolean		colNoToDataCollistMapMaden;
	FILE*		oidListFile;
	Four		bulkLoadID;
	Four        attrNum;
} ClassListEntry;

#define LOADER_CLASSLIST			((ClassListEntry*)classList.ptr)
#define LOADER_COLLIST(x)			((ColListEntry*)LOADER_CLASSLIST[x].colList.ptr)
#define LOADER_ATTRTYPEINFO(x)		((AttrTypeInfoEntry*)LOADER_CLASSLIST[x].attrTypeInfo.ptr)
#define LOADER_DATAPOOL				((char*)readData.ptr)
#define LOADER_ELEMSIZE(x,y)		(LOADER_COLLIST(x)[y].elementSize)
#define LOADER_ELEMSIZEARRAY(x,y)	((Four*)LOADER_COLLIST(x)[y].elementSize.ptr)

/* function prototype */
Four loader_OpenInputFile(LOM_Handle*, Four, Four, char*, map<int, int>& pageRankMap);
Four loader_Init(LOM_Handle*);
Four loader_CreateMapTable(LOM_Handle*, Four, LOM_IndexID*);
Four loader_UpdateMapTable(LOM_Handle*, int, int, OID*);
Four loader_GetReferencedOID(LOM_Handle*, char*, Four, LOM_IndexID*, OID*);
Four loader_ReadNextLine(LOM_Handle *, Four, Four, FILE*, map<int, int>& pageRankMap);
Four loader_ReadLine(char *, Four, FILE *);
Four loader_GenerateClassInfo(LOM_Handle *, Four, Four, Two, char*);
Four loader_MakeColInfo(Two, catalog_SysAttributesOverlay*, Two, char*);
void loader_AccumReadData_SetNew(void);
Four loader_AccumReadData(LOM_Handle*, char*, Boolean, Two, Two, Four);
Four loader_InsertObject(LOM_Handle *, Two, Two, OID*, map<int, int>& pageRankMap);
Four loader_GetDateValue(char*, unsigned short*, unsigned short*, unsigned short*);
Four loader_GetTimeValue(char*, unsigned short*, unsigned short*, unsigned short*);
Four loader_SetTextIndexMode(char*);
Four loader_Finalize(LOM_Handle*);
char *loader_Error(int);
Four loader_SetSpatialObject(LOM_Handle* handle, One classType, Four ocnOrScanId, Boolean useScanFlag, Four lineSegOcnOrScanId, Boolean lineSegUseScanFlag, OID* oid, char* pointString);
char* loader_GetPointFromPointString(char* pString, Four& x, Four& y);

#endif /* __LOADDB_H__ */
