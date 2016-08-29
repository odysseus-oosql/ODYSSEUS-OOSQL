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

/*
 * Module: createPrimitiveClass.c
 *
 * Description:
 *  Create primitive classes
 *
 * Usage:
 *  createClass volume_name 
 *
 */


#include <stdio.h>
#include <stdlib.h>
#include "LOM_Internal.h"
#include "LOM.h"

#define db_Ref_SIZE 36

#define MACRO_BEGIN do {
#define MACRO_END } while(0)

char *primitiveClass[LOM_NUMOFDBPRIMITIVETYPE] = {"void","d_Short","d_Long","d_UShort",
"d_ULong","d_Float","d_Double", "d_Char","d_Octet","d_Boolean","d_Date","d_Time","d_Timestamp",
"d_Interval", "d_String", };
Four primitiveType[LOM_NUMOFDBPRIMITIVETYPE] = {-1, SM_SHORT, SM_LONG, SM_SHORT, 
SM_LONG, SM_FLOAT, SM_DOUBLE, SM_STRING, SM_STRING, SM_LONG, LOM_DATE, LOM_TIME, LOM_TIMESTAMP,
LOM_INTERVAL, LOM_VARSTRING, };
Four primitiveSize[LOM_NUMOFDBPRIMITIVETYPE] = {-1, SM_SHORT_SIZE, SM_LONG_SIZE, 
SM_SHORT_SIZE, SM_LONG_SIZE, SM_FLOAT_SIZE, SM_DOUBLE_SIZE, 1, 1, SM_LONG_SIZE,
LOM_DATE_SIZE, LOM_TIME_SIZE, LOM_TIMESTAMP_SIZE, LOM_INTERVAL_SIZE, LOM_MAXVARSTRINGSIZE, };

Four ODYS_Format
(
	LOM_Handle* handle,
	Four        numOfDevices,
	char        **devNameList,
	char        *volName,
	Four        volId,
	Four        extentSize,
	Four        *numPagesInDevice,
	Four        segmentSize
)
{
	Four e;			/* error code */
	AttrInfo ainfo[10];		/* column information */
	LRDS_IndexDesc idesc;		/* key information for the clustering index */
	XactID  xactId;
	char inheritedFrom[LOM_MAXCLASSNAME];
	char className[LOM_MAXCLASSNAME];
	char domainName[LOM_MAXCLASSNAME];
	Four classId;
	Four i;

	e = LOM_FormatDataVolume(handle, numOfDevices, devNameList, volName, volId, extentSize, numPagesInDevice, segmentSize);
	if (e < 0) LOM_ERROR(handle, e);

	e = LOM_Mount(handle, numOfDevices, devNameList, &volId);
	if (e < 0) LOM_ERROR(handle, e);

	/* start new transaction for LOM_SYSCLASSES*/
	e = LOM_TransBegin(handle, &xactId, X_RR_RR);
	if (e < 0) {
		LOM_Dismount(handle, volId);
		LOM_ERROR(handle, e);
	}

	for(i=1;i< LOM_NUMOFDBPRIMITIVETYPE -1 && primitiveClass[i] != NULL;i++) {

		ainfo[0].complexType = SM_COMPLEXTYPE_BASIC;
		ainfo[0].type = SM_STRING;
		ainfo[0].length = 1;
		ainfo[0].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
		strcpy(ainfo[0].name, "hasParent");
		ainfo[0].domain = LOM_STRING;

		ainfo[1].complexType = SM_COMPLEXTYPE_BASIC;
		ainfo[1].type = primitiveType[i];
		ainfo[1].length = primitiveSize[i];
		ainfo[1].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
		strcpy(ainfo[1].name, primitiveClass[i]);
		ainfo[1].domain = primitiveType[i];

		e = LOM_CreateClass(handle, volId, primitiveClass[i], NULL, NULL, 2, (AttrInfo*)&ainfo[0]
		,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, (Four)i);
		if (e < 0) {
			LOM_TransAbort(handle, &xactId);
			LOM_Dismount(handle, volId);
			LOM_ERROR(handle, e);
		}
	}

	for(i=1;i< LOM_NUMOFDBPRIMITIVETYPE -1 && primitiveClass[i] != NULL;i++) {

		ainfo[0].complexType = SM_COMPLEXTYPE_BASIC;
		ainfo[0].type = SM_STRING;
		ainfo[0].length = 1;
		ainfo[0].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
		strcpy(ainfo[0].name, "hasParent");
		ainfo[0].domain = LOM_STRING;

		/* create set class */
		ainfo[1].complexType = LOM_COMPLEXTYPE_ODMG_COLLECTIONSET;
		ainfo[1].type = primitiveType[i];
		ainfo[1].length = primitiveSize[i];
		ainfo[1].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
		strcpy(ainfo[1].name,"elements");
		ainfo[1].domain = primitiveType[i];

		strcpy(className, "odmg_set_");
		strcat(className, primitiveClass[i]);
		e = LOM_CreateClass(handle, volId, className, NULL, NULL, 2, (AttrInfo*)&ainfo[0]
		,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, (Four)(i-1)*4+LOM_NUMOFDBPRIMITIVETYPE);
		if (e < 0) {
			LOM_TransAbort(handle, &xactId);
			LOM_Dismount(handle, volId);
			LOM_ERROR(handle, e);
		}

		/* create bag class */
		ainfo[1].complexType = LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG;
		strcpy(className, "odmg_bag_");
		strcat(className, primitiveClass[i]);
		e = LOM_CreateClass(handle, volId, className, NULL, NULL, 2, (AttrInfo*)&ainfo[0]
			,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, (Four)(i-1)*4+LOM_NUMOFDBPRIMITIVETYPE+1);
		if (e < 0) {
			LOM_TransAbort(handle, &xactId);
			LOM_Dismount(handle, volId);
			LOM_ERROR(handle, e);
		}

		/* create list class */
		ainfo[1].complexType = LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST;
		strcpy(className, "odmg_list_");
		strcat(className, primitiveClass[i]);
		e = LOM_CreateClass(handle, volId, className, NULL, NULL, 2, (AttrInfo*)&ainfo[0]
			,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, (Four)(i-1)*4+LOM_NUMOFDBPRIMITIVETYPE+2);
		if (e < 0) {
			LOM_TransAbort(handle, &xactId);
			LOM_Dismount(handle, volId);
			LOM_ERROR(handle, e);
		}

		/* create varray class */
		ainfo[1].complexType = LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY;
		strcpy(className, "odmg_varray_");
		strcat(className, primitiveClass[i]);
		e = LOM_CreateClass(handle, volId, className, NULL, NULL, 2, (AttrInfo*)&ainfo[0]
			,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, (Four)(i-1)*4+LOM_NUMOFDBPRIMITIVETYPE+3);
		if (e < 0) {
			LOM_TransAbort(handle, &xactId);
			LOM_Dismount(handle, volId);
			LOM_ERROR(handle, e);
		}
	}

	ainfo[0].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[0].type = SM_LONG;
	ainfo[0].length = SM_LONG_SIZE;
	ainfo[0].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[0].name, "hasParent");
	ainfo[0].domain = LOM_STRING;
	
	e = LOM_CreateClass(handle, volId, "d_Object", NULL, (NULL), 1, (AttrInfo*)&ainfo[0]
		,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, 15);
	if (e < 0) {
		LOM_TransAbort(handle, &xactId);
		LOM_Dismount(handle, volId);
		LOM_ERROR(handle, e);
	}

	ainfo[LOM_LARGEOBJECT_HASPARENT_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[LOM_LARGEOBJECT_HASPARENT_COLNO].type = SM_STRING;
	ainfo[LOM_LARGEOBJECT_HASPARENT_COLNO].length = 1;
	ainfo[LOM_LARGEOBJECT_HASPARENT_COLNO].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[LOM_LARGEOBJECT_HASPARENT_COLNO].name, "hasParent");
	ainfo[LOM_LARGEOBJECT_HASPARENT_COLNO].domain = LOM_STRING;

	ainfo[LOM_LARGEOBJECT_SIZE_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[LOM_LARGEOBJECT_SIZE_COLNO].type = LOM_LONG_VAR;
	ainfo[LOM_LARGEOBJECT_SIZE_COLNO].length = LOM_LONG_SIZE_VAR;
	ainfo[LOM_LARGEOBJECT_SIZE_COLNO].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[LOM_LARGEOBJECT_SIZE_COLNO].name, "size");
	ainfo[LOM_LARGEOBJECT_SIZE_COLNO].domain = LOM_LONG_VAR;

	ainfo[LOM_LARGEOBJECT_OID_COLUMN].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[LOM_LARGEOBJECT_OID_COLUMN].type = SM_OID;
	ainfo[LOM_LARGEOBJECT_OID_COLUMN].length = SM_OID_SIZE;
	ainfo[LOM_LARGEOBJECT_OID_COLUMN].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[LOM_LARGEOBJECT_OID_COLUMN].name, "oid");
	ainfo[LOM_LARGEOBJECT_OID_COLUMN].domain = LOM_OID;

	ainfo[LOM_LARGEOBJECT_OPENCLASSNUM_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[LOM_LARGEOBJECT_OPENCLASSNUM_COLNO].type = LOM_LONG_VAR;
	ainfo[LOM_LARGEOBJECT_OPENCLASSNUM_COLNO].length = LOM_LONG_SIZE_VAR;
	ainfo[LOM_LARGEOBJECT_OPENCLASSNUM_COLNO].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[LOM_LARGEOBJECT_OPENCLASSNUM_COLNO].name, "openClassNum");
	ainfo[LOM_LARGEOBJECT_OPENCLASSNUM_COLNO].domain = LOM_LONG_VAR;
	
	e = LOM_CreateClass(handle, volId, "d_LargeObj", NULL, (NULL), LOM_LARGEOBJECT_NUM_COLS, (AttrInfo*)&ainfo[0]
		,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, 16);
	if (e < 0) {
		LOM_TransAbort(handle, &xactId);
		LOM_Dismount(handle, volId);
		LOM_ERROR(handle, e);
	}

	ainfo[0].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[0].type = SM_STRING;
	ainfo[0].length = 1;
	ainfo[0].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[0].name, "hasParent");
	ainfo[0].domain = LOM_STRING;

	ainfo[1].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[1].type = SM_OID;
	ainfo[1].length = SM_OID_SIZE;
	ainfo[1].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[1].name, "oid");
	/* db_Ref's domain is what? */
	/* it should be changed */
	ainfo[1].domain = LOM_OID;

	ainfo[2].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[2].type = SM_STRING;
	ainfo[2].length = ODMG_REF_SIZE;
	ainfo[2].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[2].name, "db_Ref_$refInfo$_");
	ainfo[2].domain = LOM_STRING;

	e = LOM_CreateClass(handle, volId, "db_Ref", NULL, NULL, 3, (AttrInfo*)&ainfo[0]
		,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, 17);
	if (e < 0) {
		LOM_TransAbort(handle, &xactId);
		LOM_Dismount(handle, volId);
		LOM_ERROR(handle, e);
	}

	ainfo[0].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[0].type = SM_STRING;
	ainfo[0].length = 1;
	ainfo[0].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[0].name, "hasParent");
	ainfo[0].domain = LOM_STRING;

	ainfo[1].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[1].type = LOM_LONG_VAR;
	ainfo[1].length = LOM_LONG_SIZE_VAR;
	ainfo[1].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[1].name, "size");
	ainfo[1].domain = LOM_LONG_VAR;

	ainfo[2].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[2].type = LOM_LONG_VAR;
	ainfo[2].length = LOM_LONG_SIZE_VAR;
	ainfo[2].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[2].name, "numElem");
	ainfo[2].domain = LOM_LONG_VAR;

	ainfo[3].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[3].type = LOM_LONG_VAR;
	ainfo[3].length = LOM_LONG_SIZE_VAR;
	ainfo[3].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[3].name, "elemClassID");
	ainfo[3].domain = LOM_LONG_VAR;

	ainfo[4].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[4].type = SM_VARSTRING;
	ainfo[4].length = LOM_MAXARRAYSIZE;
	ainfo[4].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[4].name, "data");
	ainfo[4].domain = LOM_STRING;

	e = LOM_CreateClass(handle, volId, "db_Array", NULL, (NULL), 5, (AttrInfo*)&ainfo[0]
		,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, 30);
	if (e < 0) {
		LOM_TransAbort(handle, &xactId);
		LOM_Dismount(handle, volId);
		LOM_ERROR(handle, e);
	}

	ainfo[0].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[0].type = SM_STRING;
	ainfo[0].length = 1;
	ainfo[0].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[0].name, "hasParent");
	ainfo[0].domain = LOM_STRING;

	ainfo[1].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[1].type = SM_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
	ainfo[1].length = INT_MAX;
#else
	ainfo[1].length = LONG_MAX;
#endif
	ainfo[1].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[1].name, "sys_remaining_information");
	ainfo[1].domain = LOM_STRING;

	e = LOM_CreateClass(handle, volId, "db_Stream", NULL, NULL, 2, (AttrInfo*)&ainfo[0]
		,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, 40);
	if (e < 0) {
		LOM_TransAbort(handle, &xactId);
		LOM_Dismount(handle, volId);
		LOM_ERROR(handle, e);
	}

	ainfo[0].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[0].type = SM_VARSTRING;
	ainfo[0].length = LOM_MAXOBJECTNAME;
	ainfo[0].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[0].name, "objectName");
	ainfo[0].domain = LOM_STRING;

	ainfo[1].complexType = SM_COMPLEXTYPE_BASIC;
	ainfo[1].type = SM_OID;
	ainfo[1].length = SM_OID_SIZE;
	ainfo[1].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
	strcpy(ainfo[1].name, "oid");
	ainfo[1].domain = LOM_OID;

	e = LOM_CreateClass(handle, volId, "db_ObjectNameTable", NULL, NULL, 2, (AttrInfo*)&ainfo[0]
	,0,(NULL),0,(MethodInfo *)NULL, SM_FALSE, 45);
	if (e < 0) {
		LOM_TransAbort(handle, &xactId);
		LOM_Dismount(handle, volId);
		LOM_ERROR(handle, e);
	}

	e = LOM_TransCommit(handle, &xactId);
	if (e < 0) {
		LOM_TransAbort(handle, &xactId);
		LOM_Dismount(handle, volId);
		LOM_ERROR(handle, e);
	}

	e = LOM_Dismount(handle, volId);
	if (e < 0) LOM_ERROR(handle, e);
    
	return eNOERROR;
}
