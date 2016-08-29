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

#ifndef __DBINTF_INTERNAL_H_
#define __DBINTF_INTERNAL_H_

#include "ODYS_common.h"
#include "OBfM_Internal.h"

#define INITCURSOR(cursor) (cursor).ocn = -1; (cursor).scanId = -1;

#define DB_GET_SHORT(value)	(value)->data.s
#define DB_GET_INT(value) (value)->data.i
#define DB_GET_LONG(value) (value)->data.l
#define DB_GET_FLOAT(value) (value)->data.f
#define DB_GET_DOUBLE(value) (value)->data.d
#define DB_GET_USRDESC(value) (value)->data.ud
#define DB_GET_POINT(value)	(value)->data.ptr
#define DB_GET_STRING(value) (value)->data.ptr
#define DB_GET_VARSTRING(value) (value)->data.ptr

#define DB_MAKE_SHORT(value, S)	(value)->data.s = S , (value)->domainInfo.is_null=0, (value)->domainInfo.type = DB_TYPE_SHORT 
#define DB_MAKE_INT(value, I) (value)->data.i = I, (value)->domainInfo.is_null=0, (value)->domainInfo.type = DB_TYPE_INTEGER
#define DB_MAKE_LONG(value, L) (value)->data.l = L, (value)->domainInfo.is_null=0, (value)->domainInfo.type = DB_TYPE_LONG
#define DB_MAKE_FLOAT(value, F) (value)->data.f = F, (value)->domainInfo.is_null=0, (value)->domainInfo.type = DB_TYPE_FLOAT
#define DB_MAKE_DOUBLE(value, D) (value)->data.d = D, (value)->domainInfo.is_null=0, (value)->domainInfo.type = DB_TYPE_DOUBLE
#define DB_MAKE_USRDESC(value, UD) (value)->data.ud = UD,  (value)->domainInfo.is_null=0, (value)->domainInfo.type = DB_TYPE_OID
#define DB_MAKE_POINT(value, PTR)	(value)->data.ptr = (void *)PTR, (value)->domainInfo.is_null=0 , (value)->domainInfo.type = DB_TYPE_STRING 
#define DB_MAKE_STRING(value, PTR)	(value)->data.ptr = (void *)PTR, (value)->domainInfo.is_null=0, (value)->domainInfo.type = DB_TYPE_STRING
#define DB_MAKE_VARSTRING(value, PTR)	(value)->data.ptr = PTR, (value)->domainInfo.is_null=0, (value)->domainInfo.type = DB_TYPE_VARSTRING

typedef struct {
	Four ocn;	/* open class number */
	Four scanId; /* scan identifier */
	OID oid;
}db_Cursor;

typedef enum db_type {
	DB_TYPE_NULL = -1,
	DB_TYPE_VOID = -1,
	DB_TYPE_SHORT = SM_SHORT,
	DB_TYPE_INTEGER = SM_INT,
	DB_TYPE_LONG = SM_LONG,
	DB_TYPE_FLOAT = SM_FLOAT,
	DB_TYPE_DOUBLE = SM_DOUBLE,
	DB_TYPE_STRING = SM_STRING,
	DB_TYPE_VARSTRING = SM_VARSTRING,
	DB_TYPE_OID = SM_OID,
	DB_TYPE_SET = 15,
	DB_TYPE_POINTER = 16              /* method arguments only */
} db_Type;

/*
 * Typedef for db_DomainInfo
 *
 * information on attributes' domain
*/

typedef struct {
	unsigned char is_null;  /* check if the coresponding attribute is null */
	db_Type type;     
	Four domain_id;
}db_Domain;

/*
 * Typedef for db_Set
 *
 * This is primary structure used for set interface for set attribute
 *
*/

typedef struct obfm_set db_Set;

typedef struct catalog_SysClassesOverlay db_Class;
typedef struct catalog_SysAttributesOverlay db_Attribute;
typedef struct catalog_SysMethodsOverlay db_Method;

/*
 * Typedef for db_Data
 *
 * A union of all of the possible basic type. This is used in the definition
 * of db_Value which is the fundamental structure.
*/

typedef union {
	Two     s;
	int     i;
	Four    l;
	float   f;
	double  d;
	userDescriptor *ud; /* pointer to userDescriptor */
	void   *ptr;        /* pointer to data */
	obfm_Set *set;

	/* INTERNAL USE ONLY */
	objectDescriptor *od;   /* pointer to objectDescriptor */
	OID     oid;        /* INTERNAL USE ONLY */
	/* large data manipulation */
	/*
	obfm_Stream   *ld;
	*/
}db_Data;

/*
 * Typedef for db_Value
 *
 * This is primary structure used for db interface layer for accessing values
 * of objects.
 *
*/

typedef struct {
	db_Domain	domainInfo;
	db_Data			data;
}db_Value;

typedef struct db_list {
	struct db_list *next;
	union {
		db_Class *class;
		db_Attribute *attribute;
		db_Cursor *cursor;
	}data;
}db_List;	

typedef struct db_MethodInfo{
	const char *methodName;
	void (*function)(userDescriptor *, db_Value *returnVal, ...);
}db_MethodInfo;

/* Error Handling */
#define DB_ERROR(e) \
BEGIN_MACRO \
printf("Error Code %ld: %s\n", e, DB_Err(e)); \
printf("File:%s Line:%ld\n", __FILE__, __LINE__); \
if (1) return(e);  \
END_MACRO


extern Four db_databaseID; /* initialize variables */
extern Four procIndex;
extern XactID db_xactId;

#endif
