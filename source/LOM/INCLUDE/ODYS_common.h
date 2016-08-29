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

#ifndef __ODYS_COMMON_H_
#define __ODYS_COMMON_H_

/*
 * Typedef for ObjectDescriptor
 *
 * an object descriptor in object buffer
 *
 */

typedef struct {
   unsigned mark:1;
   unsigned dirty:1;
   unsigned referenced:1;
   OID oid;
}objectDescriptor;

typedef struct {
	char hasParent;
}objectHeader;

/*
 * Typedef for Object Reference
 *
 *  accessing object in object buffer
 */

typedef struct {
    OID oid;                            /* object id */
    objectDescriptor *od;               /* memory address of object*/
}objectRef;

/*
 * Typedef for objectLink
 *
 *  accessing object in object buffer
 */

typedef struct {
    OID oid;                            /* object id */
    objectDescriptor *od;               /* memory address of object*/
}objectLink;

/*
 * Typedef for User Descriptor
 *
 * Descriptor for accessing object in user application
*/

typedef struct {
    unsigned isValid:1;
    objectRef objRef;
}userDescriptor;

/*
**  Constants
*/

#define  MAXCLASSNAME			LOM_MAXCLASSNAME      /* Maximum length of class name */
#define  MAXRELATIONSHIPNAME	LOM_MAXCLASSNAME      /* Maximum length of class name */
#define  MAXTYPENAME			LOM_MAXCLASSNAME      /* Maximum length of type name */
#define  MAXATTRNAME			LOM_MAXCLASSNAME      /* Maximum length of attribute name */
#define  MAXMETHODNAME			LOM_MAXCLASSNAME      /* Maximum length of method name */
#define  MAXFUNCTIONNAME		256     /* Maximum length of method name */
#define  MAXNARGUMENT			36      /* Maximum number of argument */
#define  MAXARGUMENTNAME		LOM_MAXCLASSNAME	  /* Maximum length of argument name */
#define  MAXDIRPATH			256     /* Maximum length of directory path */
#define  ALIGNMENT_LENGTH		4       /* Alignment Length */
#define  HASPARENTEXIST 		1

/* Data Type Supported by the B+ tree */
#define ODYS_SHORT		LOM_SHORT
#define ODYS_INT		LOM_INT
#define ODYS_LONG		LOM_LONG
#define ODYS_FLOAT		LOM_FLOAT
#define ODYS_DOUBLE		LOM_DOUBLE
#define ODYS_STRING		LOM_STRING
#define ODYS_VARSTRING		LOM_VARSTRING
#define ODYS_PAGEID		LOM_PAGEID
#define ODYS_FILEID		LOM_FILEID
#define ODYS_INDEXID		LOM_INDEXID
#define ODYS_OID		LOM_OID
#define ODYS_MBR		LOM_MBR
#define ODYS_REF		LOM_REF
#define ODYS_LINK		LOM_LINK
#define ODYS_CHARSTRING		LOM_STRING
#define ODYS_DATE        	LOM_DATE
#define ODYS_TIME        	LOM_TIME
#define ODYS_TIMESTAMP   	LOM_TIMESTAMP
#define ODYS_INTERVAL    	LOM_INTERVAL
#define ODYS_TEXT     		LOM_TEXT

#define ODYS_SHORT_SIZE   	LOM_SHORT_SIZE
#define ODYS_INT_SIZE     	LOM_INT_SIZE
#define ODYS_LONG_SIZE    	LOM_LONG_SIZE
#define ODYS_FLOAT_SIZE   	LOM_FLOAT_SIZE
#define ODYS_DOUBLE_SIZE  	LOM_DOUBLE_SIZE
#define ODYS_PAGEID_SIZE  	LOM_PAGEID_SIZE
#define ODYS_INDEXID_SIZE 	LOM_INDEXID_SIZE
#define ODYS_FILEID_SIZE  	LOM_FILEID_SIZE
#define ODYS_OID_SIZE 		LOM_OID_SIZE
#define ODYS_REF_SIZE 		LOM_REF_SIZE
#define ODYS_LINK_SIZE 		LOM_LINK_SIZE
#define ODYS_MBR_SIZE 		LOM_MBR_SIZE
#define ODYS_DATE_SIZE		LOM_DATE_SIZE
#define ODYS_TIME_SIZE		LOM_TIME_SIZE
#define ODYS_TIMESTAMP_SIZE	LOM_TIMESTAMP_SIZE
#define ODYS_INTERVAL_SIZE	LOM_INTERVAL_SIZE

/* the following types are relevant to ODMG type systems */
#define ODYS_USHORT      	LOM_USHORT
#define ODYS_ULONG       	LOM_ULONG
#define ODYS_BOOLEAN     	LOM_BOOLEAN
#define ODYS_OCTET       	LOM_OCTET
#define ODYS_USHORT_SIZE	LOM_USHORT_SIZE
#define ODYS_ULONG_SIZE		LOM_ULONG_SIZE
#define ODYS_BOOLEAN_SIZE	LOM_BOOLEAN_SIZE
#define ODYS_OCTET_SIZE		LOM_OCTET_SIZE

#define ODYS_OBJDESC_SIZE 	sizeof(objectDescriptor)

#define ODYS_MAXARRAYSIZE	LONG_MAX
#define	ODYS_MAXOBJECTNAME	128


#endif /* __ODYS_COMMON_H_ */
