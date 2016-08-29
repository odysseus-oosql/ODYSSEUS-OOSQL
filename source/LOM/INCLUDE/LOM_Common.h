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

#ifndef _LOM_COMMON_H
#define _LOM_COMMON_H

typedef enum {
	LOM_GMT   = 0,  LOM_GMT12 = 12,     LOM_GMT_12 = -12,
	LOM_GMT1  = 1,  LOM_GMT_1 = -1,     LOM_GMT2   =  2,    LOM_GMT_2 = -2,
	LOM_GMT3  = 3,  LOM_GMT_3 = -3,     LOM_GMT4   =  4,    LOM_GMT_4 = -4,
	LOM_GMT5  = 5,  LOM_GMT_5 = -5,     LOM_GMT6   =  6,    LOM_GMT_6 = -6,
	LOM_GMT7  = 7,  LOM_GMT_7 = -7,     LOM_GMT8   =  8,    LOM_GMT_8 = -8,
	LOM_GMT9  = 9,  LOM_GMT_9 = -9,     LOM_GMT10  =  10,   LOM_GMT_10= -10,
	LOM_GMT11 = 11, LOM_GMT_11= -11,    LOM_USeastern = -5, LOM_UScentral = -6,
	LOM_USmoutain = -7, LOM_USpacific = -8 } LOM_TimeZone;

#define MIN(a,b) (((a) < (b)) ? (a):(b))

#define eNOERROR	0
#define BEGIN_MACRO	do {
#define END_MACRO	} while(0)
#define NULL_LENGTH -1

#define LOM_FALSE	0
#define LOM_TRUE	1
typedef One	LOM_Octet;
typedef Four 	LOM_Boolean;

/* date */
typedef UFour_Invariable LOM_Date;

/* time */
typedef struct {
	short _tzHour;
	short _tzMinute;
	short _Hour;
	short _Minute;
	short _Second;
	short _100thSec;
}LOM_Time;

/* timestamp */
typedef struct {
	LOM_Date d;
	LOM_Time t;
}LOM_Timestamp;

/* interval */
typedef double LOM_Interval;

typedef struct {
	Two colNo;                  /* IN column number */
	Boolean nullFlag;           /* TRUE if it has null value */
	Four start;                 /* IN starting offset within a column */
								/*   ALL_VALUE: read all data of this column */
	Four length;                /* IN amount of old data within a column */
	Four dataLength;            /* IN amount of new data */
	union {
		LOM_Boolean b;			/* LOM_BOOLEAN */
		LOM_Octet o;			/* LOM_OCTET */
        Two_Invariable    s;               /* LOM_SHORT */
        UTwo_Invariable   us;               /* LOM_USHORT */
        Four_Invariable    i;               /* LOM_INT */
        Four_Invariable   l;               /* LOM_LONG */
        UFour_Invariable    ul;         /* LOM_ULONG */
		Eight_Invariable	ll;
		float  f;               /* LOM_FLOAT */
		double d;               /* LOM_DOUBLE */
		PageID pid;				/* SM_PAGEID */
		FileID fid;				/* SM_FILEID */
		IndexID iid;			/* SM_INDEXID */
		OID    oid;             /* LOM_OID */
		LRDS_MBR mbr;           /* LOM_MBR */
		LOM_Date date;			/* LOM_DATE */
		LOM_Time time;			/* LOM_TIME */
		LOM_Timestamp timestamp;/* LOM_TIMESTAMP */
		LOM_Interval interval;	/* LOM_INTERVAL */
		void   *ptr;            /* pointer to data: SM_STRING, SM_VARSTRING */
	} data;
	Four retLength;             /* OUT return length of Read/Write */
} LOM_ColListStruct;

/* For ODMG Collection */
#define LOM_COMPLEXTYPE_BASIC					SM_COMPLEXTYPE_BASIC
#define LOM_COMPLEXTYPE_SET						SM_COMPLEXTYPE_SET
#define LOM_COMPLEXTYPE_COLLECTIONSET			SM_COMPLEXTYPE_COLLECTIONSET
#define LOM_COMPLEXTYPE_COLLECTIONBAG			SM_COMPLEXTYPE_COLLECTIONBAG
#define LOM_COMPLEXTYPE_COLLECTIONLIST			SM_COMPLEXTYPE_COLLECTIONLIST
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONSET		SM_COMPLEXTYPE_COLLECTIONLIST + 1
#define	LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG		SM_COMPLEXTYPE_COLLECTIONLIST + 2
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST		SM_COMPLEXTYPE_COLLECTIONLIST + 3
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY	SM_COMPLEXTYPE_COLLECTIONLIST + 4

#define	LOM_SHORT	SM_SHORT
#define LOM_INT		SM_INT
#define LOM_LONG	SM_LONG
#define LOM_LONG_LONG	SM_LONG_LONG
#define	LOM_FLOAT	SM_FLOAT
#define	LOM_DOUBLE	SM_DOUBLE
#define LOM_STRING	SM_STRING
#define LOM_VARSTRING	SM_VARSTRING
#define LOM_PAGEID	SM_PAGEID
#define LOM_FILEID	SM_FILEID
#define LOM_INDEXID	SM_INDEXID
#define LOM_OID		SM_OID
#define	LOM_MBR		SM_MBR
#define LOM_REF		30
#define LOM_LINK	31
#define	LOM_TEXT	39
#define LOM_DATE	50
#define LOM_TIME	51
#define LOM_TIMESTAMP	52
#define LOM_INTERVAL	53

/* The following types are relevant to ODMG type system */
#define LOM_USHORT	54
#define LOM_ULONG	55
#define LOM_BOOLEAN	56
#define LOM_OCTET	57

#define LOM_OGIS_GEOMETRY               128
#define LOM_OGIS_POINT                  129
#define LOM_OGIS_LINESTRING             130
#define LOM_OGIS_POLYGON                131
#define LOM_OGIS_GEOMETRYCOLLECTION     132
#define LOM_OGIS_MULTIPOINT             133
#define LOM_OGIS_MULTILINESTRING        134
#define LOM_OGIS_MULTIPOLYGON           135

#define	LOM_SHORT_SIZE		SM_SHORT_SIZE
#define LOM_INT_SIZE		SM_INT_SIZE
#define LOM_LONG_SIZE		SM_LONG_SIZE
#define LOM_LONG_LONG_SIZE	SM_LONG_LONG_SIZE
#define	LOM_FLOAT_SIZE		SM_FLOAT_SIZE
#define	LOM_DOUBLE_SIZE		SM_DOUBLE_SIZE
#define LOM_PAGEID_SIZE		SM_PAGEID_SIZE
#define LOM_FILEID_SIZE		SM_FILEID_SIZE
#define LOM_INDEXID_SIZE	SM_INDEXID_SIZE
#define LOM_OID_SIZE		SM_OID_SIZE
#define	LOM_MBR_SIZE		SM_MBR_SIZE
#define LOM_REF_SIZE		SM_OID_SIZE


#define LOM_DATE_SIZE		sizeof(UFour_Invariable)
#define LOM_TIME_SIZE		sizeof(short)*6
#define LOM_TIMESTAMP_SIZE	(sizeof(UFour_Invariable) + sizeof(Two_Invariable)*6)
#define LOM_INTERVAL_SIZE	sizeof(double)
#define LOM_USHORT_SIZE		LOM_SHORT_SIZE
#define LOM_ULONG_SIZE		LOM_LONG_SIZE
#define LOM_BOOLEAN_SIZE	sizeof(LOM_Boolean)
#define LOM_OCTET_SIZE		sizeof(LOM_Octet)


#endif
