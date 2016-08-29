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
	OQL_types.hxx
*/


#ifndef _OQL_TYPES_HXX_
#define _OQL_TYPES_HXX_


/* type id definition for user-defined database classes */
#define DB_TYPE_userdefined     100
#define DB_TYPE_predefined	13
#define DB_TYPE_fundamental	7

/* type id definitions for atomic literal types */
#define DB_TYPE_void		-1		/* void */
#define ODMG_TYPE_int		SM_INT		/* 1: int */
#define ODMG_TYPE_uint		9		/* unsigned int */
#define ODMG_TYPE_long		SM_LONG		/* 2: long */
#define ODMG_TYPE_ulong		10		/* unsigned long */
#define ODMG_TYPE_short		SM_SHORT	/* 10 : short int */
#define ODMG_TYPE_ushort	8		/* unsigned short */
#define ODMG_TYPE_float		SM_FLOAT	/* 3 : float */
#define ODMG_TYPE_double	SM_DOUBLE	/* 4 : double */
#define ODMG_TYPE_boolean	12		/* boolean */
#define ODMG_TYPE_octet				/* octet */
#define	ODMG_TYPE_char		SM_STRING	/* 5 : char */
#define ODMG_TYPE_uchar		7		/* unsigned char */
#define	ODMG_TYPE_string
#define ODMG_TYPE_varstring	SM_VARSTRING   	/* 6 : variable-length string */
#define	ODMG_TYPE_enum

/* type id definitions for collection literal types */
#define	ODMG_TYPE_set
#define	ODMG_TYPE_bag
#define	ODMG_TYPE_list
#define	ODMG_TYPE_array

/* type id definitions for structured literal types */
#define	ODMG_TYPE_date
#define	ODMG_TYPE_time
#define	ODMG_TYPE_timestamp
#define	ODMG_TYPE_interval
#define	ODMG_TYPE_structure

/* type id definitions for object types */
#define	ODMG_TYPE_OID		SM_OID		/* 10 : OID of user-defined type */
#define DB_TYPE_ref		13		/* reference to user-defined type */
#define	ODMG_TYPE_d_set
#define	ODMG_TYPE_d_bag
#define	ODMG_TYPE_d_list
#define	ODMG_TYPE_d_varray


typedef	struct type_info {
	short		type;
	ClassInfo	classInfo;
	struct type_info *next;
} TypeInfo;


#endif _OQL_TYPES_HXX_
