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

#ifndef	_QP_Common_hxx_
#define	_QP_Common_hxx_

/*
    MODULE:
    	QP_Common.hxx

    DESCRIPTION:
    	This header file defines commonly used constants and structures for both
    	OQL client and OOSQL server.
*/


/**************************************
 * commonly used constant definitions *
 **************************************/

// maximum query string length
#ifndef	MAXQUERYSTRING
#define	MAXQUERYSTRING	256
#endif

// value returned from server
#define ENDOFEVAL       1       // end of evaluation


/*
 * constant and macro definitions for Client/Server communication
 */
// default communication buffer size to transmit query result information
#define QUERYRESULTINFOSIZE_OOSQL       1024
#define	QUERYRESULTBUFSIZE_OOSQL		4096

// define constant for header length of variable sized column
#define VARCOL_HDRSIZE  		sizeof(Four)

#define	QUERYRESULT_BUFHDRSIZE_OOSQL	sizeof(Four)	// for the # of query result tuples

// query result tuple header size
#define	GET_TUPLEHEADERSIZE(nVarCols)	(nVarCols * VARCOL_HDRSIZE)

// base offset of first fixed-sized column
#define	GET_FIRSTFIXEDCOLOFFSET(nVarCols)	GET_TUPLEHEADERSIZE(nVarCols)

/*
 * constant definitions for basic types
 */

#define	OOSQL_TYPE_short	0	// LOM_SHORT
#define	OOSQL_TYPE_int		1	// LOM_INT
#define	OOSQL_TYPE_long		2	// LOM_LONG
#define	OOSQL_TYPE_long_long	14	// LOM_LONG
#define	OOSQL_TYPE_float	3	// LOM_FLOAT
#define	OOSQL_TYPE_double	4	// LOM_DOUBLE
#define	OOSQL_TYPE_string	5	// LOM_STRING
#define	OOSQL_TYPE_varstring	6	// LOM_VARSTRING
#define	OOSQL_TYPE_OID		10	// LOM_OID
#define	OOSQL_TYPE_MBR		12	// LOM_MBR
#define	OOSQL_TYPE_Ref		13	// DB_TYPE_db_Ref in "DB_TypeId.hxx" 
#define	OOSQL_TYPE_TEXT		39	// LOM_TEXT

#define OOSQL_TYPE_DATE      50 // LOM_DATE
#define OOSQL_TYPE_TIME      51 // LOM_TIME
#define OOSQL_TYPE_TIMESTAMP 52 // LOM_TIMESTAMP
#define OOSQL_TYPE_INTERVAL  53 // LOM_INTERVAL

#define	OOSQL_SIZE_short	sizeof(short)	// SM_SHORT_SIZE
#define	OOSQL_SIZE_int		sizeof(int)	// SM_INT_SIZE
#define	OOSQL_SIZE_long		sizeof(int)	// SM_LONG_SIZE
#define	OOSQL_SIZE_float	sizeof(float)	// SM_FLOAT_SIZE
#define	OOSQL_SIZE_double	sizeof(double)	// SM_DOUBLE_SIZE
#define	OOSQL_SIZE_OID		sizeof(OID)	// SM_OID_SIZE


#endif
