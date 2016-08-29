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
    MODULE:
    	OOSQL_ResultInfo.cxx

    DESCRIPTION:
    	This module implements member functions for the class OOSQL_ResultInfo.

    IMPORTS:

    EXPORTS:
*/


#include <stdio.h>
#include <string.h>
#include "OOSQL_ResultInfo.hxx"

#ifdef	_OOSQL_SERVER_
#include "OOSQL_Error.h"
#else defined _OQL_CLIENT_
#include "OOSQL_Error.h"
#endif


OOSQL_ResultInfo::OOSQL_ResultInfo(
	Four		numCols)		// IN: the # of query result columns
/*
    Function:
    	Constructor.

    Side effect:
    	Initialize member variables.

    Return value:
*/
{
	nVarCols = 0;
	firstVarColOffset = -1;		/* NOTE: if firstVarColOffset is -1, then it means
								 *	query result info is not filled up.
								 */
	colInfo = (OOSQL_ResultColInfo*)pMemoryManager->Alloc(sizeof(OOSQL_ResultColInfo) * numCols);
	nCols   = numCols;
}

OOSQL_ResultInfo::~OOSQL_ResultInfo()
{
	if(colInfo)
		pMemoryManager->Free(colInfo);
}


Four	OOSQL_ResultInfo::getMinResTupleSize(
	Four	&minSize
)
/*
    Function:
    	Calculate the min. result tuple size.

    	NOTE: min result tuple size =
    		( sum of length of all fixed-sized columns ) +
		( sum of header length of all fixed-sized columns )

    Side effect:

    Return value:
    	Min. result tuple size.
*/
{
	Four	i;		// loop iteration variable

	// check if result information is filled up
	if (nCols > 0 && firstVarColOffset < 0) {
#ifdef	_OOSQL_SERVER_
#else defined _OQL_CLIENT_
	    OQL_RETERR( eUNINITIALIZED_STRUCT_OQL);
#endif
	}

	for (minSize = 0, i = 0; i < nCols; i++) {
	    if (colInfo[i].typeId == OOSQL_TYPE_varstring) {
	    	minSize += VARCOL_HDRSIZE;
	    }
	    else {
	    	minSize += colInfo[i].length;
	    }
	}
	return eNOERROR;
}


OOSQL_ResultInfo*	OOSQL_ResultInfo::copy(
	OOSQL_ResultInfo	*resInfo	// IN: ptr. to be copied
)
/*
    Function:
    	copy members from resInfo.

    Side effect:
    	all members are copied from resInfo.

    Return value:
    	pointer to this object.
*/
{
	Four	i;

	nCols = resInfo->nCols;
	nVarCols = resInfo->nVarCols;
	firstVarColOffset = resInfo->firstVarColOffset;

	for (i = 0; i < nCols; i++) {
		colInfo[i] = resInfo->colInfo[i];
	}

	return this;
}


Four	OOSQL_ResultInfo::getResultInfoLengthInChar(
	Four	numCols		// IN: the # of result columns
)
/*
    Function:

    Side effect:

    Return value:
*/
{
	return (RESULTINFO_SIZE + (RESULTCOLINFO_SIZE * numCols));
}


void	OOSQL_ResultInfo::dump()
{
	int	i;
	char	typeName[20];

	fprintf(stderr,"\n  nCols = %ld", nCols);
	fprintf(stderr,"\n  nVarCols = %ld", nVarCols);
	fprintf(stderr,"\n  firstVarColOffset = %ld", firstVarColOffset);

	for (i = 0; i < nCols; i++) {
		fprintf(stderr,"\n  %ld-th column information", i);
		getTypeName(colInfo[i].typeId, &typeName[0]);
		fprintf(stderr,"\n    type ID = %s", typeName);
		fprintf(stderr,"\n    length = %ld", colInfo[i].length);
		fprintf(stderr,"\n    offset = %ld", colInfo[i].offset);
	}
}

Four	OOSQL_ResultInfo::getTypeName( Four typeId, char *typeName )
{
	switch (typeId) {
	    case OOSQL_TYPE_short:
		strcpy( typeName, "short" );
		break;
	    case OOSQL_TYPE_int:
		strcpy( typeName, "int" );
		break;
	    case OOSQL_TYPE_long:
		strcpy( typeName, "long" );
		break;
	    case OOSQL_TYPE_long_long:
		strcpy( typeName, "long long" );
		break;
	    case OOSQL_TYPE_float:
		strcpy( typeName, "float" );
		break;
	    case OOSQL_TYPE_double:
		strcpy( typeName, "double" );
		break;
	    case OOSQL_TYPE_TEXT:
		strcpy( typeName, "Text" );
		break;
	    case OOSQL_TYPE_OID:
		strcpy( typeName, "OID" );
		break;
	    case OOSQL_TYPE_string:
		strcpy( typeName, "string" );
		break;
	    case OOSQL_TYPE_varstring:
		strcpy( typeName, "varstring" );
		break;
	    default:
#ifdef	_OOSQL_SERVER_
#else defined _OQL_CLIENT_
	    	OQL_RETERR( eINVALID_COLTYPE_OQL);
#endif
			break;
	}

	return eNOERROR;
}

