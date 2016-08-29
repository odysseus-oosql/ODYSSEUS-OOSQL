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

#ifndef SLIMDOWN_TEXTIR

/*
    MODULE:
        oosql_Eval_TextIR_DS.cxx

    DESCRIPTION:
        This module implements evaluation data structures for Text IR.

    IMPORTS:

    EXPORTS:
*/


/*
 * include files
 */
#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_TextIR.hxx"
#include "oosql_Eval_TextIR_Index.hxx"
#include <string.h>

oosql_TextIR_TempFileInfo::oosql_TextIR_TempFileInfo()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    /*
     * initialize column information
     */

    nCols = TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION;

    /* column 0: logical doc. ID */
    attrInfo[0].complexType = SM_COMPLEXTYPE_BASIC;
    attrInfo[0].type = OOSQL_TYPE_LONG_VAR;
    attrInfo[0].length = OOSQL_TYPE_LONG_SIZE_VAR;
	strcpy(attrInfo[0].name, "");		// NO NAME
	attrInfo[0].domain = OOSQL_TYPE_LONG_VAR;

    /* column 1: doc. ID */
    attrInfo[1].complexType = SM_COMPLEXTYPE_BASIC;
    attrInfo[1].type = OOSQL_TYPE_OID;
    attrInfo[1].length = OOSQL_TYPE_OID_SIZE;
	strcpy(attrInfo[1].name, "");		// NO NAME
	attrInfo[1].domain = OOSQL_TYPE_OID;

    /* column 2: weight */
    attrInfo[2].complexType = SM_COMPLEXTYPE_BASIC;
    attrInfo[2].type = OOSQL_TYPE_FLOAT;
    attrInfo[2].length = OOSQL_TYPE_FLOAT_SIZE;
	strcpy(attrInfo[2].name, "");		// NO NAME
	attrInfo[2].domain = OOSQL_TYPE_FLOAT;

    /* 
     * initialize column list structure 
     */
    /* column 0: logical doc. ID */
    clist[0].colNo = 0;
    clist[0].start = ALL_VALUE;
    clist[0].length = TEXTIR_SIZE_LOGICALDOCID;
    clist[0].dataLength = TEXTIR_SIZE_LOGICALDOCID;

    /* column 1: doc. ID */
    clist[1].colNo = 1;
    clist[1].start = ALL_VALUE;
    clist[1].length = TEXTIR_SIZE_DOCID;
    clist[1].dataLength = TEXTIR_SIZE_DOCID;

    /* column 2: weight */
    clist[2].colNo = 2;
    clist[2].start = ALL_VALUE;
    clist[2].length = TEXTIR_SIZE_WEIGHT;
    clist[2].dataLength = TEXTIR_SIZE_WEIGHT;

	sortBufferPtr				= NULL;
	sortBufferLength			= 0;
	sortBufferCurrentReadOffset	= 0;

	sortStream = NULL;
}

OOSQL_TextIR_SubPlanEvalBuffer::OOSQL_TextIR_SubPlanEvalBuffer(
        Four    poolSize                // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    int i;

    nBufElem = (poolSize > 0)? poolSize: 0;

	if(nBufElem > 0)
	{
		OOSQL_ARRAYNEW(evalBufferPool, pMemoryManager, oosql_TextIR_SubPlanEvalBufferElem, nBufElem);
	}
	else
	{
		evalBufferPool = NULL;
	}
}

OOSQL_TextIR_SubPlanEvalBuffer::~OOSQL_TextIR_SubPlanEvalBuffer()
{
	if(evalBufferPool)
		OOSQL_ARRAYDELETE(oosql_TextIR_SubPlanEvalBufferElem, evalBufferPool);
}

#else  /* SLIMDOWN_TEXTIR */

#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_TextIR.hxx"
#include "oosql_Eval_TextIR_Index.hxx"
#include <string.h>

oosql_TextIR_TempFileInfo::oosql_TextIR_TempFileInfo()
{
    ;
}

OOSQL_TextIR_SubPlanEvalBuffer::OOSQL_TextIR_SubPlanEvalBuffer(
	Four    poolSize                // IN:
)
{
    ;
}

OOSQL_TextIR_SubPlanEvalBuffer::~OOSQL_TextIR_SubPlanEvalBuffer()
{
    ;
}

#endif /* SLIMDOWN_TEXTIR */

