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
/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
/*    Coarse-Granule Locking (Volume Lock) Version                            */
/*    Version 3.0                                                             */
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
 * Module: Util_varArray.c
 *
 * Description:
 *  This file contains those routines which handel the variable size array.
 *  Initially we allocated some chunk of memory to the variable array.
 *  If the space gets small by inserting many entries, we double the chunk
 *  size so that the array contain more entries.
 *
 * Exports:
 *  Four Util_initVarArray(VarArray*, Four, Four)
 *  Four Util_doublesizeVarArray(VarArray*, Four)
 *  Four Util_finalVarArray(VarArray*)
 */


#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/* internal function prototypes */
Four util_increasesize(Four, VarArray*, Four, Four);



/*@================================
 * Util_initVarArray()
 *================================*/
/*
 * Function: Four Util_initVarArray(VarArray*, Four, Four)
 *
 * Description:
 *  Initialize the variable size array.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER_UTIL
 *    eMEMORYALLOCERR_UTIL
 *    some errors caused by function calls
 */
Four Util_initVarArray(
    Four handle,
    VarArray *var_array,	/* IN variable array to initialize */
    Four size,			/* IN size of an element of the array */
    Four number)		/* IN number of elements at start time */
{
    TR_PRINT(TR_UTIL, TR1,
             ("Util_initVarArray(handle, var_array=%P, size=%ld, number=%ld)",
	      var_array, size, number));


    /*@ check parameter */
    if (var_array == NULL) ERR(handle, eBADPARAMETER);

    if (size < 0) ERR(handle, eBADPARAMETER);

    if (number <= 0) ERR(handle, eBADPARAMETER);

    var_array->ptr = (void *)malloc(number * size);

    if (var_array->ptr == NULL) ERR(handle, eMEMORYALLOCERR); 

    var_array->nEntries = number;

    return(eNOERROR);

} /* Util_initVarArray() */



/*@================================
 * util_increasesize()
 *================================*/
/*
 * Function: Four util_increase(VarArray*, Four, Four)
 *
 * Description:
 *  Increase the number of entries in the given array
 *
 * Returns
 *  eMEMORYALLOCERR_UTIL
 *  some errors caused by function calls
 */
Four util_increasesize(
    Four handle,
    VarArray *var_array,	/* IN variable array to increase its size */
    Four size,			/* IN entry size of the variable array */
    Four number)		/* IN # of entries to increase */
{
    void *temptr;


    TR_PRINT(TR_UTIL, TR1,
             ("util_increasesize(handle, var_array=%P, size=%ld, number=%ld)",
	      var_array, size, number));

    temptr = (void *)realloc(var_array->ptr, (var_array->nEntries + number) * size);

    if (temptr == NULL) ERR(handle, eMEMORYALLOCERR);

    var_array->nEntries += number;
    var_array->ptr = temptr;

    return(eNOERROR);

} /* util_increasesize() */



/*@================================
 * Util_reallocVarArray()
 *================================*/
/*
 * Function: Four Util_reallocVarArray(VarArray*, Four, Four)
 *
 * Description:
 *  Reallocate the variable-length array
 */
Four Util_reallocVarArray(
    Four handle,
    VarArray 		*var_array,	/* IN variable array to increase its size */
    Four 		size,		/* IN entry size of the variable array */
    Four 		number)		/* IN requested number of entries */
{
    Four 		e;
    Four                nNeededEntries;


    TR_PRINT(TR_UTIL, TR1,
             ("Util_reallocVarArray(handle, var_array=%P, size=%ld, number=%ld)",
	      var_array, size, number));

    nNeededEntries = var_array->nEntries;

    if (nNeededEntries >= number) return(eNOERROR);

    do {
        nNeededEntries *= 2;
    } while (nNeededEntries < number);

    e = util_increasesize(handle, var_array, size, nNeededEntries - var_array->nEntries);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* Util_reallocVarArray() */



/*@================================
 * Util_doublesizeVarArray()
 *================================*/
/*
 * Function: Four Util_doublesizeVarArray(VarArray*, Four)
 *
 * Description:
 *  Double the size of the variable array.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER_UTIL
 *    some errors caused by function calls
 */
Four Util_doublesizeVarArray(
    Four handle,
    VarArray *var_array,	/* IN the variable array to double */
    Four size)			/* IN the entry size */
{
    Four e;			/* error number */


    TR_PRINT(TR_UTIL, TR1,
             ("Util_doublesizeVarArray(handle, var_array=%P, size=%ld)", var_array, size));

    /*@ check parameter */
    if (var_array == NULL) ERR(handle, eBADPARAMETER);

    if (size < 0) ERR(handle, eBADPARAMETER);


    e = util_increasesize(handle, var_array, size, var_array->nEntries);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* Util_doublesizeVarArray() */



/*@================================
 * Util_finalVarArray()
 *================================*/
/*
 * Function: Four Util_finalVarArray(VarArray*)
 *
 * Description:
 *  Finalize the variable array. We deallocate the allocated memory.
 *
 * Returns:
 *  eBADPARAMETER_UTIL
 *  eMEMORYFREEERR_UTIL
 *  some errors caused by function calls
 */
Four Util_finalVarArray(
    Four     handle,
    VarArray *var_array)	/* IN variable array to finalize */
{
    Four e;			/* error number */

    TR_PRINT(TR_UTIL, TR1, ("Util_finalVarArray(handle, var_array=%P)", var_array));


    /*@ check parameter */
    if (var_array == NULL) ERR(handle, eBADPARAMETER);

    /* deallocate the allocated memory */
    (void) free(var_array->ptr);

    return(eNOERROR);

} /* Util_finalVarArray() */
