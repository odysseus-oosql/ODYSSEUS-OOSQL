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
/*    Fine-Granule Locking Version                                            */
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
 * Module: Util_Bit.c
 *
 * Description:
 *  Includes a set of bit operations.
 *
 * Exports:
 *  void Util_ClearBits(Four, register unsigned char*, register Four, Four)
 *  void Util_TestBitSet(Four, register unsigned char*, Four, Boolean*)
 *  void Util_TestBitsSet(Four, register unsigned char*, Four, Four, Boolean*)
 *  void Util_CountBitsSet(Four, register unsigned char*, Four, Four, Four*)
 *  void Util_FindBits(Four, register unsigned char*, Four, Four, Four, Four*)
 *  void Util_SetBits(Four, register unsigned char*, Four, Four)
 *  Four Util_PrintBits(Four, register unsigned char*, Four, Four)
 */


#include <stdio.h>
#include <limits.h>
#include "common.h"
#include "error.h"
#include "latch.h"
#include "Util.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Function: void Util_ClearBits(Four, register unsigned char*, register Four, Four)
 *
 * Description:
 *  clear n bits from the (start)th bit in the bit map
 *
 * Returns:
 *  Error code
 *    eNOERROR
 *
 * Side Effects:
 *  the n bits in the bit map page is cleared
 */
void Util_ClearBits(
    Four                   handle,
    register unsigned char *ptr, /* IN pointer to bit map */
    register Four	   start,  /* IN bit position */
    Four		   count)  /* IN count to be cleared */
{
    register Four	mask;	/* an integer for masking */
    Four		i;	/* loop index */


    /* set up the first byte and mask field */
    ptr += start / CHAR_BIT;
    start %= CHAR_BIT;
    mask = (1 << CHAR_BIT-1-start);

    /* search until the count becomes 0 */
    for (i=0; count>0; i++, (count)--) {

	/* set the bit */
	*ptr &= ~mask;

	if ((mask>>=1) == 0) /* advance to next bit */
        {
	    /* cross word boundary */
	    mask = (1 << CHAR_BIT-1);
	    ptr++;  /* advance map pointer */

	} /* end if */

    } /* end for */

} /* Util_ClearBits() */



/*
 * Function: void Util_TestBitSet(Four, register unsigned char*, Four, Boolean*)
 *
 * Description:
 *  given a position start, test whether the bit is set
 *
 * Returns:
 *  Error code
 *    eNOERROR
 *    eALREADYSETBIT_RDsM -  any of n bits are already
 */
void	Util_TestBitSet(
    Four                   handle,
    register unsigned char *ptr, /* IN pointer to bit map */
    Four		   start,  /* IN start position to be checked in this page */
    Boolean		   *flag)  /* OUT indicate whether the bit is set or not */
{
    register Four	mask;		/* an integer for masking */


    /* set up the first byte and mask field */
    ptr += (start) / CHAR_BIT;
    start %= CHAR_BIT;
    mask = (1 << CHAR_BIT-1-start);

    /* the bit set is found */
    if (*ptr & mask)
	*flag = TRUE;
    else
	*flag = FALSE;

} /* Util_TestBitSet() */



/*
 * Function: void Util_TestBitsSet(Four, register unsigned char*, Four, Four, Boolean*)
 *
 * Description:
 *  given a position start, test whether n bits are set
 *  however, this procedure is not used in this version of RDsM
 *
 * Returns:
 *  Error code
 */
void Util_TestBitsSet(
    Four                   handle,
    register unsigned char *ptr, /* IN pointer to bit map */
    Four		   start,  /* IN start position to be checked in this page */
    Four		   n,	   /* IN the number of bits to be checked */
    Boolean		   *flag)  /* OUT indicate whether the bit is set or not */
{
    register Four	mask;		/* an integer for masking */


    /* set up the first byte and mask field */
    ptr += (start) / CHAR_BIT;
    start %= CHAR_BIT;
    mask = (1 << CHAR_BIT-1-start);

    /* test whether any of n bits are already reset */
    while (--n >= 0) {

	/* the bit set is found */
	if (*ptr & mask) {
            *flag = TRUE;
            return;
        }

	if ((mask>>=1) == 0) {/* advance to next bit */
	    /* cross word boundary */
	    mask = (1 << CHAR_BIT-1);
	    ptr++;	/* advance map pointer */
	}

    } /* end while */

    *flag = FALSE;

} /* Util_TestBitsSet() */



/*
 * Function: void Util_CountBitsSet(Four, register unsigned char*, Four, Four, Four*)
 *
 * Description:
 *  given a position start, count the number of bits that are set
 *
 * Returns:
 *  Error code
 *    eNOERROR
 *    eALREADYSETBITRDsM -  any of n bits are already
 */
void Util_CountBitsSet(
    Four                   handle,
    register unsigned char *ptr, /* IN pointer to bit map */
    Four		   start,  /* IN start position to be checked in this page */
    Four		   n,	   /* IN the number of bits to be checked */
    Four                   *count) /* OUT the number of bits set */
{
    register Four	mask;		/* an integer for masking */


    /* set up the first byte and mask field */
    ptr += (start) / CHAR_BIT;
    start %= CHAR_BIT;
    mask = (1 << CHAR_BIT-1-start);

    /* initialize the return value */
    (*count) = 0;

    /* test whether any of n bits are already reset */
    while (--n >= 0) {

	/* the bit set is found */
	if (*ptr & mask)
	    (*count)++;

	if ((mask>>=1) == 0) {/* advance to next bit */
	    /* cross word boundary */
	    mask = (1 << CHAR_BIT-1);
	    ptr++;	/* advance map pointer */
	}

    } /* end while */

} /* Util_CountBitsSet() */




/*
 * Function: void Util_FindBits(Four, register unsigned char*, Four, Four, Four, Four*)
 *
 * Description:
 *  given the position start, find the n bits in the map
 *  the parameter 'ith' returns the ith position of the first set bit of the contiguous bis from start, NIL if no more
 *
 * Returns:
 *  Error code
 *    eNOERROR
 */
void	Util_FindBits(
    Four                    	handle,
    register unsigned char 	*ptr, 		/* IN pointer to bit map */
    Four	       		start,	   	/* IN start position to be checked in this page */
    Four	       		remains,   	/* IN the number of bits to be checked */
    Four	       		contiguous, 	/* IN the number of contiguous bits that the caller wants */
    Four	       		*ith)	   	/* OUT the first bit of the set bits */
{
    register Four	mask;	/* an integer for masking */
    Four		i;	/* loop index */
    Four		count=0; /* for counting the number of contigous bis */


    /* set up the first byte and mask field */
    ptr += (start) / CHAR_BIT;
    start %= CHAR_BIT;
    mask = (1 << CHAR_BIT-1-start);

    /* search until the remains becomes 0 */
    for (i=0; remains>0; i++, remains--) {

	/* the bit set is found */
	if (*ptr & mask) {

	    count++;
	    if (count == 1)
		*ith = i;
	    if (count == contiguous) return;

	}	/* end if */

	else
	    count = 0;

	if ((mask>>=1) == 0) /* advance to next bit */
        {
	    /* cross word boundary */
	    mask = (1 << CHAR_BIT-1);
	    ptr++;	/* advance map pointer */
 	}
    }

    /*
     * not found
     */
    *ith = NIL;

} /* Util_FindBits() */



/*
 * Function: void Util_SetBits(Four, register unsigned char*, Four, Four)
 *
 * Description:
 *  given the position pos, clear n bits in the map
 *
 * Returns:
 *  Error code
 *    eNOERROR
 *
 * Side Effects:
 *  set corresponding n bits to 1
 */
void Util_SetBits(
    Four                   handle,
    register unsigned char *ptr, /* IN pointer to bit map */
    Four		   start,  /* IN start position to be checked in this page */
    Four                   count)  /* IN the number of bits to be checked */
{
    register Four	mask;	/* an integer for masking */
    Four		i;	/* loop index */


    /* set up the first byte and mask field */
    ptr += start / CHAR_BIT;
    start %= CHAR_BIT;
    mask = (1 << CHAR_BIT-1-start);

    /* search until the count becomes 0 */
    for (i=0; count>0; i++, (count)--) {

	/* set the bit */
	*ptr |= mask;

	if ((mask>>=1) == 0) /* advance to next bit */
        {
	    /* cross word boundary */
	    mask = (1 << CHAR_BIT-1);
	    ptr++;	/* advance map pointer */
	}
    }

} /* Util_SetBits() */



/*
 * Function: Four Util_PrintBits(Four, register unsigned char*, Four, Four)
 *
 * Description:
 *  given the position pos, clear n bits in the map
 *
 * Returns:
 *  Error code
 *    eNOERROR
 *
 * Side Effects:
 *   print corresponding n bits
 */
Four Util_PrintBits(
    Four                   handle,
    register unsigned char *ptr, /* IN pointer to bit map */
    Four		   start,  /* IN start position to be checked in this page */
    Four		   count)  /* IN the number of bits to be checked */
{
    register Four	mask;	/* an integer for masking */
    Four		i;	/* loop index */


    /* set up the first byte and mask field */
    ptr += (start) / CHAR_BIT;
    start %= CHAR_BIT;
    mask = (1 << CHAR_BIT-1-start);

    /* search until the count becomes 0 */
    for (i=0; count>0; i++, (count)--) {

	/* check the bit */
	if ((*ptr & mask) == 0)
	    printf("%ld,", 0);
	else
	    printf("%ld,", 1);

	if ((mask>>=1) == 0) /* advance to next bit */
	{
	    /* cross word boundary */
	    mask = (1 << CHAR_BIT-1);
	    ptr++;	/* advance map pointer */
	}
    }
    printf("\n");

    return(eNOERROR);

} /* Util_PrintBits() */
