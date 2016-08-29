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
 * Module: RDsM_bit.c
 *
 * Description:
 *
 *
 * Exports:
 *  void RDsM_clear_bits(PageType*, register Four, Four)
 *  Four RDsM_test_n_bits_set(PageType*, Four, Four)
 *  Four RDsM_find_bits(PageType*, Four, Four, Four)
 *  void RDsM_set_bits(PageType*, Four, Four)
 *  void RDsM_print_bits(PageType*, Four, Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * RDsM_clear_bits()
 *================================*/
/*
 * Function: void RDsM_clear_bits(register unsigned char, register Four, Four)
 *
 * Description:
 *  clear n bits from the (start)th bit in the bit map
 *
 * Returns:
 *  error code
 *
 * Side Effects:
 *  the n bits in the bit map page is cleared
 */
void RDsM_clear_bits(
    Four						handle,
    PageType        			*aPage,         /* IN pointer to a page */
    register Four				start,          /* IN bit position */
    Four		    			count)          /* IN count to be cleared */
{
    register Four_Invariable    mask;			/* an integer for masking */
    unsigned char               *ptr;			/* pointer to bit map */


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_clear_bits(handle, aPage=%P, start=%lD, count=%lD)", aPage, start, count));


    /* set up starting bit position */
    ptr = (unsigned char *) aPage->ch.mp;

    /* set up the first byte and mask field */
    ptr += start / BITSPERBYTE;
    start %= BITSPERBYTE;
    mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1-start));

    /*@ search until the count becomes 0 */
    for (; count>0; (count)--) {

	/* set the bit */
	*ptr &= ~mask;

        if ((mask>>=CONSTANT_ONE) == 0) /* advance to next bit */
	    {
		/* cross word boundary */
		mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1));
		ptr++;  /* advance map pointer */

	    }	/* end if */

    }	/* end for */

} /* RDsM_clear_bits() */



/*@================================
 * RDsM_test_n_bits_set()
 *================================*/
/*
 * Function: Four RDsM_test_n_bits_set(register unsigned char*, Four, Four)
 *
 * Description:
 *  given a position start, test where n bits are set
 *
 * Returns:
 *  error code
 *    eALREADYSETBITRDsM -  any of n bits are already
 */
Four	RDsM_test_n_bits_set(
    Four 						handle,
    PageType                    *aPage,         /* IN pointer to a page */
    Four		        		start,          /* IN start position to be checked in this page */
    Four		        		n)	        	/* IN the number of bits to be checked */
{
    register Four_Invariable	mask;	        /* an integer for masking */
    unsigned char	        	*ptr;	        /* pointer to bit map */


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_test_n_bits_set(handle, aPage=%P, start=%lD, n=%lD)", aPage, start, n));


    /* set up starting bit position */
    ptr = (unsigned char *) aPage->ch.mp; 

    /* set up the first byte and mask field */
    ptr += (start) / BITSPERBYTE;
    start %= BITSPERBYTE;
    mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1-start));

    /*@ test whether any of n bits are already reset */
    while (--n >= 0) {

	/* the bit set is found */
        if (*ptr & mask) ERR(handle, eALREADYSETBIT_RDSM);

        if ((mask>>=CONSTANT_ONE) == 0) {/* advance to next bit */
	    /* cross word boundary */
	    mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1));
	    ptr++;	/* advance map pointer */
	}

    } /* end while */

    return(eNOERROR);

} /* RDsM_test_n_bits_set() */



/*@================================
 * RDsM_find_bits()
 *================================*/
/*
 * Function: Four RDsM_find_bits(register unsigned char*, Four, Four, Four)
 *
 * Description:
 *  given the position start, find the n bits in the map
 *
 * Returns:
 *  the ith position of the first set bit of the contiguous bis from start, NIL if no more
 */
Four	RDsM_find_bits(
    Four						handle,
    PageType            		*aPage,         /* IN pointer to a page */
    Four               			start,          /* IN start position to be checked in this page */
    Four		        		remains,        /* IN the number of bits to be checked */
    Four		        		contiguous)     /* IN the number of contiguous bits that the caller wants */
{
    register Four_Invariable    mask;			/* an integer for masking */
    unsigned char               *ptr;			/* pointer to bit map */
    Four	                	i;				/* loop index */
    Four	                	ith;			/* the first bit position of the n contiguous set bits */
    Four	                	count=0;	/* for counting the number of contigous bis */


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_find_bits(handle, aPage=%P, start=%lD, remains=%lD, contiguous=%lD)", aPage, start, remains, contiguous));


    /* set up starting bit position */
    ptr = (unsigned char *) aPage->ch.mp;

    /* set up the first byte and mask field */
    ptr += (start) / BITSPERBYTE;
    start %= BITSPERBYTE;
    mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1-start));

    /*@ search until the remains becomes 0 */
    for (i=0; remains>0; i++, remains--) {

	/* the bit set is found */
	if (*ptr & mask) {

	    count++;
	    if (count == 1)
		ith = i;
	    if (count == contiguous)
		return (ith);

	}	/* end if */

	else
	    count = 0;

        if ((mask>>=CONSTANT_ONE) == 0) /* advance to next bit */
	    {
		/* cross word boundary */
		mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1));
		ptr++;	/* advance map pointer */
	    }
    }

    return(NIL); /* not found */

} /* RDsM_find_bits() */



/*@================================
 * RDsM_set_bits()
 *================================*/
/*
 * Function: void RDsM_set_bits(register unsigned char*, Four, Four)
 *
 * Description:
 *  given the position pos, clear n bits in the map
 *
 * Returns:
 *  error code
 *
 * Side Effects:
 *  set corresponding n bits to 1
 */
void RDsM_set_bits(
    Four						handle, 
    PageType                	*aPage,         /* IN pointer to a page */
    Four		        		start,          /* IN start position to be checked in this page */
    Four		        		count)          /* IN the number of bits to be checked */
{
    register Four_Invariable	mask;	        /* an integer for masking */
    unsigned char	        	*ptr;	        /* pointer to bit map */
    

    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_set_bits(handle, aPage=%P, start=%lD, count=%lD)", aPage, start, count));


    /* set up starting bit position */
    ptr = (unsigned char *) aPage->ch.mp; 

    /* set up the first byte and mask field */
    ptr += start / BITSPERBYTE;
    start %= BITSPERBYTE;
    mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1-start));

    /*@ search until the count becomes 0 */
    for (; count>0; (count)--) {

	/* set the bit */
	*ptr |= mask;

        if ((mask>>=CONSTANT_ONE) == 0) /* advance to next bit */
	    {
		/* cross word boundary */
		mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1));
		ptr++;	/* advance map pointer */
	    }
    }

} /* RDsM_set_bits() */



/*@================================
 * RDsM_print_bits()
 *================================*/
/*
 * Function: void RDsM_print_bits(register unsigned char*, Four, Four)
 *
 * Description:
 *  given the position pos, clear n bits in the map
 *
 * Returns:
 *  error code
 *
 * Side Effects:
 *  print corresponding n bits
 */
void RDsM_print_bits(
    Four						handle,
    PageType                    *aPage,         /* IN pointer to a page */
    Four	                	start,          /* IN start position to be checked in this page */
    Four		        		count)          /* IN the number of bits to be checked */
{
    register Four_Invariable	mask;	        /* an integer for masking */
    unsigned char	        	*ptr;	        /* pointer to bit map */


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_print_bits(handle, aPage=%P, start=%lD, count=%lD)", aPage, start, count));


    /* set up starting bit position */
    ptr = (unsigned char *) aPage->ch.mp;

    /* set up the first byte and mask field */
    ptr += (start) / BITSPERBYTE;
    start %= BITSPERBYTE;
    mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1-start));

    /*@ search until the count becomes 0 */
    for (; count>0; (count)--) {

	/*@ check the bit */
	if ((*ptr & mask) == 0)
/*          printf(" %ld,", 0); */
            printf("%ld", 0);
	else
/*          printf(" %ld,", 1); */
            printf("%ld", 1);

        if ((mask>>=CONSTANT_ONE) == 0) /* advance to next bit */
	    {
		/* cross word boundary */
		mask = (((Four_Invariable)CONSTANT_ONE) << (BITSPERBYTE-1));
		ptr++;	/* advance map pointer */
	    }
    }
    printf("\n");

} /* RDsM_print_bits() */
