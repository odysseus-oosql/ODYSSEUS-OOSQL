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

/****************************************************************************
*
* Description:	C implementation for the pre-defined hashing routines
*				for the class library.
*
****************************************************************************/

#include "OOSQL_Hashtable.hxx"
#include "OOSQL_TCError.hxx"

/*---------------------------- Implementation -----------------------------*/

UFour_Invariable OOSQL_TCL_hashAdd(unsigned char *name)
/****************************************************************************
*
* Function:		OOSQL_TCL_hashAdd
* Parameters:	name	- String to hash
* Returns:		hash value of the string
*
* Description:	This hash function simply adds together the characters
*				in the name.
*
****************************************************************************/
{
	UFour_Invariable	h;

	for (h = 0; *name; h += *name++)
		;
	return h;
}

#if		defined(__16BIT__)
#define	NBITS_IN_UNSIGNED		16
#else
#define	NBITS_IN_UNSIGNED		32
#endif
#define SEVENTY_FIVE_PERCENT	((int)(NBITS_IN_UNSIGNED * .75))
#define	TWELVE_PERCENT			((int)(NBITS_IN_UNSIGNED * .125))
#define	HIGH_BITS				( ~( (unsigned)(~0) >> TWELVE_PERCENT) )

UFour_Invariable OOSQL_TCL_hashPJW(unsigned char *name)
/****************************************************************************
*
* Function:		TCL_hashPJW
* Parameters:	name	- String to hash
* Returns:		hash value for the string
*
* Description:	This hash function uses a shift-and-XOR strategy to
*				randomise the input key. The main iteration of the loop
*				shifts the accumulated hash value to the left by a few
*				bits and adds in the current character. When the number
*				gets too large, it is randomised by XORing it with a
*				shifted version of itself.
*
****************************************************************************/
{
	UFour_Invariable	h = 0;			// The hash value
	UFour_Invariable	g;

	for (; *name; name++) {
		h = (h << TWELVE_PERCENT) + *name;
		if ((g = (h & HIGH_BITS)) != 0)
			h = (h ^ (g >> SEVENTY_FIVE_PERCENT)) ^ g;
		}

	return h;
}

UFour_Invariable OOSQL_TCL_hashSA(unsigned char *name)
/****************************************************************************
*
* Function:		TCL_hashSA
* Parameters:	name	- String to hash
* Returns:		hash value for the string
*
* Description:	This hash function was taken from Sedgewick's Algorithms
*				textbook.
*
****************************************************************************/
{
	UFour_Invariable	h = *name++;

	while (*name)
		h = (h << 7) + *name++;
	return h;
}

UFour_Invariable OOSQL_TCL_hashGE(unsigned char *name)
/****************************************************************************
*
* Function:		TCL_hashGE
* Parameters:	name	- String to hash
* Returns:		hash value for the string
*
* Description:	This hash function was taken from Gosling's Emac's.
*
****************************************************************************/
{
	UFour_Invariable	h = 0;

	while (*name)
		h = (h << 5) - h + *name++;
	return h;
}

static unsigned char permutation[256] = {
	 51, 140, 233,  27, 118, 125, 170, 138,
	119, 132, 174,  97,  25, 110,   1,  14,
	 65,  36,  40, 188,  73, 173,   7,  30,
	 68,  56, 169, 234, 107, 177, 197,  87,
	 28, 210, 186,  67,   2,  15, 115,  48,
	223, 148, 211,  57, 190, 104, 213,  49,
	144, 172, 147, 124, 157, 238, 167, 183,
	 78,  75,  58,  22,  70, 103, 181,  12,
	254,  41, 198, 168,  46,  79, 241, 156,
	 83, 128,  66,  60,  86, 141, 161, 176,
	221,  54, 192, 252, 116,  95, 206,  35,
	 88, 133, 154, 250, 237, 253,  85, 178,
	 93, 159, 155,  42,   9,  89,   3,  61,
	201, 158, 106,  82, 240, 255, 218, 102,
	189,   8,  33,   4, 145,  16, 150,  26,
	 99, 100, 195, 175,  34,  50,  80, 166,
	194, 195, 164,  29, 134, 105,  55, 143,
	122, 130, 245, 208,  72,  77,  64, 121,
	139, 232, 191, 108, 228, 137,  59,  74,
	 11, 126, 171,   5, 242, 101, 239, 193,
	112, 113,  98,  21, 207, 225, 151, 251,
	 92,  91,  17, 127,  20,  81,  24,   6,
	 43, 196, 204, 247, 212, 224, 220,  94,
	 32,  13, 187, 199, 214,  18, 226,  84,
	 71, 231, 165,  19, 202, 217,  90, 129,
	136, 153, 182, 111, 244,  45, 236, 249,
	109,  47, 180, 205, 215, 160,  53, 162,
	114, 246, 179,  62, 227,  96, 142, 230,
	184, 146, 117,  39,  69,  37,  23,  63,
	 52, 216,   0, 135, 149,  31,  38,  44,
	209, 120,  76, 203, 229, 123, 131, 152,
	 10, 219, 243, 248, 235, 222, 200, 163,
	};

unsigned char OOSQL_TCL_hash8(unsigned char *name)
/****************************************************************************
*
* Function:		TCL_hash8
* Parameters:	name	- String to hash
* Returns:		hash value for the string
*
* Description:	Returns a randomised hash value distributed over an 8 bit
*				number.
*
****************************************************************************/
{
	unsigned char h;

	if (!*name) return 0;
	h = permutation[*name];
	while (*++name)
		h ^= permutation[h ^ *name];
	return h;
}

UTwo_Invariable OOSQL_TCL_hash16(unsigned char *name)
/****************************************************************************
*
* Function:		TCL_hash16
* Parameters:	name	- String to hash
* Returns:		hash value for the string
*
* Description:	Returns a randomised hash value distributed over a 16 bit
*				number.
*
****************************************************************************/
{
	unsigned char	h1,h2;

	if (!*name) return 0;
	h1 = permutation[*name];
	h2 = permutation[*name + 1];
	while (*++name) {
		h1 ^= permutation[h1 ^ *name];
		h2 ^= permutation[h2 ^ *name];
		}
	return (UTwo_Invariable)(((UTwo_Invariable)h1 << 8) | h2);
}

UFour_Invariable OOSQL_TCL_hash32(unsigned char *name)
/****************************************************************************
*
* Function:		TCL_hash32
* Parameters:	name	- String to hash
* Returns:		hash value for the string
*
* Description:	Returns a randomised hash value distributed over a 32 bit
*				number.
*
****************************************************************************/
{
	unsigned char	h1,h2,h3,h4;

	if (!*name) return 0;
	h1 = permutation[*name];
	h2 = permutation[*name + 1];
	h3 = permutation[*name + 2];
	h4 = permutation[*name + 3];
	while (*++name) {
		h1 ^= permutation[h1 ^ *name];
		h2 ^= permutation[h2 ^ *name];
		h3 ^= permutation[h3 ^ *name];
		h4 ^= permutation[h4 ^ *name];
		}
	return (((UFour_Invariable)h1 << 24) | ((UFour_Invariable)h2 << 16) | ((UFour_Invariable)h3 << 8) | h4);
}
