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
* Description:  Header file for the BitSet class, a class designed
*               for representing bit valued sets of arbitrary size.
*
*
****************************************************************************/

#ifndef _OOSQL_TCL_BITSET_H_
#define _OOSQL_TCL_BITSET_H_

#include "OOSQL_Techlib.hxx"
#include <memory.h>

/*--------------------- Macros and type definitions -----------------------*/

// The following defines the smallest element in the set. By default this
// is an unsigned short, but you may change it to be an unsigned long for
// better performance on 32 bit machines.

#ifdef  _LONG_ELEMS

typedef unsigned long       _SETTYPE;
#define _BITS_IN_WORD       32
#define _BYTES_IN_ARRAY(x)  ((x) << 2)      // # of bytes in bit map
#define _DIV_WSIZE(x)       ((UFour)(x) >> 5)
#define _MOD_WSIZE(x)       ((x) & 0x1F)

#else

typedef unsigned short      _SETTYPE;       // one cell in bit map
#define _BITS_IN_WORD       16
#define _BYTES_IN_ARRAY(x)  ((x) << 1)      // # of bytes in bit map
#define _DIV_WSIZE(x)       ((UFour)(x) >> 4)
#define _MOD_WSIZE(x)       ((x) & 0x0F)

#endif

// Macro to modify the appropriate bit in the bitmap

#define _GBIT(m,b,op)   ( ((m)[_DIV_WSIZE(b)]) op (1 << _MOD_WSIZE(b)) )

/*--------------------------- Class Definition ----------------------------*/

//---------------------------------------------------------------------------
// The following template class defines a fixed bit set class, which is
// used for manipulating small sets of numbers that are not required to
// change in size over time.
//---------------------------------------------------------------------------

template <int nbits = 128> class OOSQL_TCFixedBitSet {
protected:
    bool        compl;          // True if set is complemented
    _SETTYPE    defmap[nbits / _BITS_IN_WORD];

            UFour inline defBits()  { return nbits; };
            UFour inline defWords() { return nbits / _BITS_IN_WORD; };
            UFour inline round(UFour bit)
                { return (((_DIV_WSIZE(bit) + 8) >> 3) << 3); };

    enum { UNION, INTERSECT, DIFFERENCE };

            // Method to perform above operations on sets
            void _set_op(const OOSQL_TCFixedBitSet<nbits>& bs1,
                const OOSQL_TCFixedBitSet<nbits>& bs2,int op);

public:
    enum { Equivalent, Disjoint, Intersect };

            // Constructor
            OOSQL_TCFixedBitSet();

            // Methods to add and remove elements
            void add(int bit);
            void remove(int bit);

            // Methods to test for membership in the set
            bool member(int bit);
            bool test(int bit)  { return member(bit) ? !compl : compl; };

            // Methods to determine equivalence of sets
            int testSet(const OOSQL_TCFixedBitSet<nbits>& bs);
            bool isDisjoint(const OOSQL_TCFixedBitSet<nbits>& bs)
                { return testSet(bs) == Disjoint; };
            bool isIntersecting(const OOSQL_TCFixedBitSet<nbits>& bs)
                { return testSet(bs) == Intersect; };
            bool isEquivalent(const OOSQL_TCFixedBitSet<nbits>& bs)
                { return testSet(bs) == Equivalent; };
            bool isSubset(const OOSQL_TCFixedBitSet<nbits>& bs);

            // Methods to compute set union, intersection and difference
            void setUnion(const OOSQL_TCFixedBitSet<nbits>& bs1,
                const OOSQL_TCFixedBitSet<nbits>& bs2) { _set_op(bs1,bs2,UNION); }
            void setIntersect(const OOSQL_TCFixedBitSet<nbits>& bs1,
                const OOSQL_TCFixedBitSet<nbits>& bs2) { _set_op(bs1,bs2,INTERSECT); };
            void setDifference(const OOSQL_TCFixedBitSet<nbits>& bs1,
                const OOSQL_TCFixedBitSet<nbits>& bs2) { _set_op(bs1,bs2,DIFFERENCE); };

            // Method to compare two sets
            int cmp(const OOSQL_TCFixedBitSet<nbits>& bs);

            // Return the cardinality of the set
            int numElements();

            // Miscellaneous methods
            void clear();
            void fill();
            void complement()   { compl = !compl; };
            void invert();
            bool isEmpty()  { return numElements() == 0; };

            // Method to print the set!!

            // operator <
            // operator <=
            // operator ==  for calls to set comparision function
            // operator >=
            // operator >

            // operator +=  add a member to the set
            // operator -=  remove a member from the set
            // operator |   test for set membership
            // operator ~   invert a set
            // operator +   union of two sets
            // operator -   difference of two sets
            // operator &   intersection of two sets

    };

// Need to include iterators for the sets to iterate over the elements in
// the set!!!

//---------------------------------------------------------------------------
// The following defines a general bit set class, for representing bit
// set of arbitrary size that expands and shrinks to fit the required
// allocation.
//---------------------------------------------------------------------------

class OOSQL_TCBitSet : public OOSQL_TCFixedBitSet<128> {
protected:
public:
    };

//---------------------------------------------------------------------------
// The following defines a character set class, for reprensenting sets of
// available characters.
//---------------------------------------------------------------------------

class OOSQL_TCCharSet : public OOSQL_TCFixedBitSet<255> {
protected:
public:
    };

/*------------------------ Inline member functions ------------------------*/

template <int nbits> inline OOSQL_TCFixedBitSet<nbits>::OOSQL_TCFixedBitSet()
/****************************************************************************
*
* Function:     OOSQL_TCFixedBitSet<nbits>::OOSQL_TCFixedBitSet
*
* Description:  Default constructor for the OOSQL_TCFixedBitSet class. This sets
*               up an empty set.
*
****************************************************************************/
{
    memset(defmap,0,sizeof(defmap));    // Clear out the set
    compl = false;
}

template <int nbits> void OOSQL_TCFixedBitSet<nbits>::add(int bit)
/****************************************************************************
*
* Function:     OOSQL_TCFixedBitSet<nbits>::add
* Parameters:   bit - Bit to add to the set
*
* Description:  Adds a bit to the set.
*
****************************************************************************/
{
    PRECONDITION(bit < nbits);
    _GBIT(defmap,bit,|=);
}

template <int nbits> void OOSQL_TCFixedBitSet<nbits>::remove(int bit)
/****************************************************************************
*
* Function:     OOSQL_TCFixedBitSet<nbits>::remove
* Parameters:   bit - Bit to remove from the set if present
*
* Description:  Adds a bit to the set.
*
****************************************************************************/
{
    PRECONDITION(bit < nbits);
    _GBIT(defmap,bit,&= ~);
}

template <int nbits> bool OOSQL_TCFixedBitSet<nbits>::member(int bit)
/****************************************************************************
*
* Function:     OOSQL_TCFixedBitSet<nbits>::member
* Parameters:   bit - Bit to test for membership in set
* Returns:      True if bit is a member, false if not.
*
****************************************************************************/
{
    PRECONDITION(bit < nbits);
    return _GBIT(defmap,bit,&);
}

#endif  // _OOSQL_TCL_BITSET_H_
