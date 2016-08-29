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
* Description:  Header file for a OOSQL_TCHashTable class.
*
****************************************************************************/

#ifndef _OOSQL_TCL_HASHTAB_H_
#define _OOSQL_TCL_HASHTAB_H_

#include "OOSQL_Common.h"
#include <iostream>

class OOSQL_TCGenHashTable;

/*--------------------------- Class Definition ----------------------------*/

//---------------------------------------------------------------------------
// The OOSQL_TCHashTableNode class is a simple class used to link the objects in
// a single hash table bucket together. To put anything useful into the
// table, you must derive the object place in the table from OOSQL_TCHashTableNode.
//---------------------------------------------------------------------------

class OOSQL_TCHashTableNode {
protected:
    OOSQL_TCHashTableNode   *next;
    OOSQL_TCHashTableNode   **prev;

    friend class OOSQL_TCGenHashTable;
    friend  std::ostream& operator << (std::ostream& o,OOSQL_TCGenHashTable& h);
public:
            // Constructor to satisfy some compilers :-(
            OOSQL_TCHashTableNode() {};

            // Virtual destructor to delete a hash table node
    virtual ~OOSQL_TCHashTableNode();

            // Virtual member to compute the hash value of a symbol
    virtual UFour hash() const = 0;

            // Virtual equality operator for a hash table node and a key
    virtual bool operator == (const OOSQL_TCHashTableNode& key) const = 0;

            // Virtual member to display a hash table node
    virtual void printOn(std::ostream& o) const = 0;

            // Method to display a hash table node
    friend  std::ostream& operator << (std::ostream& o,OOSQL_TCHashTableNode& n)
            {
                n.printOn(o);
                return o;
            };
    };

//---------------------------------------------------------------------------
// The OOSQL_TCHashTable class is a class designed to hold a number of unordered
// objects together in a hash table for fast access. In the simple form,
// OOSQL_TCHashTableNode objects contain nothing special. To add an arbitrary class
// to the table, you must derive the class from OOSQL_TCHashTableNode (either
// through single or multiple inheritance).
//---------------------------------------------------------------------------

class OOSQL_TCGenHashTable {
protected:
    UFour           size;       // Size of hash table
    UFour           count;      // Number of objects in table
    OOSQL_TCHashTableNode   **table;    // Table of hash table node pointers

public:
            // Constructor
            OOSQL_TCGenHashTable(UFour tabsize = 0);

            // Destructor
            ~OOSQL_TCGenHashTable();

            // Member function to add a symbol to the table
            void add(OOSQL_TCHashTableNode* node);

            // Member function to remove a symbol from the table
            OOSQL_TCHashTableNode* remove(OOSQL_TCHashTableNode* node);

            // Member function to find a symbol in the table
            OOSQL_TCHashTableNode* find(OOSQL_TCHashTableNode* key) const;

            // Member function to find a symbol in the table (cached)
            OOSQL_TCHashTableNode* findCached(OOSQL_TCHashTableNode* key);

            // Member function to find the next symbol in chain
            OOSQL_TCHashTableNode* next(OOSQL_TCHashTableNode* last) const;

            // Empties the entire table by destroying all nodes.
            void empty();

            // Returns the number of items in the table
            UFour numberOfItems() const { return count; };

            // Returns true if the table is empty
            bool isEmpty() const        { return (count == 0)?true:false; };

            // Returns the load factor for the table
            UFour loadFactor() const    { return (size * 1000L) / count; };

            // Friend to display the contents of the hash table

    friend  std::ostream& operator << (std::ostream& o,OOSQL_TCGenHashTable& h);
    };

//---------------------------------------------------------------------------
// Template wrapper class for declaring Type Safe Hash Tables.
//---------------------------------------------------------------------------

template <class T> class OOSQL_TCHashTable : public OOSQL_TCGenHashTable {
public:
            OOSQL_TCHashTable(UFour tabsize = 0)
                : OOSQL_TCGenHashTable(tabsize){};
            T* remove(T* node)
                { return (T*)OOSQL_TCGenHashTable::remove(node); };
            T* find(T* key) const
                { return (T*)OOSQL_TCGenHashTable::find(key); };
            T* findCached(T* key)
                { return (T*)OOSQL_TCGenHashTable::findCached(key); };
            T* next(T* last) const
                { return (T*)OOSQL_TCGenHashTable::next(last); };
    };

/*-------------------------- Function Prototypes --------------------------*/

// Pre-defined hash routines for C style strings

UFour_Invariable   OOSQL_TCL_hashAdd(unsigned char *name);     // Simple and fast
UFour_Invariable   OOSQL_TCL_hashPJW(unsigned char *name);     // Better distribution of symbols
UFour_Invariable   OOSQL_TCL_hashSA(unsigned char *name);      // From Sedgewick's Algorithms
UFour_Invariable   OOSQL_TCL_hashGE(unsigned char *name);      // From Gosling's Emac's
UOne_Invariable    OOSQL_TCL_hash8(unsigned char *name);       // Randomised 8 bit hash value
UTwo_Invariable    OOSQL_TCL_hash16(unsigned char *name);      // Randomised 16 bit hash value
UFour_Invariable   OOSQL_TCL_hash32(unsigned char *name);      // Randomised 32 bit hash value

#endif  // _OOSQL_TCL_HASHTAB_H_
