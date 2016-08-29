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
* Description:  Header file for a GEO_OIDHashTable class.
*
****************************************************************************/

#ifndef	__GEO_OIDHASHTABLE_HXX
#define	__GEO_OIDHASHTABLE_HXX

#include "GEO_Internal.h"

/*--------------------------- Class Definition ----------------------------*/

//---------------------------------------------------------------------------
// The GEO_OIDHashTableNode class is a simple class used to link the objects in
// a single hash table bucket together. To put anything useful into the
// table, you must derive the object place in the table from GEO_OIDHashTableNode.
//---------------------------------------------------------------------------

class GEO_OIDHashTableNode {
protected:
    GEO_OIDHashTableNode   *next;
    GEO_OIDHashTableNode   **prev;
	OID						oid;

    friend class GEO_OIDHashTable;
public:
            // Constructor to satisfy some compilers :-(
            GEO_OIDHashTableNode() {};

            // Constructor to satisfy some compilers :-(
            GEO_OIDHashTableNode(OID oid2)
			{
				oid = oid2;
			};

            // Constructor to satisfy some compilers :-(
            GEO_OIDHashTableNode(TupleID tid)
			{
				oid.pageNo = tid.pageNo;
				oid.volNo = tid.volNo;
				oid.slotNo = tid.slotNo;
				oid.unique = tid.unique;
				oid.classID = 0;
			};

			void setOID(OID oid2)
			{
				oid = oid2;
			};

            // Virtual destructor to delete a hash table node
			~GEO_OIDHashTableNode();

            // Virtual member to compute the hash value of a symbol
			UFour hash() const
			{
				return oid.pageNo + oid.volNo + oid.slotNo + oid.unique;
			};

            // Virtual equality operator for a hash table node and a key
			bool operator == (const GEO_OIDHashTableNode& key) const
			{
				if( oid.pageNo  == key.oid.pageNo &&
					oid.volNo   == key.oid.volNo  &&
					oid.slotNo  == key.oid.slotNo &&
					oid.unique  == key.oid.unique  )
					return 1;
				return 0;
			};
            // Virtual member to display a hash table node
			void print() const
			{
				printf("pageNo: %d, volNo: %d, slotNo: %d, unique: %d, classID: %d\n", oid.pageNo, oid.volNo, oid.slotNo, oid.unique, oid.classID);
			};
			
    };

//---------------------------------------------------------------------------
// The GEO_OIDHashTable class is a class designed to hold a number of unordered
// objects together in a hash table for fast access. In the simple form,
// GEO_OIDHashTableNode objects contain nothing special. To add an arbitrary class
// to the table, you must derive the class from GEO_OIDHashTableNode (either
// through single or multiple inheritance).
//---------------------------------------------------------------------------

class GEO_OIDHashTable {
protected:
    UFour           size;       // Size of hash table
    UFour           count;      // Number of objects in table
    GEO_OIDHashTableNode   **table;    // Table of hash table node pointers

public:
            // Constructor
            GEO_OIDHashTable();
            GEO_OIDHashTable(UFour tabsize);

            // Destructor
            ~GEO_OIDHashTable();

            // Member function to add a symbol to the table
            void add(GEO_OIDHashTableNode* node);

            // Member function to remove a symbol from the table
            GEO_OIDHashTableNode* remove(GEO_OIDHashTableNode* node);

            // Member function to find a symbol in the table
            GEO_OIDHashTableNode* find(GEO_OIDHashTableNode* key) const;

            // Member function to find a symbol in the table (cached)
            GEO_OIDHashTableNode* findCached(GEO_OIDHashTableNode* key);

            // Member function to find the next symbol in chain
            GEO_OIDHashTableNode* next(GEO_OIDHashTableNode* last) const;

            // Empties the entire table by destroying all nodes.
            void empty();

            // Returns the number of items in the table
            UFour numberOfItems() const { return count; };

            // Returns true if the table is empty
            bool isEmpty() const        { return (count == 0)?true:false; };

            // Returns the load factor for the table
            UFour loadFactor() const    { return (size * 1000L) / count; };

            // Friend to display the contents of the hash table
			void print() const;
};

#endif  // __GEO_OIDHASHTABLE_HXX
