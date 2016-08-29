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
* Description:	Member functions for the HashTable class, a class designed
*				to map a bunch of unsorted objects into a hash table for
*				fast and efficient access.
*
****************************************************************************/

#include "OOSQL_Hashtable.hxx"
#include "OOSQL_TCError.hxx"
#include <stdio.h>

/*--------------------------- Member functions ----------------------------*/

OOSQL_TCGenHashTable::OOSQL_TCGenHashTable(UFour tabsize)
/****************************************************************************
*
* Function:		OOSQL_TCGenHashTable::OOSQL_TCGenHashTable
* Parameters:	tabsize	- Size of the hash table to construct
*
* Description:	Creates a new hash table of the given size. Note that
*				for best performance, tabsize should be a prime number.
*
****************************************************************************/
{
	if (tabsize == 0)
		size = 127;			// Default size
	else
		size = tabsize;

	// Allocate memory for the table.

	table = new OOSQL_TCHashTableNode*[size];
	for (UFour i = 0; i < size; i++)
		table[i] = NULL;
	count = 0;
}

OOSQL_TCGenHashTable::~OOSQL_TCGenHashTable()
/****************************************************************************
*
* Function:		OOSQL_TCGenHashTable::~OOSQL_TCGenHashTable
*
* Description:	Destructor for hash table. We simply empty the hash table
*				of all symbols and then delete any memory used.
*
****************************************************************************/
{
	empty();
	delete [] table;
}

void OOSQL_TCGenHashTable::empty(void)
/****************************************************************************
*
* Function:		OOSQL_TCGenHashTable::empty
*
* Description:	Empties the hash table of all symbols by deleting all the
*				symbols stored in the table.
*
****************************************************************************/
{
	OOSQL_TCHashTableNode	*node,*p;

	for (UFour i = 0; i < size; i++) {
		node = table[i];
		while (node) {
			p = node;
			node = node->next;
			delete p;
			}
		}
}

void OOSQL_TCGenHashTable::add(OOSQL_TCHashTableNode* node)
/****************************************************************************
*
* Function:		OOSQL_TCGenHashTable::add
* Parameters:	node	- Node to add to the table
*
* Description:	Adds a symbol to the hash table. The new symbol is linked
*				onto the head of the chain at it's particular hash location.
*
****************************************************************************/
{
	OOSQL_TCHashTableNode	**p,*temp;

	p = &(table[node->hash() % size]);
	temp = *p;
	*p = node;
	node->prev = p;
	node->next = temp;
	if (temp)
		temp->prev = &node->next;
	count++;
}

OOSQL_TCHashTableNode* OOSQL_TCGenHashTable::remove(OOSQL_TCHashTableNode* node)
/****************************************************************************
*
* Function:		OOSQL_TCGenHashTable::remove
* Parameters:	node	- Node to remove from the table
*
* Description:	Removes a symbol from the hash table. "node" is a pointer
*				from a previous find() call.
*
****************************************************************************/
{
	if ((*(node->prev) = node->next) != NULL)
		node->next->prev = node->prev;
	count--;
	return node;
}

OOSQL_TCHashTableNode* OOSQL_TCGenHashTable::find(OOSQL_TCHashTableNode* key) const
/****************************************************************************
*
* Function:		OOSQL_TCGenHashTable::find
* Parameters:	key		- Key of node to find
* Returns:		Pointer to the node if found, NULL if none exist.
*
* Description:	Returns a pointer to the hash table node having a
*				particular key, or NULL if the node isn't in the table.
*
****************************************************************************/
{
	OOSQL_TCHashTableNode* p = table[key->hash() % size];

	while (p) {
		if (*p == *key)
			break;
		p = p->next;
		}

	return p;
}

OOSQL_TCHashTableNode* OOSQL_TCGenHashTable::findCached(OOSQL_TCHashTableNode* key)
/****************************************************************************
*
* Function:		OOSQL_TCGenHashTable::findCached
* Parameters:	key		- Key of node to find
* Returns:		Pointer to the node if found, NULL if none exist.
*
* Description:	Returns a pointer to the hash table node having a
*				particular key, or NULL if the node isn't in the table.
*
*				This routine caches the value that was found, by moving it
*				to the start of the chain that the item was found on.
*
****************************************************************************/
{
	OOSQL_TCHashTableNode	*q,*temp,**p = &(table[key->hash() % size]);

	q = *p;

	while (q) {
		if (*q == *key)
			break;
		q = q->next;
		}

	// If we have found the key, and it is not at the start of the chain,
	// then cache the value by moving to the start of the chain.

	if (q != NULL && q != *p) {

		// Remove it from the chain

		if ((*(q->prev) = q->next) != NULL)
			q->next->prev = q->prev;

		// Add it to the front of the chain (at least one left in chain)

		temp = *p;
		*p = q;
		q->prev = p;
		q->next = temp;
		temp->prev = &q->next;
		}

	return q;
}

OOSQL_TCHashTableNode* OOSQL_TCGenHashTable::next(OOSQL_TCHashTableNode* last) const
/****************************************************************************
*
* Function:		OOSQL_TCGenHashTable::next
* Parameters:	last	- Last node found with find()
* Returns:		Pointer to the next node with the same key as last, or
*				NULL is none exist.
*
* Description:	Returns a pointer to the next node in the current chain that
*				has the same key as the last node found (or NULL if there
*				is no such node). "last" is a pointer returned from a
*				previous find() or next() call.
*
****************************************************************************/
{
	OOSQL_TCHashTableNode* p = last;

	while (last) {
		last = last->next;
		if (*p == *last)
			break;
		}
	return last;
}

std::ostream& operator << (std::ostream& o,OOSQL_TCGenHashTable& h)
/****************************************************************************
*
* Function:		operator <<
* Parameters:	o	- Stream to send output to
*				h	- Hash Table to display
* Returns:		Stream to send output to
*
* Description:	Dumps the contents of the hash table to the stream 'o' by
*				dumping each individual node of the table one by one.
*
****************************************************************************/
{
	OOSQL_TCHashTableNode	*p;

    std::cout << "Hash Table (" << h.count << " items):\n";
	for (UFour i = 0; i < h.size; i++) {
		p = h.table[i];
		while (p) {
			o << *p;
			p = p->next;
			}
		}
	return o;
}

// Virtual destructor for OOSQL_TCHashTableNode's - does nothing.

OOSQL_TCHashTableNode::~OOSQL_TCHashTableNode()
{
}
