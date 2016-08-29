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
* Description:	Member functions for the dlist class, a class designed to
*				link a series of objects together into a doubly linked
*				list. All items placed in the list MUST be derived from
*				the class OOSQL_TCDListNode.
*
****************************************************************************/

#include "OOSQL_DList.hxx"

/*--------------------------- Member functions ----------------------------*/

_OOSQL_TCGenDListCmp	OOSQL_TCGenDList::cmp;
OOSQL_TCDListNode		*OOSQL_TCGenDList::_z;

OOSQL_TCGenDList::OOSQL_TCGenDList()
/****************************************************************************
*
* Function:		OOSQL_TCGenDList::OOSQL_TCGenDList
*
* Description:	Constructor for the list class. We set the count of items
*				in the list to 0, and initialise the head and tail pointers
*				for the list. These both point to dummy nodes within the
*				list class, so that the dummy tail node always points to
*				itself ensuring we cannot iterate of the end of the list.
*				This also simplifies maintaining the head and tail pointers.
*
****************************************************************************/
{
	count = 0;
	head = &hz[0];				// Setup head and tail pointers
	z = &hz[1];
	head->next = z->next = z;
	z->prev = head->prev = head;
}

OOSQL_TCGenDList::~OOSQL_TCGenDList()
/****************************************************************************
*
* Function:		OOSQL_TCGenDList::~OOSQL_TCGenDList
*
* Description:	Destructor for the list class. All we do here is ask the
*				list to empty itself.
*
****************************************************************************/
{
	empty();
}

void OOSQL_TCGenDList::empty(void)
/****************************************************************************
*
* Function:		OOSQL_TCGenDList::empty
*
* Description:	Empties the list of all elements. We do this by stepping
*				through the list deleting all the elements as we go.
*
****************************************************************************/
{
	OOSQL_TCDListNode *temp;

	while (head->next != z) {
		temp = head->next;
		head->next = head->next->next;
		delete temp;
		}
	z->prev = head;
	count = 0;
}

OOSQL_TCDListNode* OOSQL_TCGenDList::merge(OOSQL_TCDListNode *a,OOSQL_TCDListNode *b,OOSQL_TCDListNode*& end)
/****************************************************************************
*
* Function:		OOSQL_TCGenDList::merge
* Parameters:	a,b		- Sublist's to merge
*				end		- Pointer to end of merged list
* Returns:		Pointer to the merged sublists.
*
* Description:	Merges two sorted lists of nodes together into a single
*				sorted list, and sets a pointer to the end of the newly
*				merged lists.
*
****************************************************************************/
{
	OOSQL_TCDListNode	*c;

	// Go through the lists, merging them together in sorted order

	c = _z;
	while (a != _z && b != _z) {
		if (cmp(a,b) <= 0) {
			c->next = a; c = a; a = a->next;
			}
		else {
			c->next = b; c = b; b = b->next;
			}
		};

	// If one of the lists is not exhausted, then re-attach it to the end
	// of the newly merged list

	if (a != _z) c->next = a;
	if (b != _z) c->next = b;

	// Set end to point to the end of the newly merged list

	while (c->next != _z) c = c->next;
	end = c;

	// Determine the start of the merged lists, and reset _z to point to
	// itself

	c = _z->next; _z->next = _z;
	return c;
}

void OOSQL_TCGenDList::sort(_OOSQL_TCGenDListCmp cmp_func)
/****************************************************************************
*
* Function:		OOSQL_TCGenDList::sort
* Parameters:	cmp	- Function to compare the contents of two ListNode's.
*
* Description:	Mergesort's all the nodes in the list. 'cmp' must point to
*				a comparison function that can compare the contents of
*				two ListNode's. 'cmp' should work the same as strcmp(), in
*				terms of the values it returns.
*
****************************************************************************/
{
	int			i,N;
	OOSQL_TCDListNode	*a,*b;		// Pointers to sublists to merge
	OOSQL_TCDListNode	*c;			// Pointer to end of sorted sublists
	OOSQL_TCDListNode	*todo;		// Pointer to sublists yet to be sorted
	OOSQL_TCDListNode	*t;			// Temporary

	// Set up globals required by DList::merge().
	OOSQL_TCDListNode *head = this->head;
	_z = z;
	cmp = cmp_func;

	for (N = 1,a = _z; a != head->next; N = N + N) {
		todo = head->next; c = head;
		while (todo != _z) {

			// Build first sublist to be merged, and splice from main list

			a = t = todo;
			for (i = 1; i < N; i++) t = t->next;
			b = t->next; t->next = _z; t = b;

			// Build second sublist to be merged and splice from main list

			for (i = 1; i < N; i++) t = t->next;
			todo = t->next; t->next = _z;

			// Merge the two sublists created, and set 'c' to point to the
			// end of the newly merged sublists.

			c->next = merge(a,b,t); c = t;
			}
		}

	// After sorting the list, traverse the list from head to tail fixing
	// all the previous pointers as we go.

	a = b = head;
	b = b->next;
	while (1) {
		b->prev = a;
		if (b == _z)
			break;
		a = a->next;
		b = b->next;
		}
}

// Virtual destructor for OOSQL_TCDListNode's. Does nothing.

OOSQL_TCDListNode::~OOSQL_TCDListNode()
{
}
