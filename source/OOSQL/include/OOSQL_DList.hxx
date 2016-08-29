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
* Description:  Header file for a class to link objects together into a
*               doubly linked list.
*
*
****************************************************************************/

#ifndef _OOSQL_TCL_DLIST_H_
#define _OOSQL_TCL_DLIST_H_

#include "OOSQL_Common.h"
#include <stdio.h>

/*--------------------------- Class Definition ----------------------------*/

//---------------------------------------------------------------------------
// The OOSQL_TCDListNode class is a simple class used to link the objects in the
// list together. To put anything useful into the list, you must derive the
// object placed into the list from OOSQL_TCDListNode.
//---------------------------------------------------------------------------

class OOSQL_TCDListNode {
protected:
    OOSQL_TCDListNode   *next;
    OOSQL_TCDListNode   *prev;

    friend class OOSQL_TCGenDList;
    friend class OOSQL_TCGenDListIterator;
public:
            // Constructor to satisfy some compilers :-(
            OOSQL_TCDListNode() {};

            // Virtual destructor to delete a list node
    virtual ~OOSQL_TCDListNode();
    };

//---------------------------------------------------------------------------
// The list class is designed to manipulate a list of OOSQL_TCDListNode objects.
// In the simple form, OOSQL_TCDListNode objects contain nothing special. To add
// an arbitrary class to the list, you must derive the class from OOSQL_TCDListNode
// (either through single or multiple inheritance).
//---------------------------------------------------------------------------

typedef int (*_OOSQL_TCGenDListCmp)(OOSQL_TCDListNode*,OOSQL_TCDListNode*);

class OOSQL_TCGenDList {
protected:
    UFour       count;      // Number of objects in list
    OOSQL_TCDListNode   *head;      // Pointer to first node in list
    OOSQL_TCDListNode   *z;         // Pointer to last node in list
    OOSQL_TCDListNode   hz[2];      // Space for head and z nodes

    static  _OOSQL_TCGenDListCmp cmp;
    static  OOSQL_TCDListNode   *_z;

            // Protected member to merge two lists together
    static  OOSQL_TCDListNode* merge(OOSQL_TCDListNode* a,OOSQL_TCDListNode* b, OOSQL_TCDListNode*& end);

public:
            // Constructor
            OOSQL_TCGenDList();

            // Destructor
            ~OOSQL_TCGenDList();

            // Method to examine the first node in the List
            OOSQL_TCDListNode* peekHead() const;

            // Method to examine the last node in the List
            OOSQL_TCDListNode* peekTail() const;

            // Method to return the next node in the list
            OOSQL_TCDListNode* next(OOSQL_TCDListNode* node) const;

            // Method to return the prev node in the list
            OOSQL_TCDListNode* prev(OOSQL_TCDListNode* node) const;

            // Method to add a node to the head of the list
            void addToHead(OOSQL_TCDListNode* node);

            // Method to add a node to the tail of the list
            void addToTail(OOSQL_TCDListNode* node);

            // Method to add a node after another node in the list
            void addAfter(OOSQL_TCDListNode* node,OOSQL_TCDListNode* after);

            // Method to add a node before another node in the list
            void addBefore(OOSQL_TCDListNode* node,OOSQL_TCDListNode* before);

            // Method to detach a specified OOSQL_TCDListNode from the list.
            OOSQL_TCDListNode* remove(OOSQL_TCDListNode* node);

            // Method to detach a specified OOSQL_TCDListNode from the list.
            OOSQL_TCDListNode* removeNext(OOSQL_TCDListNode *prev);

            // Method to detach the first node from the list.
            OOSQL_TCDListNode* removeFromHead();

            // Method to detach the last node from the list.
            OOSQL_TCDListNode* removeFromTail();

            // Sort the linked list of objects
            void sort(_OOSQL_TCGenDListCmp cmp);

            // Empties the entire list by destroying all nodes
            void empty();

            // Returns the number of items in the list
            UFour numberOfItems() const { return count; };

            // Returns true if the list is empty
            bool isEmpty() const    { return (count == 0)?true:false; };

private:
    friend class OOSQL_TCGenDListIterator;
    };

//---------------------------------------------------------------------------
// The list iterator is the class of iterator that is used to step through
// the elements in the list.
//---------------------------------------------------------------------------

class OOSQL_TCGenDListIterator {
protected:
    OOSQL_TCDListNode   *cursor;
    OOSQL_TCGenDList    *beingIterated;
public:
            // Constructor
            OOSQL_TCGenDListIterator();

            // Constructor given a list reference
            OOSQL_TCGenDListIterator(const OOSQL_TCGenDList& l);

            // Intialise a list iterator from a list
            void operator = (const OOSQL_TCGenDList& l);

            // assignment operator between two listIterators
            void operator = (const OOSQL_TCGenDListIterator& i);

            // Overloaded cast to an integer
            operator int ();

            // Convert the iterator to the corresponding node
            OOSQL_TCDListNode* node();

            // Pre-increment operator for the iterator
            OOSQL_TCDListNode* operator ++ ();

            // Post-increment operator for the iterator
            OOSQL_TCDListNode* operator ++ (int);

            // Pre-decrement operator for the iterator
            OOSQL_TCDListNode* operator -- ();

            // Post-decrement operator for the iterator
            OOSQL_TCDListNode* operator -- (int);

            // Method to restart the iterator at head of list
            void restart();

            // Method to restart the iterator at tail of list
            void restartTail();
    };

//---------------------------------------------------------------------------
// Template wrapper class for declaring Type Safe doubly linked lists.
//---------------------------------------------------------------------------

template <class T> class OOSQL_TCDList : public OOSQL_TCGenDList {
public:
            T* peekHead() const
                { return (T*)OOSQL_TCGenDList::peekHead(); };
            T* peekTail() const
                { return (T*)OOSQL_TCGenDList::peekTail(); };
            T* next(T* node) const
                { return (T*)OOSQL_TCGenDList::next(node); };
            T* prev(T* node) const
                { return (T*)OOSQL_TCGenDList::prev(node); };
            T* remove(T* node)
                { return (T*)OOSQL_TCGenDList::remove(node); };
            T* removeNext(T* node)
                { return (T*)OOSQL_TCGenDList::removeNext(node); };
            T* removeFromHead()
                { return (T*)OOSQL_TCGenDList::removeFromHead(); };
            T* removeFromTail()
                { return (T*)OOSQL_TCGenDList::removeFromTail(); };
            void sort(int (*cmp)(T*,T*))
                { OOSQL_TCGenDList::sort((_OOSQL_TCGenDListCmp)cmp); };
    };

template <class T> class OOSQL_TCDListIterator : public OOSQL_TCGenDListIterator {
public:
            OOSQL_TCDListIterator()
                : OOSQL_TCGenDListIterator() {};
            OOSQL_TCDListIterator(const OOSQL_TCDList<T>& l)
                : OOSQL_TCGenDListIterator(l) {};
            void operator = (const OOSQL_TCDList<T>& l)
                { OOSQL_TCGenDListIterator::operator=(l); };
            void operator = (const OOSQL_TCDListIterator<T>& i)
                { OOSQL_TCGenDListIterator::operator=(i); };
            T* node()
                { return (T*)OOSQL_TCGenDListIterator::node(); };
            T* operator ++ ()
                { return (T*)OOSQL_TCGenDListIterator::operator++(); };
            T* operator ++ (int)
                { return (T*)OOSQL_TCGenDListIterator::operator++(1); };
            T* operator -- ()
                { return (T*)OOSQL_TCGenDListIterator::operator--(); };
            T* operator -- (int i)
                { return (T*)OOSQL_TCGenDListIterator::operator--(i); };
    };

/*------------------------ Inline member functions ------------------------*/

inline OOSQL_TCDListNode* OOSQL_TCGenDList::peekHead() const
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::peekHead
* Returns:      Returns a pointer to the head node on the list, or NULL if
*               the list is empty.
*
****************************************************************************/
{
    return (head->next == z ? NULL : head->next);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDList::peekTail() const
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::peekTail
* Returns:      Returns a pointer to the tail node on the list, or NULL if
*               the list is empty.
*
****************************************************************************/
{
    return (z->prev == head ? NULL : z->prev);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDList::next(OOSQL_TCDListNode *node) const
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::next
* Parameters:   node    - Node to obtain next from
* Returns:      Pointer to the next node in the list, NULL if none.
*
****************************************************************************/
{
    return (node->next == z ? NULL : node->next);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDList::prev(OOSQL_TCDListNode *node) const
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::prev
* Parameters:   node    - Node to obtain prev from
* Returns:      Pointer to the previous node in the list, NULL if none.
*
****************************************************************************/
{
    return (node->prev == head ? NULL : node->prev);
}

inline void OOSQL_TCGenDList::addAfter(OOSQL_TCDListNode* node,OOSQL_TCDListNode* after)
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::addAfter
* Parameters:   node    - New node to attach to list
*               after   - Node to attach new node after in list
*
* Description:  Attaches a new node after a specified node in the list.
*               The list must contain at least one node, and after may
*               be the tail node of the list.
*
****************************************************************************/
{
    node->next = after->next;
    after->next = node;
    node->prev = after;
    node->next->prev = node;
    count++;
}

inline void OOSQL_TCGenDList::addBefore(OOSQL_TCDListNode* node,OOSQL_TCDListNode* before)
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::addBefore
* Parameters:   node    - New node to attach to list
*               before  - Node to attach new node before in list
*
* Description:  Attaches a new node before a specified node in the list.
*               The list must contain at least one node, and before may
*               be the tail node of the list.
*
****************************************************************************/
{
    node->next = before;
    before->prev->next = node;
    node->prev = before->prev;
    before->prev = node;
    count++;
}

inline void OOSQL_TCGenDList::addToHead(OOSQL_TCDListNode* node)
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::addToHead
* Parameters:   node    - Node to add to list
*
* Description:  Attaches the node to the head of the list.
*
****************************************************************************/
{
    addAfter(node,head);
}

inline void OOSQL_TCGenDList::addToTail(OOSQL_TCDListNode* node)
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::addToTail
* Parameters:   node    - Node to add to list
*
* Description:  Attaches the node to the tail of the list.
*
****************************************************************************/
{
    addAfter(node,z->prev);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDList::remove(OOSQL_TCDListNode* node)
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::remove
* Parameters:   node    - Pointer to node remove from the list
* Returns:      Node removed from list.
*
* Description:  Removes the specified node from the list.
*
****************************************************************************/
{
    node->next->prev = node->prev;
    node->prev->next = node->next;
    count--;
    return node;
}

inline OOSQL_TCDListNode* OOSQL_TCGenDList::removeNext(OOSQL_TCDListNode* prev)
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::removeNext
* Parameters:   prev    - Pointer to the previous node in the list
* Returns:      Node removed from list, or NULL if prev is the last node.
*
* Description:  Removes the specified node from the list.
*
****************************************************************************/
{
    OOSQL_TCDListNode*  node;

    if ((node = prev->next) != z)
        return remove(node);
    else
        return NULL;
}

inline OOSQL_TCDListNode* OOSQL_TCGenDList::removeFromHead()
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::removeFromHead
* Returns:      Pointer to the node removed from the head of the list,
*               or NULL if the list is empty.
*
****************************************************************************/
{
    return removeNext(head);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDList::removeFromTail()
/****************************************************************************
*
* Function:     OOSQL_TCGenDList::removeFromTail
* Returns:      Pointer to the node removed from the tail of the list,
*               or NULL if the list is empty.
*
****************************************************************************/
{
    return removeNext(z->prev->prev);
}

inline OOSQL_TCGenDListIterator::OOSQL_TCGenDListIterator()
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::OOSQL_TCGenDListIterator
*
* Description:  Default constructor for a dlist iterator.
*
****************************************************************************/
{
    cursor = NULL;
    beingIterated = NULL;
}

inline OOSQL_TCGenDListIterator::OOSQL_TCGenDListIterator(const OOSQL_TCGenDList& l)
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::OOSQL_TCGenDListIterator
* Parameters:   l   - DList to construct iterator from
*
* Description:  Constructor for a OOSQL_TCGenDListIterator given a reference to a list
*               to iterate.
*
****************************************************************************/
{
    beingIterated = (OOSQL_TCGenDList*)&l;
    cursor = l.head->next;
}

inline void OOSQL_TCGenDListIterator::operator = (const OOSQL_TCGenDList& l)
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::operator =
* Parameters:   l   - OOSQL_TCGenDList to assign to iterator
*
* Description:  Assignment operator for a DListIterator given a reference to
*               a list to iterate.
*
****************************************************************************/
{
    beingIterated = (OOSQL_TCGenDList*)&l;
    cursor = l.head->next;
}

inline void OOSQL_TCGenDListIterator::operator = (const OOSQL_TCGenDListIterator& i)
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::operator =
* Parameters:   i   - Iterator to assign from
*
* Description:  Assignment operator for a DListIterator given a reference to
*               another DListIterator.
*
****************************************************************************/
{
    beingIterated = i.beingIterated;
    cursor = i.cursor;
}

inline OOSQL_TCGenDListIterator::operator int()
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::operator int
*
* Description:  Overloaded cast to integer for the list iterator. Evaluates
*               to 0 when the end of the list is reached.
*
****************************************************************************/
{
    return (cursor != beingIterated->z && cursor != beingIterated->head);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDListIterator::node()
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::node
* Returns:      Returns a reference to the node in the list.
*
****************************************************************************/
{
    return ((int)*this ? cursor : NULL);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDListIterator::operator ++ ()
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::operator ++
* Returns:      Pointer to node after incrementing
*
* Description:  Increments the iterator by moving it to the next object
*               in the list. We return a pointer to the node pointed to
*               after the increment takes place.
*
****************************************************************************/
{
    cursor = cursor->next;
    return (cursor == beingIterated->z ? NULL : cursor);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDListIterator::operator ++ (int)
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::operator ++ (int)
* Returns:      Pointer to node before incrementing
*
* Description:  Increments the iterator by moving it to the next object
*               in the list. We return a pointer to the node pointed to
*               before the increment takes place.
*
****************************************************************************/
{
    OOSQL_TCDListNode   *prev = cursor;

    cursor = cursor->next;
    return (prev == beingIterated->z ? NULL : prev);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDListIterator::operator -- ()
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::operator --
* Returns:      Pointer to node after decrementing
*
* Description:  Decrements the iterator by moving it to the next object
*               in the list. We return a pointer to the node pointed to
*               after the decrement takes place.
*
****************************************************************************/
{
    cursor = cursor->prev;
    return (cursor == beingIterated->head ? NULL : cursor);
}

inline OOSQL_TCDListNode* OOSQL_TCGenDListIterator::operator -- (int)
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::operator -- (int)
* Returns:      Pointer to node before decrementing
*
* Description:  Decrements the iterator by moving it to the next object
*               in the list. We return a pointer to the node pointed to
*               before the decrement takes place.
*
****************************************************************************/
{
    OOSQL_TCDListNode   *prev = cursor;

    cursor = cursor->prev;
    return (prev == beingIterated->head ? NULL : prev);
}

inline void OOSQL_TCGenDListIterator::restart()
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::restart
*
* Description:  Restart the iterator at the beginning of the list.
*
****************************************************************************/
{
    cursor = beingIterated->head->next;
}

inline void OOSQL_TCGenDListIterator::restartTail()
/****************************************************************************
*
* Function:     OOSQL_TCGenDListIterator::restartTail
*
* Description:  Restart the iterator at the end of the list.
*
****************************************************************************/
{
    cursor = beingIterated->z->prev;
}

#endif  // _OOSQL_TCL_DLIST_H_
