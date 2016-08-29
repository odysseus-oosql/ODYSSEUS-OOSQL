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

/*
* Description:  Header file for a class to link objects together into a
*               singly linked list.
*
*
****************************************************************************/

#ifndef _OSQL_TCL_LIST_H_
#define _OSQL_TCL_LIST_H_

#include "OOSQL_Common.h"
#include <stdio.h>

/*--------------------------- Class Definition ----------------------------*/

//---------------------------------------------------------------------------
// The OOSQL_TCListNode class is a simple class used to link the objects in the list
// together. To put anything useful into the list, you must derive the
// object placed into the list from OOSQL_TCListNode.
//---------------------------------------------------------------------------

class OOSQL_TCListNode {
protected:
    OOSQL_TCListNode    *next;

    friend class OOSQL_TCGenList;
    friend class OOSQL_TCGenListIterator;
    friend class OOSQL_TCSimpleGenList;
    friend class OOSQL_TCSimpleGenListIterator;
public:
            // Constructor to statisfy some compilers :-)
            OOSQL_TCListNode()  {};

            // Virtual destructor to delete a list node
    virtual ~OOSQL_TCListNode();
    };

//---------------------------------------------------------------------------
// The OOSQL_TCGenList class is designed to manipulate a list of OOSQL_TCListNode objects.
// In the simple form, OOSQL_TCListNode objects contain nothing special. To add
// an arbitrary class to the list, you must derive the class from OOSQL_TCListNode
// (either through single or multiple inheritance).
//---------------------------------------------------------------------------

typedef int (*_OOSQL_TCGenListCmp)(OOSQL_TCListNode*,OOSQL_TCListNode*);

class OOSQL_TCGenList {
protected:
    UFour       count;      // Number of objects in list
    OOSQL_TCListNode    *head;      // Pointer to first node in list
    OOSQL_TCListNode    *z;         // Pointer to last node in list
    OOSQL_TCListNode    hz[2];      // Space for head and z nodes

    static  _OOSQL_TCGenListCmp cmp;
    static  OOSQL_TCListNode    *_z;

            // Protected member to merge two lists together
    static  OOSQL_TCListNode* merge(OOSQL_TCListNode* a,OOSQL_TCListNode* b, OOSQL_TCListNode*& end);

public:
            // Constructor
            OOSQL_TCGenList();

            // Destructor
            ~OOSQL_TCGenList();

            // Method to examine the first node in the TCList
            OOSQL_TCListNode* peekHead() const;

            // Method to return the next node in the list
            OOSQL_TCListNode* next(OOSQL_TCListNode* node) const;

            // Method to add a node to the head of the list
            void addToHead(OOSQL_TCListNode* node);

            // Method to add a node after another node in the list
            void addAfter(OOSQL_TCListNode* node,OOSQL_TCListNode* after);

            // Method to detach a specified OOSQL_TCListNode from the list.
            OOSQL_TCListNode* removeNext(OOSQL_TCListNode* prev);

            // Method to detach the first node from the list.
            OOSQL_TCListNode* removeFromHead();

            // Sort the linked list of objects
            void sort(_OOSQL_TCGenListCmp cmp);

            // Empties the entire list by destroying all nodes
            void empty();

            // Returns the number of items in the list
            UFour numberOfItems() const { return count; };

            // Returns true if the list is empty
            bool isEmpty() const    { return (count == 0)?true:false; };

private:
    friend class OOSQL_TCGenListIterator;
    };

//---------------------------------------------------------------------------
// The OOSQL_TCSimpleGenList class is designed to manipulate a list of OOSQL_TCListNode
// objects. The OOSQL_TCSimpleGenList class only maintains a single pointer to the
// head of the list rather than using the dummy node system, so is useful
// when you need to maintain an array of linked lists, and the list type
// itself needs to be as small as possible for memory efficiency.
//---------------------------------------------------------------------------

class OOSQL_TCSimpleGenList {
protected:
    OOSQL_TCListNode    *head;      // Pointer to first node in list

public:
            // Constructor
            OOSQL_TCSimpleGenList() { head = NULL; };

            // Destructor
            ~OOSQL_TCSimpleGenList();

            // Method to examine the first node in the TCList
            OOSQL_TCListNode* peekHead() const
                { return head; };

            // Method to return the next node in the list
            OOSQL_TCListNode* next(OOSQL_TCListNode* node) const
                { return node->next; };

            // Method to add a node to the head of the list
            void addToHead(OOSQL_TCListNode* node);

            // Method to add a node after another node in the list
            void addAfter(OOSQL_TCListNode* node,OOSQL_TCListNode* after);

            // Method to detach a specified OOSQL_TCListNode from the list.
            OOSQL_TCListNode* removeNext(OOSQL_TCListNode* prev);

            // Method to detach the first node from the list.
            OOSQL_TCListNode* removeFromHead();

            // Empties the entire list by destroying all nodes
            void empty();

            // Returns the number of items in the list
            UFour numberOfItems() const;

            // Returns true if the list is empty
            bool isEmpty() const
                { return (head == NULL)?true:false; };

private:
    friend class OOSQL_TCSimpleGenListIterator;
    };

//---------------------------------------------------------------------------
// The OOSQL_TCGenListIterator is the class of iterator that is used to step through
// the elements in the list.
//---------------------------------------------------------------------------

class OOSQL_TCGenListIterator {
protected:
    OOSQL_TCListNode        *cursor;
    const OOSQL_TCGenList   *beingIterated;

public:
            // Constructor
            OOSQL_TCGenListIterator();

            // Constructor given a list reference
            OOSQL_TCGenListIterator(const OOSQL_TCGenList& l);

            // Intialise a list iterator from a list
            void operator = (const OOSQL_TCGenList& l);

            // assignment operator between two listIterators
            void operator = (const OOSQL_TCGenListIterator& i);

            // Overloaded cast to an integer
            operator int ();

            // Convert the iterator to the corresponding node
            OOSQL_TCListNode* node();

            // Pre-increment operator for the iterator
            OOSQL_TCListNode* operator ++ ();

            // Post-increment operator for the iterator
            OOSQL_TCListNode* operator ++ (int);

            // Method to restart the iterator
            void restart();
    };

//---------------------------------------------------------------------------
// The OOSQL_TCSimpleGenListIterator is the class of iterator that is used to step
// through the elements in the list.
//---------------------------------------------------------------------------

class OOSQL_TCSimpleGenListIterator {
protected:
    OOSQL_TCListNode                *cursor;
    const OOSQL_TCSimpleGenList *beingIterated;

public:
            // Constructor
            OOSQL_TCSimpleGenListIterator()
                { cursor = NULL; beingIterated = NULL; };

            // Constructor given a list reference
            OOSQL_TCSimpleGenListIterator(const OOSQL_TCSimpleGenList& l)
                { beingIterated = &l; cursor = l.head; };

            // Intialise a list iterator from a list
            void operator = (const OOSQL_TCSimpleGenList& l)
                { beingIterated = &l; cursor = l.head; };

            // assignment operator between two listIterators
            void operator = (const OOSQL_TCSimpleGenListIterator& i)
                { beingIterated = i.beingIterated; cursor = i.cursor; };

            // Overloaded cast to an integer
            operator int ()
                { return cursor != NULL; };

            // Convert the iterator to the corresponding node
            OOSQL_TCListNode* node()
                { return cursor; };

            // Pre-increment operator for the iterator
            OOSQL_TCListNode* operator ++ ();

            // Post-increment operator for the iterator
            OOSQL_TCListNode* operator ++ (int);

            // Method to restart the iterator
            void restart()
                { cursor = beingIterated->head; };
    };

//---------------------------------------------------------------------------
// Set of template wrapper classes for declaring Type Safe linked lists.
// Note that the elements of the linked list must still be derived from
// OOSQL_TCListNode.
//---------------------------------------------------------------------------

template <class T> class TCList : public OOSQL_TCGenList {
public:
            T* peekHead() const
                { return (T*)OOSQL_TCGenList::peekHead(); };
            T* next(T* node) const
                { return (T*)OOSQL_TCGenList::next(node); };
            T* removeNext(T* prev)
                { return (T*)OOSQL_TCGenList::removeNext(prev); };
            T* removeFromHead()
                { return (T*)OOSQL_TCGenList::removeFromHead(); };
            void sort(int (*cmp)(T*,T*))
                { OOSQL_TCGenList::sort((_OOSQL_TCGenListCmp)cmp); };
    };

template <class T> class TCListIterator : public OOSQL_TCGenListIterator {
public:
            TCListIterator()
                : OOSQL_TCGenListIterator() {};
            TCListIterator(const TCList<T>& l)
                : OOSQL_TCGenListIterator(l) {};
            void operator = (const TCList<T>& l)
                { OOSQL_TCGenListIterator::operator=(l); };
            void operator = (const TCListIterator<T>& i)
                { OOSQL_TCGenListIterator::operator=(i); };
            T* node()
                { return (T*)OOSQL_TCGenListIterator::node(); };
            T* operator ++ ()
                { return (T*)OOSQL_TCGenListIterator::operator++(); };
            T* operator ++ (int)
                { return (T*)OOSQL_TCGenListIterator::operator++(1); };
    };

template <class T> class TCSimpleList : public OOSQL_TCSimpleGenList {
public:
            T* peekHead() const
                { return (T*)OOSQL_TCSimpleGenList::peekHead(); };
            T* next(T* node) const
                { return (T*)OOSQL_TCSimpleGenList::next(node); };
            T* removeNext(T* prev)
                { return (T*)OOSQL_TCSimpleGenList::removeNext(prev); };
            T* removeFromHead()
                { return (T*)OOSQL_TCSimpleGenList::removeFromHead(); };
    };

template <class T> class TCSimpleListIterator : public OOSQL_TCSimpleGenListIterator {
public:
            TCSimpleListIterator()
                : OOSQL_TCSimpleGenListIterator() {};
            TCSimpleListIterator(const TCSimpleList<T>& l)
                : OOSQL_TCSimpleGenListIterator(l) {};
            void operator = (const TCSimpleList<T>& l)
                { OOSQL_TCSimpleGenListIterator::operator=(l); };
            void operator = (const TCSimpleListIterator<T>& i)
                { OOSQL_TCSimpleGenListIterator::operator=(i); };
            T* node()
                { return (T*)OOSQL_TCSimpleGenListIterator::node(); };
            T* operator ++ ()
                { return (T*)OOSQL_TCSimpleGenListIterator::operator++(); };
            T* operator ++ (int)
                { return (T*)OOSQL_TCSimpleGenListIterator::operator++(1); };
    };

/*------------------------ Inline member functions ------------------------*/

inline OOSQL_TCListNode* OOSQL_TCGenList::peekHead() const
/****************************************************************************
*
* Function:     OOSQL_TCGenList::peekHead
* Parameters:
* Returns:      Returns a pointer to the head node on the list, or NULL if
*               the list is empty.
*
****************************************************************************/
{
    return (head->next == z ? NULL : head->next);
}

inline OOSQL_TCListNode* OOSQL_TCGenList::next(OOSQL_TCListNode *node) const
/****************************************************************************
*
* Function:     OOSQL_TCGenList::next
* Parameters:   node    - Node to obtain next from
* Returns:      Pointer to the next node in the list, NULL if none.
*
****************************************************************************/
{
    return (node->next == z ? NULL : node->next);
}

inline void OOSQL_TCGenList::addAfter(OOSQL_TCListNode* node,OOSQL_TCListNode* after)
/****************************************************************************
*
* Function:     OOSQL_TCGenList::addAfter
* Parameters:   node    - Node to attach new node after in list
*               after   - New node to attach to list
*
* Description:  Attaches a new node after a specified node in the list.
*               The list must contain at least one node, and after may
*               be the tail node of the list.
*
****************************************************************************/
{
    node->next = after->next;
    after->next = node;
    count++;
}

inline void OOSQL_TCGenList::addToHead(OOSQL_TCListNode* node)
/****************************************************************************
*
* Function:     OOSQL_TCGenList::addToHead
* Parameters:   node    - Node to add to list
*
* Description:  Attaches the node to the head of the list, maintaining the
*               head and tail pointers.
*
****************************************************************************/
{
    addAfter(node,head);
}

inline OOSQL_TCListNode* OOSQL_TCGenList::removeNext(OOSQL_TCListNode* prev)
/****************************************************************************
*
* Function:     OOSQL_TCGenList::removeNext
* Parameters:   node    - Pointer to node remove from the list
*               prev    - Pointer to the previous node in the list
* Returns:      Node removed from list, or NULL if prev is the last node in
*               the list.
*
* Description:  Attempts to remove the specified node from the list. 'prev'
*               should point to the previous node in the list.
*
****************************************************************************/
{
    OOSQL_TCListNode    *node;

    if ((node = prev->next) != z) {
        prev->next = prev->next->next;
        count--;
        return node;
        }
    else
        return NULL;
}

inline OOSQL_TCListNode* OOSQL_TCGenList::removeFromHead()
/****************************************************************************
*
* Function:     OOSQL_TCGenList::removeFromHead
* Returns:      Pointer to the node removed from the head of the list,
*               or NULL if the list is empty.
*
****************************************************************************/
{
    return removeNext(head);
}

inline OOSQL_TCGenListIterator::OOSQL_TCGenListIterator()
/****************************************************************************
*
* Function:     OOSQL_TCGenListIterator::OOSQL_TCGenListIterator
*
* Description:  Default constructor for a list iterator.
*
****************************************************************************/
{
    cursor = NULL;
    beingIterated = NULL;
}

inline OOSQL_TCGenListIterator::OOSQL_TCGenListIterator(const OOSQL_TCGenList& l)
/****************************************************************************
*
* Function:     OOSQL_TCGenListIterator::OOSQL_TCGenListIterator
* Parameters:   l   - TCList to construct iterator from
*
* Description:  Constructor for a TCListIterator given a reference to a list
*               to iterate.
*
****************************************************************************/
{
    beingIterated = (OOSQL_TCGenList*)&l;
    cursor = l.head->next;
}

inline void OOSQL_TCGenListIterator::operator = (const OOSQL_TCGenList& l)
/****************************************************************************
*
* Function:     OOSQL_TCGenListIterator::operator =
* Parameters:   l   - TCList to assign to iterator
*
* Description:  Assignment operator for a TCListIterator given a reference to
*               a list to iterate.
*
****************************************************************************/
{
    beingIterated = &l;
    cursor = l.head->next;
}

inline void OOSQL_TCGenListIterator::operator = (const OOSQL_TCGenListIterator& i)
/****************************************************************************
*
* Function:     OOSQL_TCGenListIterator::operator =
* Parameters:   i   - Iterator to assign from
*
* Description:  Assignment operator for a TCListIterator given a reference to
*               another TCListIterator.
*
****************************************************************************/
{
    beingIterated = i.beingIterated;
    cursor = i.cursor;
}

inline OOSQL_TCGenListIterator::operator int()
/****************************************************************************
*
* Function:     OOSQL_TCGenListIterator::operator int
*
* Description:  Overloaded cast to integer for the list iterator. Evaluates
*               to 0 when the end of the list is reached.
*
****************************************************************************/
{
    return (cursor != beingIterated->z);
}

inline OOSQL_TCListNode* OOSQL_TCGenListIterator::node()
/****************************************************************************
*
* Function:     OOSQL_TCGenListIterator::node
* Returns:      Returns a reference to the node in the list.
*
****************************************************************************/
{
    return (cursor == beingIterated->z ? NULL : cursor);
}

inline OOSQL_TCListNode* OOSQL_TCGenListIterator::operator ++ ()
/****************************************************************************
*
* Function:     OOSQL_TCGenListIterator::operator ++
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

inline OOSQL_TCListNode* OOSQL_TCGenListIterator::operator ++ (int)
/****************************************************************************
*
* Function:     OOSQL_TCGenListIterator::operator ++ (int)
* Returns:      Pointer to node before incrementing
*
* Description:  Increments the iterator by moving it to the next object
*               in the list. We return a pointer to the node pointed to
*               before the increment takes place.
*
****************************************************************************/
{
    OOSQL_TCListNode    *prev = cursor;

    cursor = cursor->next;
    return (prev == beingIterated->z ? NULL : prev);
}

inline void OOSQL_TCGenListIterator::restart()
/****************************************************************************
*
* Function:     OOSQL_TCGenListIterator::restart
*
* Description:  Restart the iterator at the beginning of the list.
*
****************************************************************************/
{
    cursor = beingIterated->head->next;
}

inline void OOSQL_TCSimpleGenList::addToHead(OOSQL_TCListNode* node)
/****************************************************************************
*
* Function:     OOSQL_TCSimpleGenList::addToHead
* Parameters:   node    - Node to add to TCSimpleList
*
* Description:  Attaches the node to the head of the TCSimpleList.
*
****************************************************************************/
{
    node->next = head;
    head = node;
}

inline void OOSQL_TCSimpleGenList::addAfter(OOSQL_TCListNode* node,OOSQL_TCListNode* after)
/****************************************************************************
*
* Function:     OOSQL_TCSimpleGenList::addAfter
* Parameters:   node    - Node to attach new node after in TCSimpleList
*               after   - New node to attach to TCSimpleList
*
* Description:  Attaches a new node after a specified node in the TCSimpleList.
*               The TCSimpleList must contain at least one node, and after may
*               be the tail node of the TCSimpleList.
*
****************************************************************************/
{
    node->next = after->next;
    after->next = node;
}

inline OOSQL_TCListNode* OOSQL_TCSimpleGenList::removeNext(OOSQL_TCListNode* prev)
/****************************************************************************
*
* Function:     OOSQL_TCSimpleGenList::removeNext
* Parameters:   node    - Pointer to node remove from the TCSimpleList
*               prev    - Pointer to the previous node in the TCSimpleList
* Returns:      Node removed from TCSimpleList, or NULL if prev is the last
*               node in the TCSimpleList.
*
* Description:  Attempts to remove the specified node from the TCSimpleList.
*               'prev' should point to the previous node in the TCSimpleList.
*
****************************************************************************/
{
    OOSQL_TCListNode    *node;

    if ((node = prev->next) != NULL) {
        prev->next = prev->next->next;
        return node;
        }
    else
        return NULL;
}

inline OOSQL_TCListNode* OOSQL_TCSimpleGenList::removeFromHead()
/****************************************************************************
*
* Function:     OOSQL_TCSimpleGenList::removeFromHead
* Returns:      Pointer to the node removed from the head of the TCSimpleList,
*               or NULL if the TCSimpleList is empty.
*
****************************************************************************/
{
    OOSQL_TCListNode    *node = head;

    if (head)
        head = head->next;
    return node;
}

inline OOSQL_TCListNode* OOSQL_TCSimpleGenListIterator::operator ++ ()
/****************************************************************************
*
* Function:     OOSQL_TCSimpleGenListIterator::operator ++
* Returns:      Pointer to node after incrementing
*
* Description:  Increments the iterator by moving it to the next object
*               in the TCSimpleList. We return a pointer to the node pointed to
*               after the increment takes place.
*
****************************************************************************/
{
    return cursor ? (cursor = cursor->next) : NULL;
}

inline OOSQL_TCListNode* OOSQL_TCSimpleGenListIterator::operator ++ (int)
/****************************************************************************
*
* Function:     OOSQL_TCSimpleGenListIterator::operator ++ (int)
* Returns:      Pointer to node before incrementing
*
* Description:  Increments the iterator by moving it to the next object
*               in the TCSimpleList. We return a pointer to the node pointed to
*               before the increment takes place.
*
****************************************************************************/
{
    OOSQL_TCListNode    *prev = cursor;

    if (cursor)
        cursor = cursor->next;
    return prev;
}

#endif  // _OSQL_TCL_LIST_H_
