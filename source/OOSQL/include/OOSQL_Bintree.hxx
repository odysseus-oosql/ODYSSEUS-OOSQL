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
* Description:  Header file for the BinaryTree class, a class designed
*               for storing ordered information in a simple binary tree.
*               This class does nothing to ensure the binary tree is
*               balanced, so it may well become a degenerate linked list.
*               Use the AVLTree or RedBlackTree class if you require the
*               tree to be balanced.
*
*
****************************************************************************/

#ifndef _OOSQL_TCL_BINTREE_H_
#define _OOSQL_TCL_BINTREE_H_

#include "OOSQL_Common.h"


/*--------------------------- Class Definition ----------------------------*/

//---------------------------------------------------------------------------
// The OOSQL_TCBinaryTreeNode class is a simple class used to link the objects in
// the binary tree together. To put anything useful into the tree, you must
// derive the object placed into the tree from OOSQL_TCBinaryTreeNode.
//---------------------------------------------------------------------------

class OOSQL_TCBinaryTreeNode {
protected:
    OOSQL_TCBinaryTreeNode  *left,*right,*parent;

    friend  class OOSQL_TCGenBinaryTree;
public:
            // Constructor to statisfy some compilers :-)
            OOSQL_TCBinaryTreeNode() {};

            // Virtual destructor to delete a binary tree node
    virtual ~OOSQL_TCBinaryTreeNode();

            // Virtual comparision function between nodes
    virtual int cmp(const OOSQL_TCBinaryTreeNode* node) const = 0;

            // Overload comparision operators
            bool operator < (const OOSQL_TCBinaryTreeNode& n)   { return cmp(&n) < 0; };
            bool operator <= (const OOSQL_TCBinaryTreeNode& n)  { return cmp(&n) <= 0; };
            bool operator == (const OOSQL_TCBinaryTreeNode& n)  { return cmp(&n) == 0; };
            bool operator != (const OOSQL_TCBinaryTreeNode& n)  { return cmp(&n) != 0; };
            bool operator >= (const OOSQL_TCBinaryTreeNode& n)  { return cmp(&n) >= 0; };
            bool operator > (const OOSQL_TCBinaryTreeNode& n)   { return cmp(&n) > 0; };
    };

//---------------------------------------------------------------------------
// The OOSQL_TCGenBinaryTree class is designed to manipulate a binary tree of
// OOSQL_TCBinaryTreeNode objects. In the simple form, BinartTreeNode objects
// contain nothing special. To add an arbitrary class to the tree, you must
// derive the class from OOSQL_TCBinaryTreeNode (either through single or multiple
// inheritance).
//---------------------------------------------------------------------------

class OOSQL_TCGenBinaryTree {
protected:
    UFour           count;      // Number of objects in tree
    OOSQL_TCBinaryTreeNode  *root;      // Pointer to first node in list
    OOSQL_TCBinaryTreeNode  *z;         // Pointer to last node in list
    OOSQL_TCBinaryTreeNode  hz[2];      // Space for head and z nodes

            // Internal methods
    static  void _empty(OOSQL_TCBinaryTreeNode *t);
    static  void _preOrder(OOSQL_TCBinaryTreeNode *t);
    static  void _inOrder(OOSQL_TCBinaryTreeNode *t);
    static  void _postOrder(OOSQL_TCBinaryTreeNode *t);
    static  void _range(OOSQL_TCBinaryTreeNode *t);
            OOSQL_TCBinaryTreeNode* _findMin(OOSQL_TCBinaryTreeNode *t) const;
            OOSQL_TCBinaryTreeNode* _findMax(OOSQL_TCBinaryTreeNode *t) const;

public:
            // Constructor
            OOSQL_TCGenBinaryTree();

            // Destructor
            ~OOSQL_TCGenBinaryTree();

            // Method to add a node to the tree
            OOSQL_TCBinaryTreeNode* add(OOSQL_TCBinaryTreeNode* node);

            // Methods to remove a node from the tree
            OOSQL_TCBinaryTreeNode* remove(OOSQL_TCBinaryTreeNode *key);
            OOSQL_TCBinaryTreeNode* removeMin();
            OOSQL_TCBinaryTreeNode* removeMax();

            // Methods to find a node in the tree
            OOSQL_TCBinaryTreeNode* find(OOSQL_TCBinaryTreeNode *key) const;
            OOSQL_TCBinaryTreeNode* findMin() const
                { return _findMin(root->right); };
            OOSQL_TCBinaryTreeNode* findMax() const
                { return _findMax(root->right); };

            // Methods to iterate through the tree
            OOSQL_TCBinaryTreeNode* next(OOSQL_TCBinaryTreeNode *node) const;
            OOSQL_TCBinaryTreeNode* prev(OOSQL_TCBinaryTreeNode *node) const;

            // Methods to traverse the binary tree
            void preOrder(void (*visit)(OOSQL_TCBinaryTreeNode*)) const;
            void inOrder(void (*visit)(OOSQL_TCBinaryTreeNode*)) const;
            void postOrder(void (*visit)(OOSQL_TCBinaryTreeNode*)) const;

            // Method to perform a range search on the tree
            void range(OOSQL_TCBinaryTreeNode *low,OOSQL_TCBinaryTreeNode *high,
                void (*visit)(OOSQL_TCBinaryTreeNode*)) const;

            // Empties the entire tree by destroying all nodes
            void empty();

            // Returns the number of items in the tree
            UFour numberOfItems(void) const { return count; };

            // Returns true if the tree is empty
            bool isEmpty(void) const        { return count == 0; };
    };

//---------------------------------------------------------------------------
// Set of template wrapper classes for declaring Type Safe binary trees.
// Note that the elements of the binary tree must still be derived from
// OOSQL_TCBinaryTreeNode.
//---------------------------------------------------------------------------

typedef void (*_OOSQL_TCGenBinaryTreeVisit)(OOSQL_TCBinaryTreeNode*);

template <class T> class OOSQL_TCBinaryTree : public OOSQL_TCGenBinaryTree {
public:
            T* remove(T* key)   { return (T*)OOSQL_TCGenBinaryTree::remove(key); };
            T* removeMin()      { return (T*)OOSQL_TCGenBinaryTree::removeMin(); };
            T* removeMax()      { return (T*)OOSQL_TCGenBinaryTree::removeMax(); };
            T* find(T* key)     { return (T*)OOSQL_TCGenBinaryTree::find(key); };
            T* findMin()        { return (T*)OOSQL_TCGenBinaryTree::findMin(); };
            T* findMax()        { return (T*)OOSQL_TCGenBinaryTree::findMax(); };
            T* next(T* node)    { return (T*)OOSQL_TCGenBinaryTree::next(node); };
            T* prev(T* node)    { return (T*)OOSQL_TCGenBinaryTree::prev(node); };
            void preOrder(void (*visit)(T*))
                { OOSQL_TCGenBinaryTree::preOrder((_OOSQL_TCGenBinaryTreeVisit)visit); };
            void inOrder(void (*visit)(T*))
                { OOSQL_TCGenBinaryTree::inOrder((_OOSQL_TCGenBinaryTreeVisit)visit); };
            void postOrder(void (*visit)(T*))
                { OOSQL_TCGenBinaryTree::postOrder((_OOSQL_TCGenBinaryTreeVisit)visit); };
            void range(T* low,T* high,void (*visit)(T*))
                { OOSQL_TCGenBinaryTree::range(low,high,(_OOSQL_TCGenBinaryTreeVisit)visit); };
    };

/*------------------------ Inline member functions ------------------------*/

#endif  // _OOSQL_TCL_BINTREE_H_
