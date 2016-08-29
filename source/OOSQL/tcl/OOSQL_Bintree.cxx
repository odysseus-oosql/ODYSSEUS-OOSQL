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
* Description:	Member functions for the TCBinaryTree class, a class designed
*				for storing ordered information in a simple binary tree.
*				This class does nothing to ensure the binary tree is
*				balanced, so it may well become a degenerate linked list.
*				Use the AVLTree or RedBlackTree class if you require the
*				tree to be balanced.
*
****************************************************************************/

#include "OOSQL_Bintree.hxx"
#include <stdio.h>

/*--------------------------- Member functions ----------------------------*/

// Save a number of variabels globally when executing the recursive
// routines to save stack space and speed execution.

static	OOSQL_TCBinaryTreeNode	*o_z;
static	OOSQL_TCBinaryTreeNode	*o_low,*o_high;
static	void 					(*o_visit)(OOSQL_TCBinaryTreeNode*);

OOSQL_TCGenBinaryTree::OOSQL_TCGenBinaryTree()
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::OOSQL_TCGenBinaryTree
*
* Description:	Constructor for the OOSQL_TCGenBinaryTree class. We build an
*				empty binary tree. Note that we use special head and tail
*				nodes to simplify some of the tree manipulation routines.
*
****************************************************************************/
{
	count = 0;
	root = &hz[0];				// Setup root and tail pointers
	z = &hz[1];
	root->left = root->right = z->left = z->right = z;
	root->parent = z->parent = root;
}

OOSQL_TCGenBinaryTree::~OOSQL_TCGenBinaryTree()
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::~OOSQL_TCGenBinaryTree
*
* Description:	Destructor for the OOSQL_TCGenBinaryTree class. We simply empty
*				the tree of all nodes currently owned by the tree.
*
****************************************************************************/
{
	empty();
}

OOSQL_TCBinaryTreeNode* OOSQL_TCGenBinaryTree::add(OOSQL_TCBinaryTreeNode *node)
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::add
* Parameters:	node	- Node to add to the tree
* Returns:		NULL on success, or pointer to conflicting node.
*
* Description:	Adds a new node to the binary tree. If a node with the same
*				key already exists in the tree, we bomb out and return
*				a pointer to the existing node.
*
****************************************************************************/
{
	OOSQL_TCBinaryTreeNode	*t = root->right,*p = root;
	int				val = 1;		// Insert into right tree for first node

	// Search for the correct place to insert the node into the tree

	while (t != z) {
		p = t;
		if ((val = node->cmp(t)) < 0)
			t = t->left;
		else if (val > 0)
			t = t->right;
		else return t;				// Node already exists in tree
		}
	node->parent = p;
	if (val < 0)
		p->left = node;				// Insert into left subtree
	else p->right = node;			// Insert into right subtree
	node->left = node->right = z;
	count++;
	return NULL;					// Insertion was successful
}

OOSQL_TCBinaryTreeNode* OOSQL_TCGenBinaryTree::remove(OOSQL_TCBinaryTreeNode *key)
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::remove
* Parameters:	key	- Key to use to find the node to remove
* Returns:		Pointer to the removed node, NULL if node was not found.
*
* Description:
*
****************************************************************************/
{
	OOSQL_TCBinaryTreeNode *node,*p;

	if ((node = find(key)) == NULL)	// Find the node to delete
		return NULL;				// Node was not found

	if (node->left == z || node->right == z)
		p = node;
	else p = next(node);

	count--;
	return node;					// Deletion was successful
}

void OOSQL_TCGenBinaryTree::_empty(OOSQL_TCBinaryTreeNode* t)
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::_empty
* Parameters:	t	- Root of subtree to empty
*
* Description:	Recursive routine to delete all the nodes in a specified
*				subtree. This routine is declared as static to reduce
*				the runtime stack overheads required.
*
****************************************************************************/
{
	if (t != o_z) {
		_empty(t->left);
		_empty(t->right);
		delete t;
		}
}

void OOSQL_TCGenBinaryTree::empty()
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::empty
*
* Description:	Empties the tree of all nodes currently installed in the
*				tree.
*
****************************************************************************/
{
	o_z = z;
	_empty(root->right);
	root->left = root->right = z->left = z->right = z;
	root->parent = z->parent = root;
}

OOSQL_TCBinaryTreeNode* OOSQL_TCGenBinaryTree::find(OOSQL_TCBinaryTreeNode* key) const
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::find
* Parameters:	key	- Key used to determine if we have found the node
* Returns:		Pointer to the node found, NULL if not present.
*
* Description:	Looks up a specified node in the tree, return a pointer
*				to the actual node in the tree if found. We use a fast
*				iterative routine to perform this operation.
*
****************************************************************************/
{
	OOSQL_TCBinaryTreeNode* node = root->right;
	int				val;

	while (node != z) {
		if ((val = node->cmp(key)) == 0)
			break;
		if (val < 0)
			node = node->left;
		else node = node->right;
		}
	return (node == z) ? NULL : node;
}

OOSQL_TCBinaryTreeNode* OOSQL_TCGenBinaryTree::_findMin(OOSQL_TCBinaryTreeNode *t) const
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::_findMin
* Parameters:	t	- Root of subtree to search for minimum
* Returns:		Pointer to the smallest node in the tree, NULL if tree empty
*
****************************************************************************/
{
	while (t->left != z)
		t = t->left;
	return (t == z) ? NULL : t;
}

OOSQL_TCBinaryTreeNode* OOSQL_TCGenBinaryTree::_findMax(OOSQL_TCBinaryTreeNode *t) const
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::_findMax
* Parameters:	t	- Root of subtree to search for maximum
* Returns:		Pointer to the largest node in the tree, NULL if tree empty
*
****************************************************************************/
{
	while (t->right != z)
		t = t->right;
	return (t == z) ? NULL : t;
}

OOSQL_TCBinaryTreeNode* OOSQL_TCGenBinaryTree::next(OOSQL_TCBinaryTreeNode* node) const
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::next
* Parameters:	node	- Current node to find successor of
* Returns:		Successor node to the specified node, NULL if none.
*
* Description:	Finds the next node in the binary tree defined by an
*				inorder traversal of the tree. If there is no successor,
*				this routine returns NULL.
*
****************************************************************************/
{
	if (node->right != z)
		return _findMin(node->right);
	OOSQL_TCBinaryTreeNode *p = node->parent;
	while (p != root && p->right == node) {
		node = p;
		p = p->parent;
		}
	return (p == root) ? NULL : p;
}

OOSQL_TCBinaryTreeNode* OOSQL_TCGenBinaryTree::prev(OOSQL_TCBinaryTreeNode* node) const
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::prev
* Parameters:	node	- Current node to find predecessor of
* Returns:		Predecessor node to the specified node, NULL if none.
*
* Description:	Finds the previous node in the binary tree defined by an
*				inorder traversal of the tree. If there is no predecessor,
*				this routine returns NULL.
*
****************************************************************************/
{
	if (node->left != z)
		return _findMax(node->left);
	OOSQL_TCBinaryTreeNode *p = node->parent;
	while (p != root && p->left == node) {
		node = p;
		p = p->parent;
		}
	return (p == root) ? NULL : p;
}

void OOSQL_TCGenBinaryTree::_preOrder(OOSQL_TCBinaryTreeNode* t)
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::_preOrder
* Parameters:	t	- Root of subtree to perform traversal on
*
* Description:	Recursive routine to perform a pre-order traversal on the
*				subtree.
*
****************************************************************************/
{
	if (t != o_z) {
		o_visit(t);
		_preOrder(t->left);
		_preOrder(t->right);
		}
}

void OOSQL_TCGenBinaryTree::_inOrder(OOSQL_TCBinaryTreeNode* t)
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::_inOrder
* Parameters:	t	- Root of subtree to perform traversal on
*
* Description:	Recursive routine to perform an in-order traversal on the
*				subtree.
*
****************************************************************************/
{
	if (t != o_z) {
		_inOrder(t->left);
		o_visit(t);
		_inOrder(t->right);
		}
}

void OOSQL_TCGenBinaryTree::_postOrder(OOSQL_TCBinaryTreeNode* t)
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::_postOrder
* Parameters:	t	- Root of subtree to perform traversal on
*
* Description:	Recursive routine to perform a post-order traversal on the
*				subtree.
*
****************************************************************************/
{
	if (t != o_z) {
		_postOrder(t->left);
		_postOrder(t->right);
		o_visit(t);
		}
}

void OOSQL_TCGenBinaryTree::preOrder(void (*visit)(OOSQL_TCBinaryTreeNode*)) const
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::preOrder
* Parameters:	visit	- Function to call for each node visited
*
****************************************************************************/
{
	o_z = z;
	o_visit = visit;
	_preOrder(root->right);
}

void OOSQL_TCGenBinaryTree::inOrder(void (*visit)(OOSQL_TCBinaryTreeNode*)) const
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::inOrder
* Parameters:	visit	- Function to call for each node visited
*
****************************************************************************/
{
	o_z = z;
	o_visit = visit;
	_inOrder(root->right);
}

void OOSQL_TCGenBinaryTree::postOrder(void (*visit)(OOSQL_TCBinaryTreeNode*)) const
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::postOrder
* Parameters:	visit	- Function to call for each node visited
*
****************************************************************************/
{
	o_z = z;
	o_visit = visit;
	_postOrder(root->right);
}

void OOSQL_TCGenBinaryTree::_range(OOSQL_TCBinaryTreeNode* t)
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::_range
* Parameters:	t	- Root of subtree to perform range search on
*
* Description:	Recursive routine to perform a range search on the specified
*				subtree.
*
****************************************************************************/
{
	if (t != o_z) {
		if (*t < *o_low)
			_range(t->left);
		else if (*t > *o_high)
			_range(t->right);
		else {
			_range(t->left);
			o_visit(t);
			_range(t->right);
			}
		}
}

void OOSQL_TCGenBinaryTree::range(OOSQL_TCBinaryTreeNode* low,OOSQL_TCBinaryTreeNode *high,
	void (*visit)(OOSQL_TCBinaryTreeNode*)) const
/****************************************************************************
*
* Function:		OOSQL_TCGenBinaryTree::range
* Parameters:	low		- Low value to be used in range search
*				high	- High value to be used in range search
*				visit	- Function to call for each node with the range
*
* Description:	Visits all the nodes with the specified range, calling the
*				visit routine for each node within the range in sorted
*				order.
*
****************************************************************************/
{
	o_z = z;
	o_visit = visit;
	o_low = low;
	o_high = high;
	_range(root->right);
}

OOSQL_TCBinaryTreeNode::~OOSQL_TCBinaryTreeNode()
{
}
