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
* Description:  Header file for a creating typed stack classes, with the
*               objects in a contiguous array.
*
*
****************************************************************************/

#ifndef _OOSQL_TCL_CSTACK_H_
#define _OOSQL_TCL_CSTACK_H_

#include "OOSQL_Common.h"
#include "OOSQL_TCError.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

/*--------------------------- Class Definition ----------------------------*/

//---------------------------------------------------------------------------
// Template class to create a downward growing stack class that maintains
// elements of a particular type (and only this type) in a contiguous array.
// The following operations are supported on the stack:
//
//  StackName(int size) - Construct a stack of a given size
//  top()               - Examine the top item on the stack
//  element(int)        - Examine the nth item from the top of the stack
//  push(elementType&)  - Push an element onto the stack
//  pushq(elementType&) - Push an element with no range checking
//  pop()               - Pop the top element from the stack
//  popq()              - Pop the top element with no range checking
//  popn(int)           - Pop n items from the stack, returning top element
//  popqn(int)          - Pop n items from stack with no range checking
//  empty()             - Empty the stack of all elements
//  numberOfItems()     - Return the number of items on the stack
//  isEmpty()           - Returns true if stack is empty
//  isFull()            - Returns true if stack is full
//
//---------------------------------------------------------------------------

template <class T> class OOSQL_TCCStack {
protected:
    int         size;
    T           *stack;
    T           *p_stack;
	OOSQL_MemoryManager* pMemoryManager;
	bool		memoryManagedObjectFlag;	

public:
            OOSQL_TCCStack(int size,OOSQL_MemoryManager* memoryManager = NULL,bool memoryManagedObjectFlag = false)
            {
				if(pMemroyManager)
				{
					if(memoryManagedObjectFlag)
					{	OOSQL_ARRAYNEW(stack, pMemoryManager, T, size); }
					else
					{	OOSQL_NOTMANAGED_ARRAYNEW(stack, pMemoryManager, T, size); }
				}
				else
					stack = new T[size];

                OOSQL_TCCStack<T>::size = size;
                if (valid()) empty();
            };
    inline  ~OOSQL_TCCStack()       
			{ 
				if(pMemoryManager)
				{
					if(memoryManagedObjectFlag)
					{	OOSQL_ARRAYDELETE(T, stack); }
					else
					{	OOSQL_NOTMANAGED_ARRAYDELETE(T, stack, pMemoryManager); }
				}
				else
					delete [] stack;
			};
    inline  T& top() const          { return *p_stack; };
    inline  T& element(int offset)  { return *(p_stack + offset - 1); };
    inline  void push(T& node)
                { isFull() ? OOSQL_TCL_errorHandler(STK_OVERFLOW) : pushq(node); };
    inline  void pushq(T& node)     { *--p_stack = node; };
    inline  T& pop()
            {
                if (isEmpty())
                    OOSQL_TCL_errorHandler(STK_UNDERFLOW);
                return popq();
            };
    inline  T& popq()               { return *p_stack++; };
    inline  T& popn(int n)
            {
                if (numberOfItems() < n)
                    OOSQL_TCL_errorHandler(STK_UNDERFLOW);
                return popqn(n);
            };
    inline  T& popqn(int n)         { return (p_stack += n)[-n]; };
    inline  void empty()            { p_stack = stack + size; };
    inline  int numberOfItems() const
                { return size - (p_stack - stack); };
    inline  bool isEmpty() const
                { return p_stack >= (stack + size); };
    inline  bool isFull() const { return p_stack <= stack; };
    inline  bool valid() const      { return stack != NULL; };
    inline  int getSize() const     { return size; };
    };

//---------------------------------------------------------------------------
// Another template class to create a downward growing stack class with
// identical semantics to the above class. This version however has the
// size of the stack specified at compile time and the memory for the
// stack is allocated as part of the actual class (does not use new and
// delete).
//---------------------------------------------------------------------------

template <class T,int size> class OOSQL_TCStaticCStack {
protected:
    T           stack[size];
    T           *p_stack;

public:
            OOSQL_TCStaticCStack()      { empty(); };
    inline  T& top() const          { return *p_stack; };
    inline  T& element(int offset)  { return *(p_stack + offset - 1); };
    inline  void push(T& node)
                { isFull() ? OOSQL_TCL_errorHandler(STK_OVERFLOW) : pushq(node); };
    inline  void pushq(T& node)     { *--p_stack = node; };
    inline  T& pop()
            {
                if (isEmpty())
                    OOSQL_TCL_errorHandler(STK_UNDERFLOW);
                return popq();
            };
    inline  T& popq()               { return *p_stack++; };
    inline  T& popn(int n)
            {
                if (numberOfItems() < n)
                    OOSQL_TCL_errorHandler(STK_UNDERFLOW);
                return popqn(n);
            };
    inline  T& popqn(int n)         { return (p_stack += n)[-n]; };
    inline  void empty()            { p_stack = stack + size; };
    inline  int numberOfItems() const
                { return size - (p_stack - (T*)stack); };
    inline  bool isEmpty() const
                { return p_stack >= (stack + size); };
    inline  bool isFull() const     { return p_stack <= (T*)stack; };
    inline  int getSize() const     { return size; };
    };

#endif  // _OOSQL_TCL_CSTACK_H_
