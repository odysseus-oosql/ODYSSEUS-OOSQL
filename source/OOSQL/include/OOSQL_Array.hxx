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
* Description:  Header file for the set of Array clases, implemented using
*               templates. Defines the following classes:
*
*                   OOSQL_TCArray       - Array of T's
*                   OOSQL_TCSArray      - Sortable array of T's
*                   OOSQL_TCIArray      - Indirect array (array of pointers to T)
*                   OOSQL_TCISArray     - Indirect sortable array
*
*                   OOSQL_TCArrayIterator   - Iterator for array's
*                   OOSQL_TCIArrayIterator  - Iterator for indirect array's
*
*               Sortable array's are _not_ maintained in explicit sorted
*               order unless the addSorted() member function is used,
*               or the sort() member function is called. This allows
*               the arrays to be used to get data into sorted order
*               efficiently by filling the array normally and then calling
*               sort().
*
*
****************************************************************************/

#ifndef _OOSQL_TCL_ARRAY_H_
#define _OOSQL_TCL_ARRAY_H_

#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include "OOSQL_Techlib.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

/*--------------------------- Class Definition ----------------------------*/

template <class T> class OOSQL_TCArrayIterator;
template <class T> class OOSQL_TCIArrayIterator;

//---------------------------------------------------------------------------
// The following class maintains an array of T's in contiguous memory.
// The array will dynamically adjust it's size depending on how many items
// are stored in it. The class T must have a valid default constructor and
// copy semantics, as well as valid operator == semantics. For efficiency
// reasons, when direct array's are copied, we copy the data with memcpy(),
// rather than invoking the copy constructor for each element. This
// essentially always performs a shallow copy on each of the elements in
// the array, which would have to be manually deepened if a deep copy is
// required.
//
// The resizeable array uses a simple delta technique to expand the size
// of the array to a new possible size. A better technique that guarantees
// amortised O(1) access to array elements no matter how large the array
// needs to be is to use expand the size of the array by a ratio, rather
// than a set delta value.
//---------------------------------------------------------------------------

template <class T> class OOSQL_TCArray {
protected:
    T       *data;              // Pointer to the array of data
    UFour   _size,_delta;       // Size and increment values
    UFour   count;              // Number of items in array
	OOSQL_MemoryManager* pMemoryManager;
	bool	memoryManagedObjectFlag;

            // Resize the array to a new size
            void expand(UFour newSize);
            void shrink();

            // Member to compute the actual size
            UFour computeSize(UFour sz)
            {   return (_delta != 0) && (sz % _delta) ?
                    (((sz + _delta) / _delta) * _delta) : sz;
            };

public:
            // Default constructor
            OOSQL_TCArray(OOSQL_MemoryManager* memoryManager = NULL,bool memoryManagedObjectFlag = false);

            // Constructor
            OOSQL_TCArray(UFour size,UFour delta = 0,OOSQL_MemoryManager* memoryManager = NULL,bool memoryManagedObjectFlag = false);

            // Destructor
            ~OOSQL_TCArray();

            // Copy constructors
            OOSQL_TCArray(const OOSQL_TCArray<T>& a,UFour delta = UINT_MAX);
            const OOSQL_TCArray<T>& operator = (const OOSQL_TCArray<T>& a);

            // Comparision methods for arrays
            bool operator == (const OOSQL_TCArray<T>& a);
            bool operator != (const OOSQL_TCArray<T>& a)
                { return (!(*this == a))?true:false; };

            // Indexing operators
            T& operator [] (UFour index)
            {   PRECONDITION(data != NULL && index < count);
                return data[index];
            };

            const T& operator [] (UFour index) const
            {   PRECONDITION(data != NULL && index < count);
                return data[index];
            };

            // Methods to add elements to the array
            void add(T item);
            void insert(T item,UFour index,UFour count = 1);

            // Method to replace an element in the array
            void replace(T item,UFour index);

            // Method to remove an element from the array
            void remove(UFour index);

            // Method to search for an element in the array
            UFour search(T item,UFour first,UFour last,int direction = +1) const;
            UFour search(T item,int direction = +1) const
                { return search(item,0,count-1,direction); };

            // Overloaded cast to a const T* and T*
            T* ptr()                { return data; };
            const T* ptr() const    { return data; };

            // Method to set the number of elements in the array (empties it)
            void setCount(UFour newCount);

            // Return the number of items in the array
            UFour numberOfItems() const { return count; };

            // Returns true if the array is empty
            bool isEmpty() const        { return (count == 0)?true:false; };

            // Returns true if the array is full
            bool isFull() const
                { return ((_delta == 0) && (count == _size))?true:false; };

            // Return the size/delta for the array
            UFour size() const          { return _size; };
            UFour delta() const         { return _delta; };

            // Modify the delta value for the array
            void setDelta(UFour delta);

            // Resize the array to a new size (array is emptied of data)
            void resize(UFour newSize);

            // Empties the array of all data
            void empty();

            // Returns true if the array is valid
            bool valid() const          { return (data != NULL)?true:false; };

private:
    friend  class OOSQL_TCArrayIterator<T>;
    };

//---------------------------------------------------------------------------
// The following class maintains an array of T's in contiguous memory, and
// is derived from the Array class. The items in the array are assumed to
// be sortable, and have valid <, == and > operators. The items are _not_
// maintained in explicit sorted order unless the addSorted() member
// function is used to add items, or the sort() member function is called.
//---------------------------------------------------------------------------

template <class T> class OOSQL_TCSArray : public OOSQL_TCArray<T> {
public:
            // Comparison function for calling qsort().
    static  int cmp(const void *t1,const void *t2);

            // Default constructor
            OOSQL_TCSArray() : OOSQL_TCArray<T>() {};

            // Constructor
            OOSQL_TCSArray(UFour size,UFour delta = 0)
                : OOSQL_TCArray<T>(size,delta) {};

            // Method to add elements to the array in sorted order
            void addSorted(T item);

            // Method to do a binary search for an item
            UFour binarySearch(T item,UFour first,UFour last) const;
			// In GCC 3.4.x, for accessing local variables, use "this pointer"
            UFour binarySearch(T item) const
                { return binarySearch(item,0,this->count-1); };	

            // Method to sort the elements in the array
			// In GCC 3.4.x, for accessing local variables, use "this pointer"
            void sort()
                { qsort(this->data,this->count,sizeof(T),OOSQL_TCSArray<T>::cmp); };
    };

//---------------------------------------------------------------------------
// The following class maintains an array of pointers to T in contiguous
// memory. The array will dynamically adjust it's size depending on how
// many items are stored in it.
//
// If a copy is made of an array, shouldDelete is set to false so that
// we do not attempt to delete the data twice. You should still however be
// careful about copying indirect array's and freeing the memory used.
//---------------------------------------------------------------------------

template <class T> class OOSQL_TCIArray : public OOSQL_TCArray<T*> {
protected:
    bool    shouldDelete;           // Flags if elements should be deleted

public:
            // Default constructor
            OOSQL_TCIArray() : OOSQL_TCArray<T*>()
                { shouldDelete = true; };

            // Constructor
            OOSQL_TCIArray(UFour size,UFour delta = 0,bool shouldDelete = true)
                : OOSQL_TCArray<T*>(size,delta), shouldDelete(shouldDelete) {};

            // Destructor
            ~OOSQL_TCIArray()   { empty(); };

            // Copy constructors
            OOSQL_TCIArray(const OOSQL_TCIArray<T>& a,UFour delta = UINT_MAX)
                : OOSQL_TCArray<T*>(a,delta) { shouldDelete = false; };
            const OOSQL_TCIArray<T>& operator = (const OOSQL_TCIArray<T>& a);

            // Comparision methods for indirect arrays
            bool operator == (const OOSQL_TCIArray<T>& a);
            bool operator != (const OOSQL_TCIArray<T>& a)
                { return !(*this == a); };

            // Method to replace an element in the array
            void replace(T* item,UFour index);

            // Method to destroy an element in the array
            void destroy(UFour index);

            // Methods to search for an element in the array
            UFour search(const T* item,UFour first,UFour last,int direction = +1) const;
			// In GCC 3.4.x, for accessing local variables, use "this pointer"
            UFour search(const T* item,int direction = +1) const
                { return search(item,0,this->count-1,direction); };

            // Method to set the number of elements in the array (empties it)
            void setCount(UFour newCount);

            // Resize the array to a new size (array is emptied of data)
            void resize(UFour newSize);

            // Empties the array of all data
            void empty();

            // Method to set the shouldDelete value
            void setShouldDelete(bool s)    { shouldDelete = s; };

private:
    friend  class OOSQL_TCIArrayIterator<T>;
    };

//---------------------------------------------------------------------------
// The following class maintains an array of pointers to T in contiguous
// memory, and is derived from the OOSQL_TCIArray class. The items in the array are
// assumed to be sortable, and have valid <, == and > operators. The items
// are _not_ maintained in explicit sorted order unless the addSorted()
// member function is used to add items, or the sort() member function is
// called.
//---------------------------------------------------------------------------

template <class T> class OOSQL_TCISArray : public OOSQL_TCIArray<T> {
public:
            // Comparison function for calling qsort().
    static  int cmp(const void *t1,const void *t2);

            // Default constructor
            OOSQL_TCISArray() : OOSQL_TCIArray<T>() {};

            // Constructor
            OOSQL_TCISArray(UFour size,UFour delta = 0,bool shouldDelete = true)
                : OOSQL_TCIArray<T>(size,delta,shouldDelete) {};

            // Method to add elements to the array in sorted order
            void addSorted(T* item);

            // Method to do a binary search for an item
            UFour binarySearch(const T* item,UFour first,UFour last) const;
			// In GCC 3.4.x, for accessing local variables, use "this pointer"
            UFour binarySearch(const T* item) const
                { return binarySearch(item,0,this->count-1); };

            // Method to sort the elements in the array
			// In GCC 3.4.x, for accessing local variables, use "this pointer"
            void sort()
                { qsort(this->data,this->count,sizeof(T*),OOSQL_TCISArray<T>::cmp); };
    };

//---------------------------------------------------------------------------
// The following classes are used to iterate through the elements in the
// array. Generally you can simply do normal array indexing rather than
// using an iterator, but iterators are provided for all the fundamental
// data structures such as linked lists and thus arrays and lists can be
// treated the same through the use of iterators.
//---------------------------------------------------------------------------

template <class T> class OOSQL_TCArrayIterator {
protected:
    const OOSQL_TCArray<T>  *a;
    UFour               cursor,lower,upper;

public:
            // Default constructor
            OOSQL_TCArrayIterator()     { a = NULL; cursor = lower = upper = 0; };

            // Constructor given an array reference
            OOSQL_TCArrayIterator(const OOSQL_TCArray<T>& arr)
                { a = &arr; restart(0,arr.numberOfItems()); };

            // Constructor given an array reference and range
            OOSQL_TCArrayIterator(const OOSQL_TCArray<T>& arr,UFour start,UFour stop)
                { a = &arr; restart(start,stop); };

            // Initialise an array iterator from an array
            const OOSQL_TCArrayIterator<T>& operator = (const OOSQL_TCArray<T>& arr)
            {   a = &arr; restart(0,arr.numberOfItems());
                return *this;
            };

            // Initialise an array iterator from another iterator
            const OOSQL_TCArrayIterator<T>& operator = (const OOSQL_TCArrayIterator<T>& i)
            {   a = i.a; restart(i.lower,i.upper);
                return *this;
            };

            // Overloaded cast to an integer
            operator int ()     { return cursor < upper; };

            // Convert the iterator to the corresponding node
            T node()
            {   PRECONDITION(cursor < upper);
                return (*a)[cursor];
            };

            // Pre-increment operator
            T operator ++ ()
            {   if (++cursor < upper)
                    return (*a)[cursor];
                else
                    return (*a)[upper-1];
            };

            // Post-increment operator
            T operator ++ (int)
            {   if (cursor < upper)
                    return (*a)[cursor++];
                else
                    return (*a)[upper-1];
            };

            // Method to restart the array iterator
            void restart()  { restart(lower,upper); };

            // Method to restart with a new range
            void restart(UFour start,UFour stop)
                {  cursor = lower = start; upper = stop; };
    };

template <class T> class OOSQL_TCIArrayIterator : public OOSQL_TCArrayIterator<T*> {
public:
            // Default constructor
            OOSQL_TCIArrayIterator() : OOSQL_TCArrayIterator<T*>() {};

            // Constructor given an array reference
            OOSQL_TCIArrayIterator(const OOSQL_TCIArray<T>& arr) : OOSQL_TCArrayIterator<T*>(arr) {};

            // Constructor given an array reference and range
            OOSQL_TCIArrayIterator(const OOSQL_TCIArray<T>& arr,UFour start,UFour stop)
                : OOSQL_TCArrayIterator<T*>(arr,start,stop) {};

            // Initialise an array iterator from an array
            const OOSQL_TCIArrayIterator<T>& operator = (const OOSQL_TCIArray<T>& arr)
                { return (OOSQL_TCIArrayIterator<T>&)OOSQL_TCArrayIterator<T*>::operator=(arr); };

            // Initialise an array iterator from another iterator
            const OOSQL_TCIArrayIterator<T>& operator = (const OOSQL_TCIArrayIterator<T>& i)
                { return (OOSQL_TCIArrayIterator<T>&)OOSQL_TCArrayIterator<T*>::operator=(i); };
    };

/*------------------------ Inline member functions ------------------------*/

template <class T> inline void OOSQL_TCArray<T>::add(T item)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::add
* Parameters:   item    - Item to add to the array
*
* Description:  Resizes the array if the count exceeds the size of the
*               array, and copies the item into the last position.
*
****************************************************************************/
{
    CHECK(valid());
    if (count == _size)
        expand(count+1);
    CHECK(count < _size);
    if (valid())
        data[count++] = item;
}

template <class T> inline void OOSQL_TCArray<T>::setCount(UFour newCount)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::setCount
* Parameters:   newCount    - New count for the array
*
* Description:  Sets the count value for the array, expanding the size
*               if necessary. The array will be filled with the default
*               value.
*
****************************************************************************/
{
    resize(newCount);
    count = newCount;
}

template <class T> inline void OOSQL_TCArray<T>::replace(T item,UFour index)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::replace
* Parameters:   item    - Item to replace in the array
*               index   - Index of the item in the array to replace
*
* Description:  Replaces the item in the array with the new item. The index
*               MUST fall within the current bounds of the array.
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(index < count);
    data[index] = item;
}

template <class T> inline void OOSQL_TCIArray<T>::setCount(UFour newCount)
/****************************************************************************
*
* Function:     OOSQL_TCIArray<T>::setCount
* Parameters:   newCount    - New count for the array
*
* Description:  Sets the count value for the array, expanding the size
*               if necessary. The array will be filled with the default
*               value.
*
****************************************************************************/
{
    resize(newCount);
	// In GCC 3.4.x, for accessing local variables, use "this pointer"
    this->count = newCount;
}

/*---------------------------- Implementation -----------------------------*/

template <class T> OOSQL_TCArray<T>::OOSQL_TCArray(OOSQL_MemoryManager* memoryManager, bool memoryManagedObject)
    : _size(1), _delta(1), count(0), pMemoryManager(memoryManager), memoryManagedObjectFlag(memoryManagedObject)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::OOSQL_TCArray
*
* Description:  Default constructor for the array template. This creates
*               an empty array of size 1, with an increment of 1.
*
****************************************************************************/
{
	if(pMemoryManager)
	{
		if(memoryManagedObjectFlag)
		{	
			OOSQL_ARRAYNEW(data, pMemoryManager, T, _size); 
		}
		else
		{	
			OOSQL_NOTMANAGED_ARRAYNEW(data, pMemoryManager, T, _size); 
		}
	}
	else
		data = new T[_size];

    CHECK(data != NULL);
}

template <class T> OOSQL_TCArray<T>::OOSQL_TCArray(UFour size,UFour delta,OOSQL_MemoryManager* memoryManager, bool memoryManagedObject)
    : _delta(delta), count(0), pMemoryManager(memoryManager), memoryManagedObjectFlag(memoryManagedObject)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::OOSQL_TCArray
* Parameters:   size    - Initial size of the array
*               delta   - Initial increment value for the array
*
* Description:  Constructor for the array template, given the size of the
*               array to create, and the expansion ratio. If the increment
*               is zero, the array is of a fixed size and will not be
*               expanded.
*
*               The array is initially empty containing no valid data.
*
****************************************************************************/
{
    _size = computeSize(size);
   	if(pMemoryManager)
	{
		if(memoryManagedObjectFlag)
		{	OOSQL_ARRAYNEW(data, pMemoryManager, T, _size); }
		else
		{	OOSQL_NOTMANAGED_ARRAYNEW(data, pMemoryManager, T, _size); }
	}
	else
		data = new T[_size];
    CHECK(data != NULL);
}

template <class T> OOSQL_TCArray<T>::OOSQL_TCArray(const OOSQL_TCArray<T>& a,UFour delta)
    : count(a.count)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::OOSQL_TCArray
* Parameters:   a   - OOSQL_TCArray to copy
*
* Description:  Copy constructor for array's. We simply allocate space for
*               the new array, and copy each item individually. We copy
*               the data with a fast memcpy.
*
****************************************************************************/
{
    CHECK(a.valid());
    _delta = (delta == UINT_MAX) ? a._delta : delta;
    _size = computeSize(a._size);

	pMemoryManager          = a.pMemoryManager;
	memoryManagedObjectFlag = a.memoryManagedObjectFlag;
	if(pMemoryManager)
	{
		if(memoryManagedObjectFlag)
		{	OOSQL_ARRAYNEW(data, pMemoryManager, T, _size); }
		else
		{	OOSQL_NOTMANAGED_ARRAYNEW(data, pMemoryManager, T, _size); }
	}
	else
		data = new T[_size];
    if (valid())
    {
        for(int i = 0; i < count; i++)
            data[i] = a.data[i];
    }
}

template <class T> OOSQL_TCArray<T>::~OOSQL_TCArray()
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::~OOSQL_TCArray
* Parameters:   
*
* Description:  Destructor
*
****************************************************************************/
{
	if(pMemoryManager)
	{
		if(memoryManagedObjectFlag)
		{	OOSQL_ARRAYDELETE(T, data); }
		else
		{	OOSQL_NOTMANAGED_ARRAYDELETE(T, data, pMemoryManager); }
	}
	else
		delete [] data;
}

template <class T> const OOSQL_TCArray<T>& OOSQL_TCArray<T>::operator = (const OOSQL_TCArray<T>& a)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::operator =
* Parameters:   a   - Array to copy
*
* Description:  Assignment operator for array's. Allocates space for the
*               new array and copies the data with a fast memcpy call.
*
****************************************************************************/
{
    CHECK(valid() && a.valid());
    if (data != a.data) {
        _size = a._size;
        _delta = a._delta;
        count = a.count;
		if(pMemoryManager)
		{
			if(memoryManagedObjectFlag)
			{	OOSQL_ARRAYDELETE(T, data); }
			else
			{	OOSQL_NOTMANAGED_ARRAYDELETE(T, data, pMemoryManager); }
		}
		else
			delete [] data;
        if(pMemoryManager)
		{
			if(memoryManagedObjectFlag)
			{	OOSQL_ARRAYNEW(data, pMemoryManager, T, _size); }
			else
			{	OOSQL_NOTMANAGED_ARRAYNEW(data, pMemoryManager, T, _size); }
		}
		else
			data = new T[_size];
        if (valid())
        {   
            for(int i = 0; i < count; i++)
                data[i] = a.data[i];
        }
    }
    return *this;
}

template <class T> bool OOSQL_TCArray<T>::operator == (const OOSQL_TCArray<T>& a)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::operator ==
* Parameters:   a   - Array to compare to this array
* Returns:      True if the array elements are all equal, false if not.
*
****************************************************************************/
{
    CHECK(valid() && a.valid());
    if (count != a.count)
        return false;
    T *p = data, *pa = a.data;
    for (UFour i = 0; i < count; i++)
        if (!(*p++ == *pa++))
            return false;
    return true;
}

template <class T> void OOSQL_TCArray<T>::insert(T item,UFour index,UFour aCount)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::insert
* Parameters:   item    - Item to insert into the array
*               index   - Index to insert the item in front of
*               aCount  - Count of elements to insert
*
* Description:  Inserts the specified item into the array at the index'th
*               position. All the elements from [index,count] are moved
*               up one position to make room. The index must be in the
*               range [0,count], and if the value is count it is simply
*               tacked onto the end of the array.
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(index <= count);
    if (count+aCount > _size)   expand(count+aCount);

    // Move the items up one position in the array to make room, and insert

    if (valid()) {
		for(int i = 0; i < (count - index); i++)
		{
			data[index + aCount + i] = data[index + i];
		}

        for (UFour i = 0; i < aCount; i++)
            data[index+i] = item;
        count += aCount;
        }
}

template <class T> void OOSQL_TCArray<T>::remove(UFour index)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::remove
* Parameters:   index   - Index of element to remove
*
* Description:  Removes an indexed element from the array, by copying all
*               the data down one position. The index must be in the
*               range [0,count).
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(index < count);

    // Move the items down one position, and shrink the allocated memory

    count--;
	for(int i = 0; i < (count - index); i++)
	{
		data[index + i] = data[index + 1 + i];
	}
    shrink();
}

template <class T> UFour OOSQL_TCArray<T>::search(T item,UFour first,UFour last,
    int direction) const
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::search
* Parameters:   item        - Item to search for
*               first       - Index of first element to search
*               last        - Index of last element to search
*               direction   - End to search array from (+1 = start, -1 = end)
* Returns:      Index of the item in the array, UINT_MAX if not found
*
* Description:  Performs a simple linear search for the item from the
*               specified end of the array.
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(first < count && last < count);
    PRECONDITION(direction == +1 || direction == -1);
    if (direction == +1) {
        T *p = &data[first];
        for (UFour i = first; i <= last; i++)
            if (*p++ == item)
                return i;
        }
    else {
        T *p = &data[last];
        for (UFour i = last; i >= first; i--)
            if (*p-- == item)
                return i;
        }
    return UINT_MAX;
}

template <class T> void OOSQL_TCArray<T>::expand(UFour newSize)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::expand
* Parameters:   newSize - New size of the array
*
* Description:  Expands the array to be a multiple of 'delta' that includes
*               the specified newSize for the array. The array data is
*               re-allocated and copied to the resized array.
*
*               Note that 'count' must contain the actual number of elements
*               in the array.
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(_delta != 0);  // Array is not resizeable when _delta == 0
    PRECONDITION(newSize >= count);

    T *temp;

	_size = computeSize(newSize);
	if(pMemoryManager)
	{
		if(memoryManagedObjectFlag)
		{	OOSQL_ARRAYNEW(temp, pMemoryManager, T, _size); }
		else
		{	OOSQL_NOTMANAGED_ARRAYNEW(temp, pMemoryManager, T, _size); }
	}
	else
		temp = new T[_size];

    // Now copy the data from the old array into the newly resized array.
    // Note that we use a fast memcpy to do this.

    if (temp)
    {
        for(int i = 0; i < (int)count; i++)
            temp[i] = data[i];
    }
	if(pMemoryManager)
	{
		if(memoryManagedObjectFlag)
		{	OOSQL_ARRAYDELETE(T, data); }
		else
		{	OOSQL_NOTMANAGED_ARRAYDELETE(T, data, pMemoryManager); }
	}
	else
		delete [] data;
    data = temp;
}

template <class T> void OOSQL_TCArray<T>::shrink()
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::shrink
*
* Description:  Shrinks the allocated space for the array, if the threshold
*               point has been reached.
*
****************************************************************************/
{
    CHECK(valid());
    if (_delta == 0)        // Array is not resizeable when _delta == 0
        return;

    // Only shrink the array when the amount of free space gets smaller
    // than half of the delta value, and it is at least delta in size

    if ((_size - count) > (_delta + _delta/2) && (_size > _delta)) {
        T *temp;

		_size = computeSize(_size - _delta);
		if(pMemoryManager)
		{
			if(memoryManagedObjectFlag)
			{	OOSQL_ARRAYNEW(temp, pMemoryManager, T, _size); }
			else
			{	OOSQL_NOTMANAGED_ARRAYNEW(temp, pMemoryManager, T, _size); }
		}
		else
			temp = new T[_size];

        if (temp)
        {   
            for(int i = 0; i < (int)count; i++)
                temp[i] = data[i];
        }

		if(pMemoryManager)
		{
			if(memoryManagedObjectFlag)
			{	OOSQL_ARRAYDELETE(T, data); }
			else
			{	OOSQL_NOTMANAGED_ARRAYDELETE(T, data, pMemoryManager); }
		}
		else
			delete [] data;
        data = temp;
    }
}

template <class T> void OOSQL_TCArray<T>::setDelta(UFour delta)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::setDelta
* Parameters:   delta   - New delta value for the array
*
* Description:  Sets the delta value for the array, expanding or shrinking
*               the array as need be.
*
****************************************************************************/
{
    if (delta >= _delta) {
        _delta = delta;
        expand(_size);
        }
    else {
        _delta = delta;
        shrink();
        }
}

template <class T> void OOSQL_TCArray<T>::resize(UFour newSize)
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::resize
* Parameters:   newSize - New size for the array
*
* Description:  Resizes the array to the new size. If the array is non-
*               resizeable, we bomb out. Note that the array will be empty
*               after this operation.
*
****************************************************************************/
{
    PRECONDITION(_delta != 0);
    empty();
    expand(newSize);
}

template <class T> void OOSQL_TCArray<T>::empty()
/****************************************************************************
*
* Function:     OOSQL_TCArray<T>::empty
*
* Description:  Empties the array of all elements.
*
****************************************************************************/
{
    count = 0;
    shrink();
}

template <class T> int OOSQL_TCSArray<T>::cmp(const void *t1,const void *t2)
/****************************************************************************
*
* Function:     OOSQL_TCSArray<T>::cmp
* Parameters:   t1,t2   - Elements to compare
* Returns:      Result of comparision:
*
*                   t1 < t2,  -1
*                   t1 == t2, 0
*                   t1 > t2,  1
*
****************************************************************************/
{
    if (*((T*)t1) < *((T*)t2))
        return -1;
    else if (*((T*)t1) > *((T*)t2))
        return 1;
    return 0;
}

template <class T> void OOSQL_TCSArray<T>::addSorted(T item)
/****************************************************************************
*
* Function:     OOSQL_TCSArray<T>::addSorted
* Parameters:   item    - Item to add to the array
*
* Description:  Adds the element to the array in sorted order. This
*               function will only work if the elements are already in
*               sorted order, which can be achieved by calling sort().
*
****************************************************************************/
{
    // Search for the spot to put the new item, and insert it into the
    // array.

    CHECK(valid());
    T *p = this->data;	
	UFour i;
    for (i = 0; i < this->count; i++)
        if (*p++ > item)
            break;
    insert(item,i);
}

template <class T> UFour OOSQL_TCSArray<T>::binarySearch(T item,UFour L,UFour R) const
/****************************************************************************
*
* Function:     OOSQL_TCSArray<T>::binarySearch
* Parameters:   item    - Item to search for in the array
*               L       - Index of first element to search
*               R       - Index of last element to search
* Returns:      Index of the item in the array, UINT_MAX if not found
*
* Description:  Performs a standard binary search on the array looking
*               for the specified item. The elements in the array _must_
*               be in sorted order for this function to work (by calling
*               the sort() member function).
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(L < count && R < count);

    while (L < R) {
        UFour M = (L+R)/2;
        if ((this->data)[M] == item)	
            return M;
        if ((this->data)[M] < item)		
            L = M+1;
        else R = M-1;
        }
    if ((this->data)[L] == item)		
        return L;
    return UINT_MAX;
}

template <class T> const OOSQL_TCIArray<T>& OOSQL_TCIArray<T>::operator = (const OOSQL_TCIArray<T>& a)
/****************************************************************************
*
* Function:     OOSQL_TCIArray<T>::operator =
* Parameters:   a   - Array to copy
*
* Description:  Assignment operator for OOSQL_TCIArray's. First empties the array
*               the copies it.
*
****************************************************************************/
{
    // Check to make sure we are not being assigned to ourselves :-)

    CHECK(valid() && a.valid());
    if (this->data != a.data) {		
        empty();
        shouldDelete = false;
        OOSQL_TCArray<T*>::operator=(a);
        }
    return *this;
}

template <class T> bool OOSQL_TCIArray<T>::operator == (const OOSQL_TCIArray<T>& a)
/****************************************************************************
*
* Function:     OOSQL_TCIArray<T>::operator ==
* Parameters:   a   - Array to compare to this array
* Returns:      True if the array elements are all equal, false if not.
*
****************************************************************************/
{
    CHECK(valid() && a.valid());
    if (this->count != a.count)				
        return false;
    T **p = this->data, **pa = a.data;			
    for (UFour i = 0; i < this->count; i++)	
        if (!(*(*p++) == *(*pa++)))
            return false;
    return true;
}

template <class T> void OOSQL_TCIArray<T>::replace(T* item,UFour index)
/****************************************************************************
*
* Function:     OOSQL_TCIArray<T>::replace
* Parameters:   item    - Item to replace in the array
*               index   - Index of the item in the array to replace
*
* Description:  Replaces the item in the array with the new item. The index
*               MUST fall within the current bounds of the array.
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(index < count);
    if (shouldDelete)
        delete (this->data)[index];		
    (this->data)[index] = item;			
}

template <class T> void OOSQL_TCIArray<T>::destroy(UFour index)
/****************************************************************************
*
* Function:     OOSQL_TCIArray<T>::destroy
* Parameters:   index   - Index of element to remove
*
* Description:  Removes an indexed element from the array, by copying all
*               the data down one position. The index must be in the
*               range [0,count). If shouldDelete is true, the element is
*               deleted.
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(index < count);

    // Move the items down one position, and shrink the allocated memory

    if (shouldDelete)
        delete (this->data)[index];		
    this->count--;	
    memmove(&((this->data)[index]),&((this->data)[index+1]),(this->count-index) * sizeof(T*));	
    this->shrink();	
}

template <class T> UFour OOSQL_TCIArray<T>::search(const T* item,UFour first,UFour last,
    int direction) const
/****************************************************************************
*
* Function:     OOSQL_TCIArray<T>::search
* Parameters:   item        - Item to search for
*               first       - Index of first element to search
*               last        - Index of last element to search
*               direction   - End to search array from (+1 = start, -1 = end)
* Returns:      Index of the item in the array, UINT_MAX if not found
*
* Description:  Performs a simple linear search for the item from the
*               specified end of the array.
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(first < count && last < count);
    PRECONDITION(direction == +1 || direction == -1);
    if (direction == +1) {
        T **p = &((this->data)[first]);	
        for (UFour i = first; i <= last; i++)
            if (*(*p++) == *item)
                return i;
        }
    else {
        T **p = &((this->data)[last]);
        for (UFour i = last; i >= first; i--)
            if (*(*p--) == *item)
                return i;
        }
    return UINT_MAX;
}

template <class T> void OOSQL_TCIArray<T>::resize(UFour newSize)
/****************************************************************************
*
* Function:     OOSQL_TCIArray<T>::resize
* Parameters:   newSize - New size for the array
*
* Description:  Resizes the array to the new size. If the array is non-
*               resizeable, we bomb out. Note that the array will be empty
*               after this operation.
*
****************************************************************************/
{
    PRECONDITION(_delta != 0);
    empty();
    this->expand(newSize);
}

template <class T> void OOSQL_TCIArray<T>::empty()
/****************************************************************************
*
* Function:     OOSQL_TCIArray<T>::empty
*
* Description:  Deletes all of the elements if shouldDelete is set to true
*               (the default).
*
****************************************************************************/
{
    if (shouldDelete) {
        for (UFour i = 0; i < this->count; i++)	
            delete (T*)((this->data)[i]);		
        }
    OOSQL_TCArray<T*>::empty();
}

template <class T> int OOSQL_TCISArray<T>::cmp(const void *t1,const void *t2)
/****************************************************************************
*
* Function:     OOSQL_TCISArray<T>::cmp
* Parameters:   t1,t2   - Elements to compare
* Returns:      Result of comparision:
*
*                   t1 < t2,  -1
*                   t1 == t2, 0
*                   t1 > t2,  1
*
****************************************************************************/
{
    if (**((T**)t1) < **((T**)t2))
        return -1;
    else if (**((T**)t1) > **((T**)t2))
        return 1;
    return 0;
}

template <class T> void OOSQL_TCISArray<T>::addSorted(T* item)
/****************************************************************************
*
* Function:     OOSQL_TCISArray<T>::addSorted
* Parameters:   item    - Item to add to the array
*
* Description:  Adds the element to the array in sorted order. This
*               function will only work if the elements are already in
*               sorted order, which can be achieved by calling sort().
*
****************************************************************************/
{
    UFour   i;

    // Search for the spot to put the new item, and insert it into the
    // array.

    CHECK(valid());
    T **p = this->data;					
    for (i = 0; i < this->count; i++)	
        if (*(*p++) > *item)
            break;
    insert(item,i);
}

template <class T> UFour OOSQL_TCISArray<T>::binarySearch(const T* item,UFour L,
    UFour R) const
/****************************************************************************
*
* Function:     OOSQL_TCISArray<T>::binarySearch
* Parameters:   item    - Item to search for in the array
*               L       - Index of first element to search
*               R       - Index of last element to search
* Returns:      Index of the item in the array, UINT_MAX if not found
*
* Description:  Performs a standard binary search on the array looking
*               for the specified item. The elements in the array _must_
*               be in sorted order for this function to work (by calling
*               the sort() member function).
*
****************************************************************************/
{
    CHECK(valid());
    PRECONDITION(L < count && R < count);

    while (L < R) {
        UFour M = (L+R)/2;
        if (*((this->data)[M]) == *item)	
            return M;
        if (*((this->data)[M]) < *item)		
            L = M+1;
        else R = M-1;
        }
    if (*((this->data)[L]) == *item)		
        return L;
    return UINT_MAX;
}

#endif  // _OOSQL_TCL_ARRAY_H_
