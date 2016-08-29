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

#ifndef __OQL_POOLS_H__
#define __OQL_POOLS_H__

#include <stdio.h>
#if defined(WIN32) && defined(_DEBUG)
#include <typeinfo.h>
#endif
#include "OQL_OutStream.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Common.h"
#include "OQL_Template.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

#define DEFAULT_POOL_SIZE 32

/* 
 *  Declare pool index structure
 */
class  PoolIndexBase;
extern PoolIndexBase NULL_POOLINDEX;

class PoolIndexBase {
public:
	Four startIndex;
	Four size;
    
	void	setPoolIndex(Four astartIndex, Four asize)	{ startIndex = astartIndex; size = asize; }
	void	setNull()	{ startIndex = NULL_POOLINDEX.startIndex; size = NULL_POOLINDEX.size; }
	int		isNull()	{ if(startIndex == NULL_POOLINDEX.startIndex) return SM_TRUE; else return SM_FALSE; }
	friend OQL_OutStream& operator<<(OQL_OutStream& os, PoolIndexBase& object);
};

OQL_OutStream& operator<<(OQL_OutStream& os, PoolIndexBase& object);

inline int operator!=(PoolIndexBase& pi1, PoolIndexBase& pi2)
{
    if(pi1.startIndex == pi2.startIndex && pi1.size == pi2.size)
        return SM_FALSE;
    else 
        return SM_TRUE;
}

inline int operator==(PoolIndexBase& pi1, PoolIndexBase& pi2)
{
	if(pi1.startIndex == pi2.startIndex && pi1.size == pi2.size)
        return SM_TRUE;
    else 
        return SM_FALSE;
}

#ifndef TEMPLATE_NOT_SUPPORTED
template <class T> class OQL_Pool;
template <class T> class OQL_PoolElements;
template <class T>
class PoolIndex : public PoolIndexBase {
public:
    void				setPoolIndex(PoolIndexBase poolIndex);
	void				setPoolIndex(Four astartIndex, Four asize)	{ startIndex = astartIndex; size = asize; }
	PoolIndex<T>		getPoolIndex() { return *this; }

	T&					getElements(OQL_Pool<T>& pool, Four index);
	OQL_PoolElements<T> getElements(OQL_Pool<T>& pool);

	void				setNull();
	int					isNull();

	int operator!=(PoolIndexBase& pi);
#ifndef __LINUX__
	friend OQL_OutStream& operator<<(OQL_OutStream& os, PoolIndex<T>& object);
#endif
};

template <class T>
OQL_OutStream& operator<<(OQL_OutStream& os, PoolIndex<T>& object)
{
#if defined(WIN32) && defined(_DEBUG)
	os << typeid(object).name() << "[" << object.startIndex << "," << object.size << "]";

	return os;
#else
    if(object == NULL_POOLINDEX)
        os << "NULL_POOLINDEX";
    else
        os << "[" << object.startIndex << "," << object.size << "]";

    return os;
#endif
}

template <class T>
inline void PoolIndex<T>::setPoolIndex(PoolIndexBase poolIndex)
{
	startIndex = poolIndex.startIndex;
	size       = poolIndex.size;
}

template <class T>
inline T& PoolIndex<T>::getElements(OQL_Pool<T>& pool, Four index)  
{ 
	return pool[startIndex + index]; 
}

template <class T>
inline OQL_PoolElements<T> PoolIndex<T>::getElements(OQL_Pool<T>& pool)
{ 
	OQL_PoolElements<T> t(pool); 
	t.setPoolIndex(startIndex, size); 
	return t; 
}

template <class T>
inline void PoolIndex<T>::setNull()
{
	startIndex = NULL_POOLINDEX.startIndex;
	size       = NULL_POOLINDEX.size;	
}

template <class T>
inline int PoolIndex<T>::isNull()
{
	if(startIndex == NULL_POOLINDEX.startIndex &&  size == NULL_POOLINDEX.size)
		return SM_TRUE;
	else
		return SM_FALSE;
}

template<class T>
inline int PoolIndex<T>::operator!=(PoolIndexBase& pi)
{
    if(startIndex == pi.startIndex && size == pi.size)
        return SM_FALSE;
    else 
        return SM_TRUE;
}

#else		/* TEMPLATE_NOT_SUPPORTED */

#define PoolIndex(type)          name2(PoolIndex,type)
#define PoolIndexdeclare(type)												\
	class OQL_Pool(type);													\
	class OQL_PoolElements(type);											\
	class PoolIndex(type) : public PoolIndexBase {							\
	public:																	\
		void				setPoolIndex(PoolIndexBase poolIndex);		    \
		void				setPoolIndex(Four astartIndex, Four asize)		\
							{ startIndex = astartIndex; size = asize; }		\
		PoolIndex(type)		getPoolIndex() { return *this; }				\
																			\
		type&				   getElements(OQL_Pool(type)& pool, Four index); \
		OQL_PoolElements(type) getElements(OQL_Pool(type)& pool);			\
																			\
		void				   setNull();									\
		int					   isNull();									\
		int operator!=(PoolIndexBase& pi);									\
	};																		\
	inline void PoolIndex(type)::setPoolIndex(PoolIndexBase poolIndex)		\
	{																		\
		startIndex = poolIndex.startIndex;									\
		size       = poolIndex.size;										\
	}																		\
																			\
	inline void PoolIndex(type)::setNull()									\
	{																		\
		startIndex = NULL_POOLINDEX.startIndex;								\
		size       = NULL_POOLINDEX.size;									\
	}																		\
																			\
	inline int PoolIndex(type)::isNull()								    \
	{																		\
		if(startIndex == NULL_POOLINDEX.startIndex &&  size == NULL_POOLINDEX.size)	\
			return SM_TRUE;													\
		else																\
			return SM_FALSE;												\
	}																		\
																			\
	inline int PoolIndex(type)::operator!=(PoolIndexBase& pi)				\
	{																		\
		if(startIndex == pi.startIndex && size == pi.size)					\
			return SM_FALSE;												\
		else																\
		    return SM_TRUE;													\
	}


#define PoolIndeximplement(type)											\
	OQL_PoolElements(type) PoolIndex(type)::getElements(OQL_Pool(type)& pool) \
	{																		\
		OQL_PoolElements(type) t(pool);										\
		t.setPoolIndex(startIndex, size);									\
		return t;															\
	}																		\
																			\
	type& PoolIndex(type)::getElements(OQL_Pool(type)& pool, Four index)	\
	{																		\
		return pool[startIndex + index];									\
	}																		\
																			\

#endif		/* TEMPLATE_NOT_SUPPORTED */

/*
 *  Declare OQL_Pool
 */

#ifndef TEMPLATE_NOT_SUPPORTED
template <class T>
class OQL_Pool : public OOSQL_MemoryManagedObject {
public:
    OQL_Pool();
	OQL_Pool(OOSQL_MemoryManager* memoryManager);
    virtual ~OQL_Pool();

    PoolIndex<T>    addNewEntry();
    PoolIndex<T>    addNewEntry(Four nElements);
    Four            nElements()     { return top; };

    virtual char* name() { return "OQL_Pool"; }

    T& operator[](int index) {
        return elements[index];
    }

    friend OQL_OutStream& operator<<(OQL_OutStream& os, OQL_Pool<T>& object);

protected:
    Four    maxSize;           
    Four    top;             
    T*      elements;          
};

// OQL_Pool implementations
template <class T>
inline OQL_Pool<T>::OQL_Pool()
{
    maxSize  = DEFAULT_POOL_SIZE;
    elements = (T*)pMemoryManager->Alloc(sizeof(T) * maxSize); 
    
    if(elements == NULL)
        OOSQL_ERR_EXIT(eMEMORYALLOCERR_OOSQL);
    
    top = 0;
}

template <class T>
inline OQL_Pool<T>::OQL_Pool(OOSQL_MemoryManager* memoryManager)
{
	pMemoryManager = memoryManager;

    maxSize  = DEFAULT_POOL_SIZE;
    elements = (T*)pMemoryManager->Alloc(sizeof(T) * maxSize); 
    
    if(elements == NULL)
        OOSQL_ERR_EXIT(eMEMORYALLOCERR_OOSQL);
    
    top = 0;
}

template <class T>
inline OQL_Pool<T>::~OQL_Pool()
{
    if(elements)
        pMemoryManager->Free(elements);
}

template <class T>
PoolIndex<T> OQL_Pool<T>::addNewEntry()
{
    PoolIndex<T> poolIndex;

    poolIndex.startIndex = top;
    poolIndex.size       = 1;

    top ++;
    if(top >= maxSize)
    {
        // doubling the elements array
        T*   pTemp;
        Four i;

        // allocating new array
        pTemp = (T*)pMemoryManager->Alloc(sizeof(T) * maxSize * 2);
        if(pTemp == NULL)
            OOSQL_ERR_EXIT(eMEMORYALLOCERR_OOSQL);
        
        // copy entries which are used already from old array to new array
        for(i = 0;i < maxSize; i++)
            pTemp[i] = elements[i];

        // deallocating old array
        pMemoryManager->Free(elements);

        elements = pTemp;
        maxSize *= 2;
    }

    return poolIndex;
}

template <class T>
PoolIndex<T> OQL_Pool<T>::addNewEntry(Four nElements)
{
    Four		   i;
    PoolIndex<T>   poolIndex;

    poolIndex = addNewEntry();

    if(nElements > 0)
        for(i = 0;i < (nElements - 1); i++)
            addNewEntry();

    poolIndex.size = nElements;

    return poolIndex;
}

template <class T>
OQL_OutStream& operator<<(OQL_OutStream& os, OQL_Pool<T>& object)
{
    Four    i;

    os << "DUMP START: " << object.name() << endl;
    for(i = 0;i < object.top;i++)
    {
        os << "Element " << i << endl;
        os << object.elements[i] << endl;
    }
    os << "DUMP END  : " << object.name() << endl;
    return os;
}

#else   /* TEMPLATE_NOT_SUPPORTED */

#define OQL_Pool(type)          name2(OQL_Pool,type)
#define OQL_Pooldeclare(type)                                               \
    class OQL_Pool(type) : public OOSQL_MemoryManagedObject {               \
    public:                                                                 \
        OQL_Pool(type)();                                                   \
		OQL_Pool(type)(OOSQL_MemoryManager* memoryManager);					\
        virtual ~OQL_Pool(type)();                                          \
                                                                            \
        PoolIndex(type) addNewEntry();                                      \
        PoolIndex(type) addNewEntry(Four nElements);                        \
        Four            nElements()     { return top; };                    \
                                                                            \
        virtual char* name() { return "OQL_Pool"; }                         \
                                                                            \
        type& operator[](int index) {                                       \
            return elements[index];                                         \
        }                                                                   \
                                                                            \
        friend OQL_OutStream& operator<<(OQL_OutStream& os, OQL_Pool(type)& object);    \
    protected:                                                              \
        Four    maxSize;                                                    \
        Four    top;                                                        \
        type*   elements;                                                   \
    };                                                                      \
                                                                            \
    inline OQL_Pool(type)::OQL_Pool(type)()                                 \
    {                                                                       \
        maxSize  = DEFAULT_POOL_SIZE;										\
        elements = (type*)pMemoryManager->Alloc(sizeof(type) * maxSize);	\
																			\
        if(elements == NULL)												\
            OOSQL_ERR_EXIT(eMEMORYALLOCERR_OOSQL);							\
                                                                            \
        top = 0;                                                            \
    }                                                                       \
                                                                            \
	inline OQL_Pool(type)::OQL_Pool(type)(OOSQL_MemoryManager* memoryManager)		\
	{																		\
		pMemoryManager = memoryManager;										\
																			\
		maxSize  = DEFAULT_POOL_SIZE;										\
		elements = (type*)pMemoryManager->Alloc(sizeof(type) * maxSize);	\
																			\
		if(elements == NULL)												\
			OOSQL_ERR_EXIT(eMEMORYALLOCERR_OOSQL);							\
																			\
		top = 0;															\
	}																		\
                                                                            \
    inline OQL_Pool(type)::~OQL_Pool(type)()                                \
    {                                                                       \
        if(elements)														\
            pMemoryManager->Free(elements);                                 \
    }                                                                       
                                                                            
#define OQL_Poolimplement(type)                                             \
    PoolIndex(type) OQL_Pool(type)::addNewEntry()                           \
    {                                                                       \
        PoolIndex(type) poolIndex;                                          \
                                                                            \
        poolIndex.startIndex = top;                                         \
        poolIndex.size       = 1;                                           \
                                                                            \
        top ++;                                                             \
        if(top >= maxSize)                                                  \
        {                                                                   \
            type*   pTemp;                                                  \
            Four    i;                                                      \
                                                                            \
            pTemp  = (type*)pMemoryManager->Alloc(sizeof(type) * maxSize * 2);  \
            if(pTemp == NULL)                                               \
                OOSQL_ERR_EXIT(eMEMORYALLOCERR_OOSQL);                      \
                                                                            \
            for(i = 0;i < maxSize; i++)                                     \
                pTemp[i] = elements[i];                                     \
                                                                            \
            pMemoryManager->Free(elements);                                 \
                                                                            \
            elements = pTemp;                                               \
            maxSize *= 2;                                                   \
        }                                                                   \
                                                                            \
        return poolIndex;                                                   \
    }                                                                       \
                                                                            \
    PoolIndex(type) OQL_Pool(type)::addNewEntry(Four nElements)             \
    {                                                                       \
        Four        i;                                                      \
        PoolIndex(type)   poolIndex;                                        \
                                                                            \
        poolIndex = addNewEntry();                                          \
                                                                            \
        if(nElements > 0)                                                   \
            for(i = 0;i < (nElements - 1); i++)                             \
                addNewEntry();                                              \
                                                                            \
        poolIndex.size = nElements;                                         \
                                                                            \
        return poolIndex;                                                   \
    }                                                                       \
                                                                            \
    OQL_OutStream& operator<<(OQL_OutStream& os, OQL_Pool(type)& object)    \
    {                                                                       \
        Four    i;                                                          \
                                                                            \
        os << "DUMP START: " << object.name() << endl;                      \
                                                                            \
        for(i = 0;i < object.top;i++)                                       \
        {                                                                   \
            os << "Element " << i << endl;                                  \
            os << object.elements[i] << endl;                               \
        }                                                                   \
                                                                            \
        os << "DUMP END  : " << object.name() << endl;                      \
                                                                            \
        return os;                                                          \
    }

#endif  /* TEMPLATE_NOT_SUPPORTED */

/*
 *  Declare OQL_PoolElements
 */
#ifndef TEMPLATE_NOT_SUPPORTED
template <class T>
class OQL_PoolElements {
public:
	OQL_PoolElements(OQL_Pool<T>& poolObject);
    OQL_PoolElements();
	// In GCC-3.x or higher, copy constructor accepts only constant arguments  
    OQL_PoolElements(OQL_PoolElements<T> const& poolElements);
    virtual ~OQL_PoolElements();

    void         setPoolIndex(PoolIndexBase poolIndex);
    PoolIndex<T> getPoolIndex();
    void         setPoolIndex(Four startIndex, Four size);
    
	void		 setNull();
	int          isNull();

    OQL_Pool<T>&    getPool();
    void            setPool(OQL_Pool<T>& poolObject);

    T&                   element(int index);
    T&                   operator[](int index);
    OQL_PoolElements<T>& operator=(PoolIndex<T> pi);
	OQL_PoolElements<T>& operator=(PoolIndexBase pi);

public:
    Four    startIndex;
    Four    size;

private:
    OQL_Pool<T>*    pool;
};

// implementation
template <class T>
inline OQL_PoolElements<T>::OQL_PoolElements(OQL_Pool<T>& apoolObject)
{
    pool = &apoolObject;
    startIndex = 0;
    size       = 0;
}

template <class T>
inline OQL_PoolElements<T>::OQL_PoolElements()
{
    pool = NULL;
    startIndex = 0;
    size       = 0;
}

template <class T>
inline OQL_PoolElements<T>::OQL_PoolElements(OQL_PoolElements<T> const& poolElements)
{
    pool       = poolElements.pool;
    startIndex = poolElements.startIndex;
    size       = poolElements.size;
}

template <class T>
inline OQL_PoolElements<T>::~OQL_PoolElements() 
{}

template <class T>
inline void OQL_PoolElements<T>::setPoolIndex(PoolIndexBase apoolIndex)
{
    startIndex = apoolIndex.startIndex;
    size       = apoolIndex.size;
}

template <class T>
inline PoolIndex<T> OQL_PoolElements<T>::getPoolIndex()
{
    PoolIndex<T> poolIndex;

    poolIndex.startIndex = startIndex;
    poolIndex.size       = size;

    return poolIndex;
}

template <class T>
inline void OQL_PoolElements<T>::setPoolIndex(Four astartIndex, Four asize)
{
    startIndex = astartIndex;
    size       = asize;
}

template <class T>
inline void OQL_PoolElements<T>::setNull()
{
	startIndex = NULL_POOLINDEX.startIndex;
	size       = NULL_POOLINDEX.size;
}

template <class T>
inline int OQL_PoolElements<T>::isNull()
{
	if(startIndex == NULL_POOLINDEX.startIndex && size == NULL_POOLINDEX.size)
		return SM_TRUE;
	else
		return SM_FALSE;
}

template <class T>
inline OQL_Pool<T>& OQL_PoolElements<T>::getPool()
{
    return *pool;
}

template <class T>
inline void OQL_PoolElements<T>::setPool(OQL_Pool<T>& poolObject)
{
    pool = &poolObject;
}

template <class T>
inline T& OQL_PoolElements<T>::element(int index)
{
#if DEBUG
    if(index >= size)
        OOSQL_ERR_EXIT(eBOUNDARY_OVERFLOW_OQL);
    if(pool == NULL)
        OOSQL_ERR_EXIT(eNULLPOINTACCESS_OQL);
#endif
    return (*pool)[startIndex + index];
}

template <class T>
inline T& OQL_PoolElements<T>::operator[](int index)
{
    return element(index);
}

template <class T>
inline OQL_PoolElements<T>& OQL_PoolElements<T>::operator=(PoolIndex<T> pi)
{
    startIndex = pi.startIndex;
    size       = pi.size;

    return *this;
}

template <class T>
inline OQL_PoolElements<T>& OQL_PoolElements<T>::operator=(PoolIndexBase pi)
{
    startIndex = pi.startIndex;
    size       = pi.size;

    return *this;
}

template<class T>
inline int operator==(OQL_PoolElements<T>& pe1, OQL_PoolElements<T>& pe2)
{
    if(pe1.startIndex == pe2.startIndex && pe1.size == pe2.size)
        return SM_TRUE;
    else 
        return SM_FALSE;
}

template<class T>
inline int operator==(PoolIndex<T>& pi, OQL_PoolElements<T>& pe)
{
    if(pi.startIndex == pe.startIndex && pi.size == pe.size)
        return SM_TRUE;
    else 
        return SM_FALSE;
}

template<class T>
inline int operator==(OQL_PoolElements<T>& pe, PoolIndex<T>& pi)
{
    if(pi.startIndex == pe.startIndex && pi.size == pe.size)
        return SM_TRUE;
    else 
        return SM_FALSE;
}

template<class T>
inline int operator==(OQL_PoolElements<T>& pe, PoolIndexBase& pi)
{
    if(pi.startIndex == pe.startIndex && pi.size == pe.size)
        return SM_TRUE;
    else 
        return SM_FALSE;
}

template<class T>
inline int operator!=(OQL_PoolElements<T>& pe1, OQL_PoolElements<T>& pe2)
{
    if(pe1.startIndex == pe2.startIndex && pe1.size == pe2.size)
        return SM_FALSE;
    else 
        return SM_TRUE;
}

template<class T>
inline int operator!=(PoolIndex<T>& pi, OQL_PoolElements<T>& pe)
{
    if(pi.startIndex == pe.startIndex && pi.size == pe.size)
        return SM_FALSE;
    else 
        return SM_TRUE;
}

template<class T>
inline int operator!=(OQL_PoolElements<T>& pe, PoolIndex<T>& pi)
{
    if(pi.startIndex == pe.startIndex && pi.size == pe.size)
        return SM_FALSE;
    else 
        return SM_TRUE;
}

template<class T>
inline int operator!=(OQL_PoolElements<T>& pe, PoolIndexBase& pi)
{
    if(pi.startIndex == pe.startIndex && pi.size == pe.size)
        return SM_FALSE;
    else 
        return SM_TRUE;
}

#else   /* TEMPLATE_NOT_SUPPORTED */

#define OQL_PoolElements(type)      name2(OQL_PoolElements,type)
#define OQL_PoolElementsdeclare(type)                                       \
    class OQL_PoolElements(type) {                                          \
    public:                                                                 \
		OQL_PoolElements(type)(OQL_Pool(type)& poolObject);                 \
        OQL_PoolElements(type)();                                           \
        OQL_PoolElements(type)(OQL_PoolElements(type)& poolElements);       \
        virtual ~OQL_PoolElements(type)();                                  \
                                                                            \
        void         setPoolIndex(PoolIndexBase poolIndex);                 \
        PoolIndex(type) getPoolIndex();                                     \
        void         setPoolIndex(Four startIndex, Four size);              \
                                                                            \
		void		 setNull();												\
		int          isNull();												\
																			\
        OQL_Pool(type)& getPool();                                          \
        void            setPool(OQL_Pool(type)& poolObject);                \
                                                                            \
        type&               element(int index);                             \
        type&               operator[](int index);                          \
        OQL_PoolElements(type)& operator=(PoolIndex(type) pi);              \
		OQL_PoolElements(type)& operator=(PoolIndexBase pi);                \
                                                                            \
    public:                                                                 \
        Four    startIndex;                                                 \
        Four    size;                                                       \
                                                                            \
    private:                                                                \
        OQL_Pool(type)*    pool;                                            \
    };                                                                      \
    inline int operator==(OQL_PoolElements(type)& pe1, OQL_PoolElements(type)& pe2) \
    {                                                                       \
        if(pe1.startIndex == pe2.startIndex && pe1.size == pe2.size)        \
            return SM_TRUE;                                                 \
        else                                                                \
            return SM_FALSE;                                                \
    }                                                                       \
                                                                            \
    inline int operator==(PoolIndex(type)& pi, OQL_PoolElements(type)& pe)  \
    {                                                                       \
        if(pi.startIndex == pe.startIndex && pi.size == pe.size)            \
            return SM_TRUE;                                                 \
        else                                                                \
            return SM_FALSE;                                                \
    }                                                                       \
                                                                            \
    inline int operator==(OQL_PoolElements(type)& pe, PoolIndex(type)& pi)  \
    {                                                                       \
        if(pi.startIndex == pe.startIndex && pi.size == pe.size)            \
            return SM_TRUE;                                                 \
        else                                                                \
            return SM_FALSE;                                                \
    }                                                                       \
																			\
	inline int operator==(OQL_PoolElements(type)& pe, PoolIndexBase& pi)    \
    {                                                                       \
        if(pi.startIndex == pe.startIndex && pi.size == pe.size)            \
            return SM_TRUE;                                                 \
        else                                                                \
            return SM_FALSE;                                                \
    }                                                                       \
                                                                            \
    inline int operator!=(OQL_PoolElements(type)& pe1, OQL_PoolElements(type)& pe2) \
    {                                                                       \
        if(pe1.startIndex == pe2.startIndex && pe1.size == pe2.size)        \
            return SM_FALSE;                                                \
        else                                                                \
            return SM_TRUE;                                                 \
    }                                                                       \
                                                                            \
    inline int operator!=(PoolIndex(type)& pi, OQL_PoolElements(type)& pe)  \
    {                                                                       \
        if(pi.startIndex == pe.startIndex && pi.size == pe.size)            \
            return SM_FALSE;                                                \
        else                                                                \
            return SM_TRUE;                                                 \
    }                                                                       \
                                                                            \
	inline int operator!=(OQL_PoolElements(type)& pe, PoolIndex(type)& pi)  \
    {                                                                       \
        if(pi.startIndex == pe.startIndex && pi.size == pe.size)            \
            return SM_FALSE;                                                \
        else                                                                \
            return SM_TRUE;                                                 \
    }																		\
																			\
	inline int operator!=(OQL_PoolElements(type)& pe, PoolIndexBase& pi)    \
    {                                                                       \
        if(pi.startIndex == pe.startIndex && pi.size == pe.size)            \
            return SM_FALSE;                                                \
        else                                                                \
            return SM_TRUE;                                                 \
    }                                                                       \
                                                                            \
	inline OQL_PoolElements(type)::OQL_PoolElements(type)(OQL_Pool(type)& apoolObject) \
    {                                                                       \
        pool = &apoolObject;												\
        startIndex = 0;                                                     \
        size       = 0;                                                     \
    }                                                                       \
                                                                            \
	inline OQL_PoolElements(type)::OQL_PoolElements(type)()                 \
    {                                                                       \
        pool = NULL;                                                        \
        startIndex = 0;                                                     \
        size       = 0;                                                     \
    }                                                                       \
                                                                            \
    inline OQL_PoolElements(type)::OQL_PoolElements(type)(OQL_PoolElements(type)& poolElements)\
    {                                                                       \
        pool       = poolElements.pool;                                     \
        startIndex = poolElements.startIndex;                               \
        size       = poolElements.size;                                     \
    }                                                                       \
                                                                            \
    inline OQL_PoolElements(type)::~OQL_PoolElements(type)()                \
    {}                                                                      \
                                                                            \
    inline void OQL_PoolElements(type)::setPoolIndex(PoolIndexBase apoolIndex)    \
    {                                                                       \
        startIndex = apoolIndex.startIndex;                                 \
        size       = apoolIndex.size;                                       \
    }                                                                       \
                                                                            \
    inline PoolIndex(type) OQL_PoolElements(type)::getPoolIndex()           \
    {                                                                       \
        PoolIndex(type) poolIndex;                                          \
                                                                            \
        poolIndex.startIndex = startIndex;                                  \
        poolIndex.size       = size;                                        \
                                                                            \
        return poolIndex;                                                   \
    }                                                                       \
                                                                            \
    inline void OQL_PoolElements(type)::setPoolIndex(Four astartIndex, Four asize) \
    {                                                                       \
        startIndex = astartIndex;                                           \
        size       = asize;                                                 \
    }                                                                       \
																			\
	inline void OQL_PoolElements(type)::setNull()							\
	{																		\
		startIndex = NULL_POOLINDEX.startIndex;								\
		size       = NULL_POOLINDEX.size;									\
	}																		\
																			\
	inline int OQL_PoolElements(type)::isNull()								\
	{																		\
		if(startIndex == NULL_POOLINDEX.startIndex && size == NULL_POOLINDEX.size) \
			return SM_TRUE;													\
		else																\
			return SM_FALSE;												\
	}																		\
                                                                            \
    inline OQL_Pool(type)& OQL_PoolElements(type)::getPool()                \
    {                                                                       \
        return *pool;                                                       \
    }                                                                       \
                                                                            \
    inline void OQL_PoolElements(type)::setPool(OQL_Pool(type)& poolObject) \
    {                                                                       \
        pool = &poolObject;                                                 \
    }                                                                       \
                                                                            \
    inline type& OQL_PoolElements(type)::element(int index)                 \
    {                                                                       \
        return (*pool)[startIndex + index];                                 \
    }                                                                       \
                                                                            \
    inline type& OQL_PoolElements(type)::operator[](int index)              \
    {                                                                       \
        return element(index);                                              \
    }                                                                       \
                                                                            \
    inline OQL_PoolElements(type)& OQL_PoolElements(type)::operator=(PoolIndex(type) pi) \
    {                                                                       \
        startIndex = pi.startIndex;                                         \
        size       = pi.size;                                               \
                                                                            \
        return *this;                                                       \
    }                                                                       \
																			\
	inline OQL_PoolElements(type)& OQL_PoolElements(type)::operator=(PoolIndexBase pi) \
    {                                                                       \
        startIndex = pi.startIndex;                                         \
        size       = pi.size;                                               \
                                                                            \
        return *this;                                                       \
    }                                                                       

 #define OQL_PoolElementsimplement(type)
                                                                            
#endif  /* TEMPLATE_NOT_SUPPORTED */

#endif

