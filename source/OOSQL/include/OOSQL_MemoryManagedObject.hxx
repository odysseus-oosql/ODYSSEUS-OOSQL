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

#ifndef _OOSQL_MEMORY_MANAGED_OBJECT_H_
#define _OOSQL_MEMORY_MANAGED_OBJECT_H_

#include "OOSQL_MemoryManager.hxx"
#include <stdlib.h>

#define OOSQL_NEW(PDEST, PMEMORYMANAGER, CLASS)             (PDEST) = new(PMEMORYMANAGER) CLASS
#define OOSQL_DELETE(POBJECT)                               delete POBJECT

#define OOSQL_ARRAYNEW(PDEST, PMEMORYMANAGER, CLASS, N)                             \
    {                                                                               \
        UFour   i;                                                                  \
        void*   p;                                                                  \
        void*   pObject;                                                            \
		UFour	n = (N);															\
		UFour	size = sizeof(CLASS)*n + sizeof(Four);								\
                                                                                    \
        p = (PMEMORYMANAGER)->Alloc(size);           \
        memcpy(p, &n, sizeof(UFour));                                                  \
                                                                                    \
        pObject = (char*)p + sizeof(UFour);                          		\
        for(i = 0; i < n; i++)                                             \
        {                                                                           \
            ((OOSQL_MemoryManagedObject*)pObject)->pMemoryManager = (PMEMORYMANAGER);\
            new(pObject) CLASS;                                                     \
            pObject = (char*)pObject + sizeof(CLASS);                               \
        }                                                                           \
        PDEST = (CLASS*)((char*)p + sizeof(UFour));                                 \
    }

#define OOSQL_ARRAYDELETE(CLASS, POBJECT)                                           \
    {                                                                               \
        UFour     i;                                                                \
        UFour     n;                                                                \
        void*     p = (POBJECT);                                                    \
                                                                                    \
        n = *(UFour*)((char*)(POBJECT) - sizeof(UFour));                                    \
        for(i = 0; i < n; i++)                                                      \
        {                                                                           \
            ((CLASS*)p)->~CLASS();                                                  \
            p = (char*)p + sizeof(CLASS);                                           \
        }                                                                           \
        p = (char*)(POBJECT) - sizeof(UFour);                                         \
        ((OOSQL_MemoryManagedObject*)(POBJECT))->pMemoryManager->Free(p);           \
    }

#define OOSQL_NOTMANAGED_ARRAYNEW(PDEST, PMEMORYMANAGER, CLASS, N)                  \
    {                                                                               \
        UFour   i;                                                                  \
        void*   p;                                                                  \
        void*   pObject;                                                            \
		UFour	n = (N);															\
		UFour	size = sizeof(CLASS)*n + sizeof(UFour);							\
																				\
        p = (PMEMORYMANAGER)->Alloc(size);           \
        memcpy(p, &n, sizeof(UFour));                                                 \
                                                                                    \
        pObject = (char*)p + sizeof(UFour);                                         \
        for(i = 0; i < n; i++)                                     \
        {                                                                           \
            new(pObject) CLASS;                                                     \
            pObject = (char*)pObject + sizeof(CLASS);                               \
        }                                                                           \
        PDEST = (CLASS*)((char*)p + sizeof(UFour));                                 \
    }

#define OOSQL_NOTMANAGED_ARRAYDELETE(CLASS, POBJECT, PMEMORYMANAGER)                \
    {                                                                               \
        UFour     i;                                                                \
        UFour     n;                                                                \
        void*   p = (POBJECT);                                                      \
                                                                                    \
        n = *(UFour*)((char*)(POBJECT) - sizeof(UFour));                            \
        for(i = 0; i < n; i++)                                                      \
        {                                                                           \
            ((CLASS*)p)->~CLASS();                                                  \
            p = (char*)p + sizeof(CLASS);                                           \
        }                                                                           \
        p = (char*)POBJECT - sizeof(UFour);                                         \
        (PMEMORYMANAGER)->Free(p);                                                  \
    }



class OOSQL_MemoryManagedObject {
public:
    // constructor
    OOSQL_MemoryManagedObject() {}
    OOSQL_MemoryManagedObject(OOSQL_MemoryManager* memoryManager) { pMemoryManager = memoryManager; }

    // destructor
    virtual ~OOSQL_MemoryManagedObject() {}

    static void* operator new(size_t size, void* location) { return location; }

    static void* operator new(size_t size, OOSQL_MemoryManager* memoryManager)
    {
        void* pObject;

        pObject = memoryManager->Alloc(size);
    
        if(pObject != NULL)
            ((OOSQL_MemoryManagedObject*)pObject)->pMemoryManager = memoryManager;

        return pObject;
    }

    static void  operator delete(void* pObject)
    {
        ((OOSQL_MemoryManagedObject*)pObject)->pMemoryManager->Free(pObject);
    }

    OOSQL_MemoryManager* pMemoryManager;
};

// In GCC-2.x, new operator definition is neccesary.
// But, in GCC-3.x or higher, no longer need new operator definition
#ifndef __NEW_DEF__
#if defined(_MSC_VER) || defined(__GNUC__) && __GNUC__ <= 2 
#ifndef __PLACEMENT_NEW_INLINE
#define __PLACEMENT_NEW_INLINE
inline void* operator new(size_t size, void* location) { return location; }
#endif
#endif
#endif

#endif

