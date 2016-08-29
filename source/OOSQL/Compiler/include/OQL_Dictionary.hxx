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

#ifndef __OQL_DICTIONARY_H__
#define __OQL_DICTIONARY_H__

#include "OOSQL_Common.h"
#include "OOSQL_Error.h"
#include "OQL_Template.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

const int DIC_INITIAL_ARRAY_SIZE = 10;

/*
 *  Declare Dictionary
 */
#ifndef TEMPLATE_NOT_SUPPORTED
template <class Key, class Info>
class Dictionary : public OOSQL_MemoryManagedObject {
public:
    Dictionary();
    virtual ~Dictionary();

    Four AddEntry(Key key, Info info);
    Four FindItem(Key key, Info& info);
    Four DeleteItem(Key key);

protected:
    Key*            keys;               // SunC++ 3.0 can't define struct in class, so define separately.
    Info*           infos;
    Boolean*        useds;
    Four            size;
};

// Dictionary implementations
template <class Key, class Info>
Dictionary<Key,Info>::Dictionary()
{
	Four i;
    
    size = DIC_INITIAL_ARRAY_SIZE;
        
    keys  = (Key*)pMemoryManager->Alloc(sizeof(Key) * size);
    infos = (Info*)pMemoryManager->Alloc(sizeof(Info) * size);
    useds = (Boolean*)pMemoryManager->Alloc(sizeof(Boolean) * size);
    if(keys == NULL || infos == NULL || useds == NULL)
        OOSQL_ERR_EXIT(eMEMORYALLOCERR_OOSQL);

    
    for(i = 0;i < size;i++)
        useds[i] = SM_FALSE;
}

template <class Key, class Info>
Dictionary<Key,Info>::~Dictionary()
{
    if(keys)
        pMemoryManager->Free(keys);
    if(infos)
        pMemoryManager->Free(infos);
    if(useds)
        pMemoryManager->Free(useds);
}

template <class Key, class Info>
Four Dictionary<Key,Info>::AddEntry(Key key, Info info)
{
    Four i;

    // check duplicated key
    for(i = 0; i < size; i++)
    {
        if(useds[i] == SM_TRUE && keys[i] == key)
        {
            // find duplicated key
            return eDUPLICATED_KEY_DICTIONARY;
        }
    }

    // find empty entry
    for(i = 0; i < size; i++)
        if(useds[i] == SM_FALSE)
            break;

    if(i == size)
    {
        // there is no more room for new entry
        // do doubling the dictionary
        Key*            tempKeys;
        Info*           tempInfos;
        Boolean*        tempUseds;
        Four            i;

        tempKeys  = (Key*)pMemoryManager->Alloc(sizeof(Key) * size * 2);
        tempInfos = (Info*)pMemoryManager->Alloc(sizeof(Info) * size * 2);
        tempUseds = (Boolean*)pMemoryManager->Alloc(sizeof(Boolean) * size * 2);

        if(tempKeys == NULL || tempInfos == NULL || tempUseds == NULL)
            OOSQL_ERR(eMEMORYALLOCERR_OOSQL);

        for(i = 0;i < size; i++)
        {
            tempKeys[i]  = keys[i];
            tempInfos[i] = infos[i];
            tempUseds[i] = useds[i];

            tempUseds[i + size] = SM_FALSE;
        }

        pMemoryManager->Free(keys);
        pMemoryManager->Free(infos);
        pMemoryManager->Free(useds);

        keys  = tempKeys;
        infos = tempInfos;
        useds = tempUseds;

        i = size;
        size *= 2;
    }

    keys[i]  = key;
    infos[i] = info;
    useds[i] = SM_TRUE;

    return eNOERROR;
}

template <class Key, class Info>
Four Dictionary<Key,Info>::FindItem(Key key, Info& info)
{
    Four i;

    for(i = 0;i < size; i++)
    {
        if(useds[i] == SM_TRUE && keys[i] == key)
        {
            info = infos[i];
            return eNOERROR;
        }
    }
    
    return eNOTFOUND_DICTIONARY;
}

template <class Key, class Info>
Four Dictionary<Key,Info>::DeleteItem(Key key)
{
    Four i;

    for(i = 0;i < size; i++)
        if(useds[i] == SM_TRUE && keys[i] == key)
        {
            useds[i] = SM_FALSE;
            return eNOERROR;
        }

    OOSQL_ERR(eNOTFOUND_DICTIONARY);
}

#else   /* TEMPLATE_NOT_SUPPORTED */

#define Dictionary(Key,Info)        name3(Dictionary,Key,Info)
#define Dictionarydeclare(Key,Info)                                         \
    class Dictionary(Key,Info) : public OOSQL_MemoryManagedObject {         \
    public:                                                                 \
        Dictionary(Key,Info)();                                             \
        virtual ~Dictionary(Key,Info)();                                    \
                                                                            \
        Four AddEntry(Key key, Info info);                                  \
        Four FindItem(Key key, Info& info);                                 \
        Four DeleteItem(Key key);                                           \
                                                                            \
    private:                                                                \
        Key*            keys;                                               \
        Info*           infos;                                              \
        Boolean*        useds;                                              \
        Four            size;                                               \
};                                                                         

#define Dictionaryimplement(Key,Info)                                       \
Dictionary(Key,Info)::Dictionary(Key,Info)()                                \
{                                                                           \
    Four i;                                                                 \
                                                                            \
    size = DIC_INITIAL_ARRAY_SIZE;											\
																			\
    keys  = (Key*)pMemoryManager->Alloc(sizeof(Key) * size);				\
    infos = (Info*)pMemoryManager->Alloc(sizeof(Info) * size);				\
    useds = (Boolean*)pMemoryManager->Alloc(sizeof(Boolean) * size);		\
																			\
    if(keys == NULL || infos == NULL || useds == NULL)						\
        OOSQL_ERR_EXIT(eMEMORYALLOCERR_OOSQL);									\
                                                                            \
    for(i = 0;i < size;i++)                                                 \
        useds[i] = SM_FALSE;                                                   \
}                                                                           \
                                                                            \
Dictionary(Key,Info)::~Dictionary(Key,Info)()                               \
{                                                                           \
    if(keys)                                                                \
        pMemoryManager->Free(keys);                                         \
    if(infos)                                                               \
        pMemoryManager->Free(infos);                                        \
    if(useds)                                                               \
        pMemoryManager->Free(useds);                                        \
}                                                                           \
                                                                            \
Four Dictionary(Key,Info)::AddEntry(Key key, Info info)                     \
{                                                                           \
    Four i;                                                                 \
                                                                            \
    for(i = 0; i < size; i++)                                               \
    {                                                                       \
        if(useds[i] == SM_TRUE && keys[i] == key)                              \
        {                                                                   \
            return eDUPLICATED_KEY_DICTIONARY;                              \
        }                                                                   \
    }                                                                       \
                                                                            \
    for(i = 0; i < size; i++)                                               \
        if(useds[i] == SM_FALSE)                                               \
            break;                                                          \
                                                                            \
    if(i == size)                                                           \
    {                                                                       \
        Key*            tempKeys;                                           \
        Info*           tempInfos;                                          \
        Boolean*        tempUseds;                                          \
        Four            i;                                                  \
                                                                            \
        tempKeys  = (Key*)pMemoryManager->Alloc(sizeof(Key) * size * 2);    \
        tempInfos = (Info*)pMemoryManager->Alloc(sizeof(Info) * size * 2);  \
        tempUseds = (Boolean*)pMemoryManager->Alloc(sizeof(Boolean) * size * 2);\
        if(tempKeys == NULL || tempInfos == NULL || tempUseds == NULL)      \
            OOSQL_ERR(eMEMORYALLOCERR_OOSQL);                                   \
                                                                            \
        for(i = 0;i < size; i++)                                            \
        {                                                                   \
            tempKeys[i]  = keys[i];                                         \
            tempInfos[i] = infos[i];                                        \
            tempUseds[i] = useds[i];                                        \
                                                                            \
            tempUseds[i + size] = SM_FALSE;                                    \
        }                                                                   \
                                                                            \
        pMemoryManager->Free(keys);                                         \
        pMemoryManager->Free(infos);                                        \
        pMemoryManager->Free(useds);                                        \
                                                                            \
        keys  = tempKeys;                                                   \
        infos = tempInfos;                                                  \
        useds = tempUseds;                                                  \
                                                                            \
        i = size;                                                           \
        size *= 2;                                                          \
    }                                                                       \
                                                                            \
    keys[i]  = key;                                                         \
    infos[i] = info;                                                        \
    useds[i] = SM_TRUE;                                                        \
                                                                            \
    return eNOERROR;                                                        \
}                                                                           \
                                                                            \
Four Dictionary(Key,Info)::FindItem(Key key, Info& info)                    \
{                                                                           \
    Four i;                                                                 \
                                                                            \
    for(i = 0;i < size; i++)                                                \
    {                                                                       \
        if(useds[i] == SM_TRUE && keys[i] == key)                              \
        {                                                                   \
            info = infos[i];                                                \
            return eNOERROR;                                                \
        }                                                                   \
    }                                                                       \
                                                                            \
    return eNOTFOUND_DICTIONARY;                                            \
}                                                                           \
                                                                            \
Four Dictionary(Key,Info)::DeleteItem(Key key)                              \
{                                                                           \
    Four i;                                                                 \
                                                                            \
    for(i = 0;i < size; i++)                                                \
        if(useds[i] == SM_TRUE && keys[i] == key)                              \
        {                                                                   \
            useds[i] = SM_FALSE;                                               \
            return eNOERROR;                                                \
        }                                                                   \
                                                                            \
    OOSQL_ERR(eNOTFOUND_DICTIONARY);                                          \
}
#endif  /* TEMPLATE_NOT_SUPPORTED */

#endif  /* __OQL_DICTIONARY_H__ */
