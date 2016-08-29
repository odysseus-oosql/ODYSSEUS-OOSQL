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

#ifndef __OQL_AST_H__
#define __OQL_AST_H__

#include <stdio.h>
#include "OOSQL_Common.h"
#include "OOSQL_Error.h"

extern "C" {
#include "OQL.yacc.h"
#include "OQL_AST.h"
}

#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

class OQL_AST : public OOSQL_MemoryManagedObject {
public:
    OQL_AST(long*     intPool,         int intSize,
            float*   realPool,        int realSize,
            char*    stringPool,      int stringSize,
            int*     stringIndexPool, int stringIndexSize,
            ASTNode* astNodePool,     int astNodePoolSize);
    virtual ~OQL_AST();

public:
    long      getInt(Four index);
    float    getReal(Four index);
    char     getString(Four index);
    int      getStringIndex(Four index);
    ASTNode& getASTNode(Four index);

    int      getIntPoolSize();
    int      getRealPoolSize();
    int      getStringPoolSize();
    int      getStringIndexPoolSize();
	int		 getAstNodePoolSize();

private:
    long*     m_intPool;
    int      m_intPoolSize;

    float*   m_realPool;
    int      m_realPoolSize;

    char*    m_stringPool;
    int      m_stringPoolSize;

    int*     m_stringIndexPool;
    int      m_stringIndexPoolSize;

    ASTNode* m_astNodePool;
	int      m_astNodePoolSize;
};


inline long OQL_AST::getInt(Four index)
{
#if DEBUG
    if(index >= m_intPoolSize)
        OOSQL_ERR_EXIT(eBOUNDARY_OVERFLOW_OQL);
#endif
    return m_intPool[index];
}

inline float OQL_AST::getReal(Four index)
{
#if DEBUG
    if(index >= m_realPoolSize)
        OOSQL_ERR_EXIT(eBOUNDARY_OVERFLOW_OQL);
#endif
    return m_realPool[index];
}

inline char OQL_AST::getString(Four index)
{
#if DEBUG
    if(index >= m_stringPoolSize)
        OOSQL_ERR_EXIT(eBOUNDARY_OVERFLOW_OQL);
#endif
    return m_stringPool[index];
}

inline int OQL_AST::getStringIndex(Four index)
{
#if DEBUG
    if(index >= m_stringIndexPoolSize)
        OOSQL_ERR_EXIT(eBOUNDARY_OVERFLOW_OQL);
#endif
    return m_stringIndexPool[index];
}

inline ASTNode& OQL_AST::getASTNode(Four index)
{
    return m_astNodePool[index];
}

inline int OQL_AST::getIntPoolSize()            { return m_intPoolSize; }
inline int OQL_AST::getRealPoolSize()           { return m_realPoolSize; }
inline int OQL_AST::getStringPoolSize()         { return m_stringPoolSize; }
inline int OQL_AST::getStringIndexPoolSize()    { return m_stringIndexPoolSize; }
inline int OQL_AST::getAstNodePoolSize()		{ return m_astNodePoolSize; }

#endif /* __OQL_AST_H__ */




