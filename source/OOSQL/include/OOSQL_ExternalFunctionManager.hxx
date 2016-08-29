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

#ifndef _OOSQL_EXTERNALFUNCTIONMANAGER_H_
#define _OOSQL_EXTERNALFUNCTIONMANAGER_H_

#include "OOSQL_Common.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_String.hxx"
#include "OOSQL_Array.hxx"
#include "OOSQL_StorageManager.hxx"
#include "OQL_Plan_Struct.hxx"

class OOSQL_ExternalFunctionParameterInfo 
{
public:
	char				m_argumentName[65];
	TypeID				m_argumentType;
	char				m_argumentTypeName[65];
	Four                m_argumentTypeLength;
	ParameterMode		m_parameterMode;
	Boolean				m_asLocator;

private:
};

class OOSQL_ExternalFunctionInfo
{
public:
	Four            m_volID;
	char			m_functionSchema[65];
	char			m_functionName[65];
	char			m_specificName[65];
	char			m_definer[65];
	Four            m_funcID;
	Four            m_procID;
	TypeID			m_returnType;
	Boolean			m_returnAsLocator;
	char            m_origin;
	char            m_type;
	Four			m_paramCount;
	char			m_paramSignature[65];
	Boolean			m_deterministic;
	Boolean			m_sideEffects;
	Boolean			m_fenced;
	Boolean			m_nullCall;
	Boolean			m_castFunction;
	Boolean         m_assignFunction;
	Boolean			m_scratchPad;
	Boolean			m_finalCall;
	Boolean			m_allowParallel;
	Boolean			m_dbInfo;
	short           m_resultCols;
	char			m_language[9];
	char			m_implementation[1000];
	char			m_parameterStyle[9];
	char			m_sourceSchema[65];
	char			m_sourceSpecific[65];

	OOSQL_ExternalFunctionParameterInfo m_parameters[32];
};

class OOSQL_ExternalFunctionManager : public OOSQL_MemoryManagedObject {
public:
	OOSQL_ExternalFunctionManager(OOSQL_SystemHandle* oosqlSystemHandle);
	virtual ~OOSQL_ExternalFunctionManager();
	
	Four Find(Four volID, char* functionName, OOSQL_TCArray<OOSQL_ExternalFunctionInfo>& functionInfos);
	Four Find(Four volID, Four funcID, OOSQL_ExternalFunctionInfo& functionInfo);
	Four Invalidate(Four volID, char* functionName);

private:
	OOSQL_TCArray<OOSQL_ExternalFunctionInfo>	m_functionInfos;
	OOSQL_SystemHandle*							m_oosqlSystemHandle;

};

#endif // _OOSQL_EXTERNALFUNCTIONMANAGER_H_

