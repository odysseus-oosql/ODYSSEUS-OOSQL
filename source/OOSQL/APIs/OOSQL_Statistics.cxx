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

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_GetNumTextObjectsInVolume(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				volId,			// IN  
	Four*				numObjects		// OUT 
)
{
	OOSQL_Handle	handle;
	char			className[MAXCLASSNAME + 1];
	Four			retLength;
	Four			numObjectsInClass;
	Four			numObjectsInVolume;
	Four			e;
	Four			classId;
	
	e = OOSQL_AllocHandle(systemHandle, volId, &handle); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = OOSQL_Prepare(systemHandle, handle, "select className from lomSysClasses", NULL);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = OOSQL_Execute(systemHandle, handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	numObjectsInVolume = 0;
	while((e = OOSQL_Next(systemHandle, handle)) != EOS)
	{
		if(e < 0) OOSQL_ERROR(systemHandle, e);

		e = OOSQL_GetData(systemHandle, handle, 0, 0, className, sizeof(className) - 1, &retLength);
		if(e < 0) OOSQL_ERROR(systemHandle, e);
		if(retLength >= 0)
		{
			className[retLength] = '\0';

			e = LOM_GetClassID(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volId, className, &classId);
			if(e < 0) OOSQL_ERROR(systemHandle, e);

			if(classId >= 1000)
			{
				e = Catalog_IsLRDS_RelationBasedClass(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volId, classId);
				if(e < 0) OOSQL_ERROR(systemHandle, e);
			}
			else
				e = SM_TRUE;

			if(e == SM_FALSE && classId >= 1000)
			{
				e = LOM_IsTextClass(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volId, className);
				if(e < 0) OOSQL_ERROR(systemHandle, e);

				if(e == SM_TRUE)
				{
					e = LOM_GetNumObjectsInClass(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volId, className, &numObjectsInClass);
					if(e < 0) OOSQL_ERROR(systemHandle, e);

					if(numObjectsInClass > 0)
						numObjectsInVolume += numObjectsInClass;
				}
			}
		}
	}

	e = OOSQL_FreeHandle(systemHandle, handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	*numObjects = numObjectsInVolume;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_GetNumObjectsInVolume(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				volId,			// IN  
	Four*				numObjects		// OUT 
)
{
	OOSQL_Handle	handle;
	char			className[MAXCLASSNAME + 1];
	Four			retLength;
	Four			numObjectsInClass;
	Four			numObjectsInVolume;
	Four			e;

	e = OOSQL_AllocHandle(systemHandle, volId, &handle); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = OOSQL_Prepare(systemHandle, handle, "select className from lomSysClasses", NULL);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = OOSQL_Execute(systemHandle, handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	numObjectsInVolume = 0;
	while((e = OOSQL_Next(systemHandle, handle)) != EOS)
	{
		if(e < 0) OOSQL_ERROR(systemHandle, e);

		e = OOSQL_GetData(systemHandle, handle, 0, 0, className, sizeof(className) - 1, &retLength);
		if(e < 0) OOSQL_ERROR(systemHandle, e);
		if(retLength >= 0)
		{
			className[retLength] = '\0';

			e = LOM_GetNumObjectsInClass(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volId, className, &numObjectsInClass);
			if(e < 0) OOSQL_ERROR(systemHandle, e);

			if(numObjectsInClass > 0)	
				numObjectsInVolume += numObjectsInClass;
		}
	}

	e = OOSQL_FreeHandle(systemHandle, handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	*numObjects = numObjectsInVolume;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_GetNumObjectsInClass(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				volId,			// IN  
	char*				className,		// IN  
	Four*				numObjects		// OUT
)
{
	Four			e;

	e = LOM_GetNumObjectsInClass(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volId, className, numObjects);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}
