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

#ifndef SLIMDOWN_TEXTIR

/*
 * Module: lom_dlfcn.c
 *
 * Description:
 *  dynamic linking management module
 *
 * Imports:
 *  None
 *
 * Exports:
 *
 */
#include "LOM_Internal.h"
#include "LOM.h"
#include <dlfcn.h>

void* lom_dlopen(LOM_Handle *handle, char* moduleName, int mode)
{
	Four i, j, k;
	Four e;

	/* find dynamic library in the LOM_DLFCNTABLE */
	for(i = 0; i < LOM_DLFCNTABLE_ENTRIES(handle); i++) 
	{
#ifdef USE_RPC
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle && LOM_DLFCNTABLE(handle)[i].handle.dllClient == NULL)
#else
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle)
#endif
		{
			if(!strncmp(LOM_DLFCNTABLE(handle)[i].moduleName, moduleName, LOM_MAXPATHLENGTH))
			{
				/* found it */
				return LOM_DLFCNTABLE(handle)[i].handle.dllHandle;
			}
		}
	}

	/* not found, then open it and return */
	/* find free entry */
	for(i = 0; i < LOM_DLFCNTABLE_ENTRIES(handle); i++) 
#ifdef USE_RPC
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle == NULL && LOM_DLFCNTABLE(handle)[i].handle.dllClient == NULL)
#else
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle == NULL)
#endif
			break;

	if(i == LOM_DLFCNTABLE_ENTRIES(handle))
	{
		/* doubling table */
		e = LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_DLFCNTABLE_PTR(handle), sizeof(lom_DllfcnTableEntry));
		if(e < eNOERROR) return NULL;

		/* Initialize the newly allocated entries */
		for (j = i; j < LOM_DLFCNTABLE_ENTRIES(handle); j++) 
		{
			LOM_DLFCNTABLE(handle)[j].handle.dllHandle = NULL;
#ifdef USE_RPC	
			LOM_DLFCNTABLE(handle)[j].handle.dllClient = NULL;
#endif

			for(k = 0; k < LOM_MAXDLLFUNCPTRS; k++)
			{
				LOM_DLFCNTABLE(handle)[j].func[k].dllFunc   = NULL;
#ifdef USE_RPC
				LOM_DLFCNTABLE(handle)[j].func[k].dllClient = NULL;
#endif
			}
		}
	}

	LOM_DLFCNTABLE(handle)[i].handle.dllHandle = dlopen(moduleName, mode);
	strncpy(LOM_DLFCNTABLE(handle)[i].moduleName, moduleName, LOM_MAXPATHLENGTH);

	return LOM_DLFCNTABLE(handle)[i].handle.dllHandle;
}

int lom_dlclose(LOM_Handle *handle, void* dllHandle)
{
	/* do nothing, all dynamic library will be freed when system shutdown by LOM_FlushAll call */
	return eNOERROR;
}

void* lom_dlsym(LOM_Handle *handle, void* dllHandle, char *name)
{
	Four i, j;

	for (i = 0; i < LOM_DLFCNTABLE_ENTRIES(handle); i++) 
	{
#ifdef USE_RPC
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle == dllHandle && LOM_DLFCNTABLE(handle)[i].handle.dllClient == NULL)
#else
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle == dllHandle)
#endif
		{
			for(j = 0; j < LOM_MAXDLLFUNCPTRS; j++)
			{
				if(LOM_DLFCNTABLE(handle)[i].func[j].dllFunc != NULL && 
				   !strcmp(LOM_DLFCNTABLE(handle)[i].func[j].funcName, name))
				{
					return &LOM_DLFCNTABLE(handle)[i].func[j];
				}
			}

			for(j = 0; j < LOM_MAXDLLFUNCPTRS; j++)
			{
				if(LOM_DLFCNTABLE(handle)[i].func[j].dllFunc == NULL)
				{
					strcpy(LOM_DLFCNTABLE(handle)[i].func[j].funcName, name);
					LOM_DLFCNTABLE(handle)[i].func[j].dllFunc   = dlsym(dllHandle, name);
#ifdef USE_RPC 	
					LOM_DLFCNTABLE(handle)[i].func[j].dllClient = NULL;
#endif
					if(LOM_DLFCNTABLE(handle)[i].func[j].dllFunc == NULL)
						return NULL;
					else
						return &LOM_DLFCNTABLE(handle)[i].func[j];
				}
			}
		}
	}

	return NULL;
}

char* lom_dlerror(LOM_Handle *handle)
{
	return dlerror();
}

#else /* SLIMDOWN_TEXTIR */

#include "LOM_Internal.h"
#include "LOM.h"
#include <dlfcn.h>

void* lom_dlopen(LOM_Handle *handle, char* moduleName, int mode)
{
	    return NULL;
}


int lom_dlclose(LOM_Handle *handle, void* dllHandle)
{
	    return eTEXTIR_NOTENABLED_LOM;
}


void* lom_dlsym(LOM_Handle *handle, void* dllHandle, char *name)
{
	    return NULL;
}


char* lom_dlerror(LOM_Handle *handle)
{
	    return NULL;
}

#endif /* SLIMDOWN_TEXTIR */

