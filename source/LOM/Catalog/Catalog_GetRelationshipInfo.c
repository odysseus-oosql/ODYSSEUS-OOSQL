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

#include <stdio.h>
#include <string.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog_Internal.h"
#include "Catalog.h"

static Four catalog_AdjustAttrNum(LOM_Handle* handle, Four volId,  Four classId,  Two attrNum, Four subClassId, Two* subClassAttrNum);
static Four catalog_AddRelationship(LOM_Handle* handle, Four volId, Four fromClassId, Two fromAttrNum, Four toClassId, Two toAttrNum, One direction, One cardinality, Four relationshipId, char* relationshipName);

Four Catalog_GetRelationshipInfo(
    LOM_Handle* handle,
    Four		volId,
    Four		fromClassId,
    Four		toClassId,
    Four		relationshipId,
    Four*		relationshipInfoTblIndex
)
{
	Boolean found;
	Four	i, v;

	/* Check whether the volume is already mounted. */
    for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId)
		{
			break;
		}


	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	found = SM_FALSE;
	for (i = 0; i < CATALOG_GET_RELATIONSHIPINFOTBL_SIZE(handle, v); i++) 
	{
        if(CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].inUse == SM_TRUE)
        {
            if(CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].fromClassId    == fromClassId &&
               CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].toClassId      == toClassId &&
               CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].relationshipID == relationshipId) 
            {
                found = SM_TRUE;
                break;
            }
        }
	}
	
	if(!found) 
        *relationshipInfoTblIndex = -1;
	else 
        *relationshipInfoTblIndex = i;
        
	return eNOERROR;
}

Four Catalog_Relationship_CreateClass(
	LOM_Handle* handle, 
    Four		volId,			/* IN volume in which the relation will be placed */
    Four		classId,		/* IN class id has been created */
    Four		nSuperclasss,	/* IN number of superclasss */
    char		(*superclassList)[MAXCLASSNAME] /* IN names of superclasss */
)
{
	Four							classInfo, superClassInfo;
	Four							superClassId;
	catalog_SysClassesOverlay*      pClass;
	catalog_SysClassesOverlay*      pSuperClass;
	catalog_SysRelationshipOverlay* pRelationship;
	catalog_SysRelationshipOverlay* pOldRelationship;
	catalog_SysRelationshipOverlay* pSuperClassRelationship;
	Four						    relationshipInfo;
	Four							oldRelationshipInfo;
	Four							superClassRelationshipInfo;
	Four							nRelationships;
	Four							nOldRelationships;
	Four							nSuperClassRelationships;
	Four							i, j;
	Four							e;
	Four							mv;
	Two								fromAttrNum;
	
	e = Catalog_GetMountTableInfo(handle, volId, &mv);
	if(e < 0) CATALOG_ERROR(handle, e);

	e = catalog_GetClassInfo(handle, volId, classId, &classInfo);
	if(e < 0) CATALOG_ERROR(handle, e);
	
	if(classInfo == -1)     
	    return eNOERROR;    

	pClass = &CATALOG_GET_CLASSINFOTBL(handle, mv)[classInfo];

    for(i = 0; i < nSuperclasss; i++)
    {
        e = LOM_GetClassID(handle, volId, superclassList[i], &superClassId);
        if(e < 0) CATALOG_ERROR(handle, e);

		e = Catalog_GetClassInfo(handle, volId, superClassId, &superClassInfo);
		if(e < 0) CATALOG_ERROR(handle, e);

		pSuperClass                = &CATALOG_GET_CLASSINFOTBL(handle, mv)[superClassInfo];
		superClassRelationshipInfo = CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pSuperClass);
		nSuperClassRelationships   = CATALOG_GET_RELATIONSHIPNUM(pSuperClass);

		oldRelationshipInfo = CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass);
		nOldRelationships   = CATALOG_GET_RELATIONSHIPNUM(pClass);

		CATALOG_GET_RELATIONSHIPNUM(pClass) += (Two)nSuperClassRelationships;

		e = catalog_getFreeEntries(handle, mv, CATALOG_RELATIONSHIPINFOTBL,
								   CATALOG_GET_RELATIONSHIPNUM(pClass),
								   &(CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass)));
		if(e < 0) CATALOG_ERROR(handle, eSHORTOFMEMORY_CATALOG);

		relationshipInfo = CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass);
		nRelationships   = CATALOG_GET_RELATIONSHIPNUM(pClass);

		for(j = 0; j < nOldRelationships; j++)
		{
			pOldRelationship = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[oldRelationshipInfo + j];
			pRelationship    = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo + j];

			memcpy(pRelationship, pOldRelationship, sizeof(catalog_SysRelationshipOverlay));
		}

		for(j = 0; j < nSuperClassRelationships; j++)
		{
			pSuperClassRelationship = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[superClassRelationshipInfo + j];
			pRelationship           = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo + nOldRelationships + j];

			memcpy(pRelationship, pSuperClassRelationship, sizeof(catalog_SysRelationshipOverlay));
			
			/* adjust from-attr-num caused by multiple inheritance */
			fromAttrNum = CATALOG_GET_RELATIONSHIPFROMATTRNUM(pSuperClassRelationship);
			if(nSuperclasss > 1)
			{
				e = catalog_AdjustAttrNum(handle, volId, superClassId, fromAttrNum, classId, &fromAttrNum);
				if(e < 0) CATALOG_ERROR(handle, e);
			}

			CATALOG_GET_RELATIONSHIPFROMCLASSID(pRelationship) = classId;
			CATALOG_GET_RELATIONSHIPFROMATTRNUM(pRelationship) = fromAttrNum;
		}

		for(j = 0; j < nOldRelationships; j++)
			CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[oldRelationshipInfo + j].inUse = 0;
	}

	return eNOERROR;
}

Four Catalog_Relationship_DestroyClass(
	LOM_Handle* handle,
    Four		volId,              /* IN volume in which the relation will be placed */
    Four		classId             /* IN class id has been created */
)
{
	catalog_SysClassesOverlay*      pClass;
	catalog_SysRelationshipOverlay* pRelationship;
	catalog_SysRelationshipOverlay* pOldRelationship;
	Four						    relationshipInfo, oldRelationshipInfo;
	Four							nRelationships, nOldRelationships;
	Four							nRemovedRelationships;
	Four							i, j, k;
	Four							e;
	Four							mv;
	
	e = Catalog_GetMountTableInfo(handle, volId, &mv);
	if(e < 0) CATALOG_ERROR(handle, e);

	for(i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, mv); i++)
	{
		pClass = &CATALOG_GET_CLASSINFOTBL(handle, mv)[i];
		if(!pClass->inUse) continue; 

		relationshipInfo = CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass);

		nRelationships = CATALOG_GET_RELATIONSHIPNUM(pClass);
		for(nRemovedRelationships = 0, j = 0; j < nRelationships; j++)
		{
			pRelationship = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo + j];
			if(CATALOG_GET_RELATIONSHIPTOCLASSID(pRelationship) == classId)
				nRemovedRelationships ++;
		}

		if(nRemovedRelationships == 0)
			continue;

		oldRelationshipInfo = relationshipInfo;
		nOldRelationships   = nRelationships;

		CATALOG_GET_RELATIONSHIPNUM(pClass) -= (Two)nRemovedRelationships;
		
		if(CATALOG_GET_RELATIONSHIPNUM(pClass) > 0)
		{
			e = catalog_getFreeEntries(handle, mv, CATALOG_RELATIONSHIPINFOTBL,
									   CATALOG_GET_RELATIONSHIPNUM(pClass),
									   &(CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass)));
			if(e < 0) CATALOG_ERROR(handle, eSHORTOFMEMORY_CATALOG);

			relationshipInfo = CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass);
			nRelationships = CATALOG_GET_RELATIONSHIPNUM(pClass);

			for(k = 0, j = 0; j < nOldRelationships; j++)
			{
				pOldRelationship = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[oldRelationshipInfo + j];
				pRelationship    = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo + k];

				if(CATALOG_GET_RELATIONSHIPTOCLASSID(pOldRelationship) != classId)
				{
					memcpy(pRelationship, pOldRelationship, sizeof(catalog_SysRelationshipOverlay));
					k ++;
				}
			}
		}

		for(j = 0; j < nOldRelationships; j++)
			CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[oldRelationshipInfo + j].inUse = 0;
	}

	return eNOERROR;
}

Four Catalog_Relationship_CreateRelationship(
	LOM_Handle* handle,
    Four		volId,				/* IN volume ID */
    char*		relationshipName,	/* IN relationship name */
    Four		fromClassId,		/* IN from class ID */
    Two			fromAttrNum,		/* IN from attribute number */
    Four		toClassId,			/* IN to calss ID */
    Two			toAttrNum,			/* IN to attribute number */
    One			cardinality,		/* IN cardinality */
    One			direction,			/* IN uni or bi-directional */
    Four		relationshipId		/* IN relationship id */
)
{
    Four e;
    Four i, j;
    Four nFromSubClasses;
    Four iFromSubClass;
    Four nToSubClasses;
    Four iToSubClass;
    Four fromSubClassIdsBuf[100];
    Two  fromSubClassAttrNosBuf[100];
    Four toSubClassIdsBuf[100];
    Two  toSubClassAttrNosBuf[100];
	One  reverseCardinality;

	/* generate reverse cardinality */
    switch(cardinality)
    {
	case LOM_RELATIONSHIP_ONE_TO_ONE:
	case LOM_RELATIONSHIP_MANY_TO_MANY:
		reverseCardinality = cardinality;
		break;
	case LOM_RELATIONSHIP_MANY_TO_ONE:
		reverseCardinality = LOM_RELATIONSHIP_ONE_TO_MANY;
		break;
	case LOM_RELATIONSHIP_ONE_TO_MANY:
		reverseCardinality = LOM_RELATIONSHIP_MANY_TO_ONE;
		break;
    }

    /* create relationship between (fromClass, fromSubClasses) and (toClass, toSubClasses) */
    iFromSubClass = 0;
    while(1)
    {
		if(iFromSubClass == -1)
			break;

        if(iFromSubClass == 0)
        {
            nFromSubClasses = lom_GetSubClasses(handle, volId, fromClassId,
                                                    iFromSubClass,
                                                    sizeof(fromSubClassIdsBuf) / sizeof(Four),
                                                    &fromSubClassIdsBuf[1]);
            if(nFromSubClasses < 0) CATALOG_ERROR(handle, nFromSubClasses);
			if(nFromSubClasses == 0) 
				iFromSubClass = -1;		/* in next loop, exit loop */

            iFromSubClass   += nFromSubClasses;
            nFromSubClasses ++;

            /* adjust fromattrno */
            for(i = 0; i < (nFromSubClasses - 1); i++)
            {
                /* adjust attr-num caused by multiple inheritance */
                e = catalog_AdjustAttrNum(handle, volId, fromClassId, fromAttrNum, 
                                          fromSubClassIdsBuf[i + 1], &fromSubClassAttrNosBuf[i + 1]);
                if(e < 0) CATALOG_ERROR(handle, e);
            }

            fromSubClassIdsBuf[0]     = fromClassId;
            fromSubClassAttrNosBuf[0] = fromAttrNum;
        }
        else
        {
            nFromSubClasses = lom_GetSubClasses(handle, volId, fromClassId,
                                                    iFromSubClass,
                                                    sizeof(fromSubClassIdsBuf) / sizeof(Four),
                                                    fromSubClassIdsBuf);
            if(nFromSubClasses < 0) CATALOG_ERROR(handle, nFromSubClasses);
            if(nFromSubClasses == 0)
                break;

            iFromSubClass += nFromSubClasses;

            /* adjust fromattrno */
            for(i = 0; i < nFromSubClasses; i++)
            {
                /* adjust attr-num caused by multiple inheritance */
                e = catalog_AdjustAttrNum(handle, volId, fromClassId, fromAttrNum, 
                                          fromSubClassIdsBuf[i], &fromSubClassAttrNosBuf[i]);
                if(e < 0) CATALOG_ERROR(handle, e);
            }
        }
        
        iToSubClass = 0;
        while(1)
        {
			if(iToSubClass == -1)
				break;
            if(iToSubClass == 0)
            {
                nToSubClasses = lom_GetSubClasses(handle, volId, toClassId,
                                                      iToSubClass,
                                                      sizeof(toSubClassIdsBuf) / sizeof(Four),
                                                      &toSubClassIdsBuf[1]);
                if(nToSubClasses < 0) CATALOG_ERROR(handle, nToSubClasses);
				if(nToSubClasses == 0) 
					iToSubClass = -1;		/* in next loop, exit loop */

                iToSubClass += nToSubClasses;
                nToSubClasses ++;

                /* adjust fromattrno */
                for(i = 0; i < (nToSubClasses - 1); i++)
                {
                    /* adjust attr-num caused by multiple inheritance */
                    e = catalog_AdjustAttrNum(handle, volId, toClassId, toAttrNum, 
                                              toSubClassIdsBuf[i + 1], &toSubClassAttrNosBuf[i + 1]);
                    if(e < 0) CATALOG_ERROR(handle, e);
                }

                toSubClassIdsBuf[0]     = toClassId;
                toSubClassAttrNosBuf[0] = toAttrNum;
            }
            else
            {
                nToSubClasses = lom_GetSubClasses(handle, volId, toClassId,
                                                     iToSubClass,
                                                     sizeof(toSubClassIdsBuf) / sizeof(Four),
                                                     toSubClassIdsBuf);
                if(nToSubClasses < 0) CATALOG_ERROR(handle, nToSubClasses);
                if(nToSubClasses == 0)
                    break;

                iToSubClass += nToSubClasses;

                /* adjust toattrno */
                for(i = 0; i < nToSubClasses; i++)
                {
                    /* adjust attr-num caused by multiple inheritance */
                    e = catalog_AdjustAttrNum(handle, volId, toClassId, toAttrNum, 
                                              toSubClassIdsBuf[i], &toSubClassAttrNosBuf[i]);
                    if(e < 0) CATALOG_ERROR(handle, e);
                }
            }

            for(i = 0; i < nFromSubClasses; i++)
            {
                for(j = 0; j < nToSubClasses; j++)
                {
                    e = catalog_AddRelationship(handle, volId,
                                                fromSubClassIdsBuf[i],
                                                fromSubClassAttrNosBuf[i],
                                                toSubClassIdsBuf[j],
                                                toSubClassAttrNosBuf[j],
                                                direction, cardinality, 
                                                relationshipId,
                                                relationshipName);
                    if(e < 0) CATALOG_ERROR(handle, e);

                    if(direction == LOM_RELATIONSHIP_BIDIRECTIONAL)
                    {
                        /* create reverse direction relationship */
                        e = catalog_AddRelationship(handle, volId,
                                                    toSubClassIdsBuf[j],
                                                    toSubClassAttrNosBuf[j],
                                                    fromSubClassIdsBuf[i],
                                                    fromSubClassAttrNosBuf[i],
                                                    direction, reverseCardinality, 
                                                    relationshipId,
                                                    relationshipName);
                        if(e < 0) CATALOG_ERROR(handle, e);
                    }
                }
            }
        }
    }

    return eNOERROR;
}

Four Catalog_Relationship_DestroyRelationship(
	LOM_Handle* handle,
    Four		volId,               /* volume id */
    char*		relationshipName)    /* relationship name */
{
	catalog_SysClassesOverlay*      pClass;
	catalog_SysRelationshipOverlay* pRelationship;
	catalog_SysRelationshipOverlay* pOldRelationship;
	Four						    relationshipInfo, oldRelationshipInfo;
	Four							nRelationships, nOldRelationships;
	Four							nRemovedRelationships;
	Four							i, j, k;
	Four							e;
	Four							mv;
	
	e = Catalog_GetMountTableInfo(handle, volId, &mv);
	if(e < 0) CATALOG_ERROR(handle, e);

	for(i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, mv); i++)
	{
		pClass = &CATALOG_GET_CLASSINFOTBL(handle, mv)[i];
		relationshipInfo = CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass);

		nRelationships = CATALOG_GET_RELATIONSHIPNUM(pClass);
		for(nRemovedRelationships = 0, j = 0; j < nRelationships; j++)
		{
			pRelationship = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo + j];
			if(!strcmp(CATALOG_GET_RELATIONSHIPRELATIONSHIPNAME(pRelationship), relationshipName))
				nRemovedRelationships ++;
		}

		if(nRemovedRelationships == 0)
			continue;

		oldRelationshipInfo = relationshipInfo;
		nOldRelationships   = nRelationships;

		CATALOG_GET_RELATIONSHIPNUM(pClass) -= (Two)nRemovedRelationships;

		if(CATALOG_GET_RELATIONSHIPNUM(pClass) > 0)
		{
			e = catalog_getFreeEntries(handle, mv, CATALOG_RELATIONSHIPINFOTBL,
									   CATALOG_GET_RELATIONSHIPNUM(pClass),
									   &(CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass)));
			if(e < 0) CATALOG_ERROR(handle, eSHORTOFMEMORY_CATALOG);

			relationshipInfo = CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass);
			nRelationships = CATALOG_GET_RELATIONSHIPNUM(pClass);

			for(k = 0, j = 0; j < nOldRelationships; j++)
			{
				pOldRelationship = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[oldRelationshipInfo + j];
				pRelationship    = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo + k];

				if(strcmp(CATALOG_GET_RELATIONSHIPRELATIONSHIPNAME(pOldRelationship), relationshipName))
				{
					memcpy(pRelationship, pOldRelationship, sizeof(catalog_SysRelationshipOverlay));
					k ++;
				}
			}
		}

		for(j = 0; j < nOldRelationships; j++)
			CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[oldRelationshipInfo + j].inUse = 0;
	}

	return eNOERROR;
}

static Four catalog_AdjustAttrNum(
	LOM_Handle*	handle, 
    Four        volId,                  /* mount volume index */
    Four        classId,                /* IN  : class */
    Two         attrNum,                /* IN  : class's attr num */
    Four        subClassId,             /* IN  : sub class id */
    Two*        subClassAttrNum)        /* OUT : adjusted subclass's attr num */
{
	return lom_AdjustAttrNum(handle, volId, classId, attrNum, subClassId, subClassAttrNum);
}

static Four catalog_AddRelationship(
	LOM_Handle*		handle, 
    Four            volId,               /* IN vol id */
    Four            fromClassId,         /* IN */
    Two             fromAttrNum,         /* IN */
    Four            toClassId,           /* IN */ 
    Two             toAttrNum,           /* IN */
    One             direction,           /* IN */
    One             cardinality,         /* IN */
    Four            relationshipId,      /* IN */
    char*           relationshipName)    /* IN */
{
	Four							classInfo;
	catalog_SysClassesOverlay*      pClass;
	catalog_SysRelationshipOverlay* pRelationship;
	catalog_SysRelationshipOverlay* pOldRelationship;
	Four						    relationshipInfo, oldRelationshipInfo;
	Four							nRelationships, nOldRelationships;
	Four							i;
	Four							e;
	Four							mv;
	
	e = Catalog_GetMountTableInfo(handle, volId, &mv);
	if(e < 0) CATALOG_ERROR(handle, e);

	e = catalog_GetClassInfo(handle, volId, fromClassId, &classInfo);
	if(e < 0) CATALOG_ERROR(handle, e);

	if(classInfo == -1)		
		return eNOERROR;
	
	pClass              = &CATALOG_GET_CLASSINFOTBL(handle, mv)[classInfo];
	oldRelationshipInfo = CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass);
	nOldRelationships   = CATALOG_GET_RELATIONSHIPNUM(pClass);

	CATALOG_GET_RELATIONSHIPNUM(pClass) ++;

    e = catalog_getFreeEntries(handle, mv, CATALOG_RELATIONSHIPINFOTBL,
                               CATALOG_GET_RELATIONSHIPNUM(pClass),
                               &(CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass)));
    if(e < 0) CATALOG_ERROR(handle, eSHORTOFMEMORY_CATALOG);

	relationshipInfo = CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass);
	nRelationships   = CATALOG_GET_RELATIONSHIPNUM(pClass);

	for(i = 0; i < nOldRelationships; i++)
	{
		pOldRelationship = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[oldRelationshipInfo + i];
		pRelationship    = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo + i];

		memcpy(pRelationship, pOldRelationship, sizeof(catalog_SysRelationshipOverlay));
	}

	pRelationship    = &CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo + i];

	CATALOG_GET_RELATIONSHIPFROMCLASSID(pRelationship)		= fromClassId;
	CATALOG_GET_RELATIONSHIPTOCLASSID(pRelationship)		= toClassId;
	CATALOG_GET_RELATIONSHIPFROMATTRNUM(pRelationship)		= fromAttrNum;
	CATALOG_GET_RELATIONSHIPTOATTRNUM(pRelationship)		= toAttrNum;
	CATALOG_GET_RELATIONSHIPDIRECTION(pRelationship)		= direction;
	CATALOG_GET_RELATIONSHIPCARDINALITY(pRelationship)		= cardinality;
	strcpy(CATALOG_GET_RELATIONSHIPRELATIONSHIPNAME(pRelationship), relationshipName);
	CATALOG_GET_RELATIONSHIPRELATIONSHIPID(pRelationship)	= relationshipId;

	for(i = 0; i < nOldRelationships; i++)
		CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[oldRelationshipInfo + i].inUse = 0;

	return eNOERROR;
}

