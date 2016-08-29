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

/*
 * catalog_getFreeEntry.c
 */

#include "LOM_Internal.h"
#include "Catalog_Internal.h"
#include "Catalog.h"

Four catalog_getFreeEntry(
    LOM_Handle *handle, 
    Four mountTableVolumnIndex,	/* IN: mount table array number about volumn ID */
    CatalogEntryType entryType,	/* IN: kind of entry */
    Four *freeEntryIndex	/* OUT: free entry */
    )
{
    Four v;		/* temporary for mount table array index */
    Four freeIndex;	/* free index */
    Four maxIndex;	/* max index */
    One  found;		/* TRUE if you find free entry */
    Four oldSize, newSize;
    Four i, j;

    found = 0;
    v = mountTableVolumnIndex;
    
    switch(entryType) {
      case CATALOG_CLASSINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeClassInfoEntryIndex;
        maxIndex = CATALOG_GET_CLASSINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; i < maxIndex; i++)
            if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].inUse == 0) {
                found = 1;
                break;
            }
        /* if free entry is empty */
        if(!found) {
            
            /* get free entry */
            for( i = 0; i < freeIndex - 1; i++)
                if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].inUse == 0) {
                    found = 1;
                    break;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_CLASSINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].classInfoTbl), sizeof(catalog_SysClassesOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_CLASSINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_CLASSINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = i;
        CATALOG_GET_CLASSINFOTBL(handle, v)[i].inUse = 1;
        freeIndex = MIN(maxIndex - 1, i + 1);
        break;
        
      case CATALOG_ATTRINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeAttrInfoEntryIndex;
        maxIndex = CATALOG_GET_ATTRINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; i < maxIndex; i++)
            if(CATALOG_GET_ATTRINFOTBL(handle, v)[i].inUse == 0) {
                found = 1;
                break;
            }
        /* if free entry is empty */
        if(!found) {
            
            /* get free entry */
            for( i = 0; i < freeIndex - 1; i++)
                if(CATALOG_GET_ATTRINFOTBL(handle, v)[i].inUse == 0) {
                    found = 1;
                    break;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_ATTRINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].attrInfoTbl), sizeof(catalog_SysAttributesOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_ATTRINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_ATTRINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = i;
        CATALOG_GET_ATTRINFOTBL(handle, v)[i].inUse = 1;
        freeIndex = MIN(maxIndex - 1, i + 1);
        break;
      case CATALOG_METHODINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeClassInfoEntryIndex;
        maxIndex = CATALOG_GET_METHODINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; i < maxIndex; i++)
            if(CATALOG_GET_METHODINFOTBL(handle, v)[i].inUse == 0) {
                found = 1;
                break;
            }
        /* if free entry is empty */
        if(!found) {
            
            /* get free entry */
            for( i = 0; i < freeIndex - 1; i++)
                if(CATALOG_GET_METHODINFOTBL(handle, v)[i].inUse == 0) {
                    found = 1;
                    break;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_METHODINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].methodInfoTbl), sizeof(catalog_SysMethodsOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_METHODINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_METHODINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = i;
        CATALOG_GET_METHODINFOTBL(handle, v)[i].inUse = 1;
        freeIndex = MIN(maxIndex - 1, i + 1);
        break;
      case CATALOG_INDEXINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeIndexInfoEntryIndex;
        maxIndex = CATALOG_GET_INDEXINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; i < maxIndex; i++)
            if(CATALOG_GET_INDEXINFOTBL(handle, v)[i].inUse == 0) {
                found = 1;
                break;
            }
        /* if free entry is empty */
        if(!found) {
            
            /* get free entry */
            for( i = 0; i < freeIndex - 1; i++)
                if(CATALOG_GET_INDEXINFOTBL(handle, v)[i].inUse == 0) {
                    found = 1;
                    break;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_INDEXINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].indexInfoTbl), sizeof(catalog_SysIndexesOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_INDEXINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_INDEXINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = i;
        CATALOG_GET_INDEXINFOTBL(handle, v)[i].inUse = 1;
        freeIndex = MIN(maxIndex - 1, i + 1);
        break;
        
      case CATALOG_SUPERCLASSINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeSuperClassInfoEntryIndex;
        maxIndex = CATALOG_GET_SUPERCLASSINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; i < maxIndex; i++)
            if(CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[i].inUse == 0) {
                found = 1;
                break;
            }
        /* if free entry is empty */
        if(!found) {
            
            /* get free entry */
            for( i = 0; i < freeIndex - 1; i++)
                if(CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[i].inUse == 0) {
                    found = 1;
                    break;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_SUPERCLASSINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].superClassInfoTbl), sizeof(catalog_SysSuperClassesOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_SUPERCLASSINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = i;
        CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[i].inUse = 1;
        freeIndex = MIN(maxIndex - 1, i + 1);
        break;
      case CATALOG_SUBCLASSINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeSubClassInfoEntryIndex;
        maxIndex = CATALOG_GET_SUBCLASSINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; i < maxIndex; i++)
            if(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[i].inUse == 0) {
                found = 1;
                break;
            }
        /* if free entry is empty */
        if(!found) {
            
            /* get free entry */
            for( i = 0; i < freeIndex - 1; i++)
                if(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[i].inUse == 0) {
                    found = 1;
                    break;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_SUBCLASSINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].subClassInfoTbl), sizeof(catalog_SysSubClassesOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_SUBCLASSINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_SUBCLASSINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = i;
        CATALOG_GET_SUBCLASSINFOTBL(handle, v)[i].inUse = 1;
        freeIndex = MIN(maxIndex - 1, i + 1);
        break;
      case CATALOG_RELATIONSHIPINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeRelationshipInfoEntryIndex;
        maxIndex = CATALOG_GET_RELATIONSHIPINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; i < maxIndex; i++)
            if(CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].inUse == 0) {
                found = 1;
                break;
            }
        /* if free entry is empty */
        if(!found) {
            
            /* get free entry */
            for( i = 0; i < freeIndex - 1; i++)
                if(CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].inUse == 0) {
                    found = 1;
                    break;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_RELATIONSHIPINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].relationshipInfoTbl), sizeof(catalog_SysRelationshipOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_RELATIONSHIPINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = i;
        CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].inUse = 1;
        freeIndex = MIN(maxIndex - 1, i + 1);
        break;
      default:
        CATALOG_ERROR(handle, eBADPARAMETER_CATALOG);
    }
    
    return eNOERROR;
}

Four catalog_getFreeEntries(LOM_Handle *handle, 
    Four mountTableVolumnIndex,/* IN: mount table array number about volumn ID */
    CatalogEntryType entryType,		/* IN: kind of entry */
    Four	numOfFreeEntries,		/* IN: the number of free entrie */
    Four 	*freeEntryIndex	/* OUT: free entry */
    )
{
    Four v;		/* temporary for mount table array index */
    Four i,j;
    Four freeIndex;	/* free index */
    Four maxIndex;	/* max index */
    Four contiguousFreeEntryIndex;
    Four firstFreeEntryIndex;
    Four oldSize, newSize;
    One  found;
    
	if(numOfFreeEntries <= 0)
		return eNOERROR;
	
    /* initialize local variables */
    found = 0;
    firstFreeEntryIndex = -1;
    contiguousFreeEntryIndex = 0;
    
    /* temporary copy */
    v = mountTableVolumnIndex;
    
    /* depends on entry type */
    switch(entryType) {
      case CATALOG_ATTRINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeAttrInfoEntryIndex;
        maxIndex = CATALOG_GET_ATTRINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; (freeIndex + numOfFreeEntries) < maxIndex && i < maxIndex; i++)
            /* get contiguous free slots */
            if(CATALOG_GET_ATTRINFOTBL(handle, v)[i].inUse == 0) {
                if(firstFreeEntryIndex == -1) {
                    firstFreeEntryIndex = i;
                    contiguousFreeEntryIndex = 1;
                }
                else {
                    contiguousFreeEntryIndex++;
                }
                if(contiguousFreeEntryIndex == numOfFreeEntries) {
                    found = 1;
                    break;
                }
            }
            else {
                firstFreeEntryIndex = -1;
                contiguousFreeEntryIndex = 0;
            }
        
        /* if free entry is empty */
        if(!found) {
            
            /* get contiguous free slots */
            for( i = 0;  numOfFreeEntries < freeIndex -1 &&  i < freeIndex - 1; i++)
                if(CATALOG_GET_ATTRINFOTBL(handle, v)[i].inUse == 0) {
                    if(firstFreeEntryIndex == -1) {
                        firstFreeEntryIndex = i;
                        contiguousFreeEntryIndex = 1;
                    }
                    else {
                        contiguousFreeEntryIndex++;
                    }
                    if(contiguousFreeEntryIndex == numOfFreeEntries) {
                        found = 1;
                        break;
                    }
                }
                else {
                    firstFreeEntryIndex = -1;
                    contiguousFreeEntryIndex = 0;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_ATTRINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].attrInfoTbl), sizeof(catalog_SysAttributesOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_ATTRINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_ATTRINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
                firstFreeEntryIndex = i;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = firstFreeEntryIndex;
        
        /* set valid flag */
        for(j = firstFreeEntryIndex; j < (firstFreeEntryIndex + numOfFreeEntries); j++) 
            CATALOG_GET_ATTRINFOTBL(handle, v)[j].inUse = 1;
        
        LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeAttrInfoEntryIndex = MIN(maxIndex - 1, firstFreeEntryIndex + numOfFreeEntries);
        break;
        
      case CATALOG_METHODINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeMethodInfoEntryIndex;
        maxIndex = CATALOG_GET_METHODINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; (freeIndex + numOfFreeEntries) < maxIndex && i < maxIndex; i++)
            /* get contiguous free slots */
            if(CATALOG_GET_METHODINFOTBL(handle, v)[i].inUse == 0) {
                if(firstFreeEntryIndex == -1) {
                    firstFreeEntryIndex = i;
                    contiguousFreeEntryIndex = 1;
                }
                else {
                    contiguousFreeEntryIndex++;
                }
                if(contiguousFreeEntryIndex == numOfFreeEntries) {
                    found = 1;
                    break;
                }
            }
            else {
                firstFreeEntryIndex = -1;
                contiguousFreeEntryIndex = 0;
            }
        
        /* if free entry is empty */
        if(!found) {
            
            /* get contiguous free slots */
            for( i = 0;  numOfFreeEntries < freeIndex -1 &&  i < freeIndex - 1; i++)
                if(CATALOG_GET_METHODINFOTBL(handle, v)[i].inUse == 0) {
                    if(firstFreeEntryIndex == -1) {
                        firstFreeEntryIndex = i;
                        contiguousFreeEntryIndex = 1;
                    }
                    else {
                        contiguousFreeEntryIndex++;
                    }
                    if(contiguousFreeEntryIndex == numOfFreeEntries) {
                        found = 1;
                        break;
                    }
                }
                else {
                    firstFreeEntryIndex = -1;
                    contiguousFreeEntryIndex = 0;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_METHODINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].methodInfoTbl), sizeof(catalog_SysMethodsOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_METHODINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_METHODINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
                firstFreeEntryIndex = i;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = firstFreeEntryIndex;
        
        /* set valid flag */
        for(j = firstFreeEntryIndex; j < (firstFreeEntryIndex + numOfFreeEntries); j++) 
            CATALOG_GET_METHODINFOTBL(handle, v)[j].inUse = 1;
        
        LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeMethodInfoEntryIndex = MIN(maxIndex - 1, firstFreeEntryIndex + numOfFreeEntries);
        break;
        
      case CATALOG_INDEXINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeIndexInfoEntryIndex;
        maxIndex = CATALOG_GET_INDEXINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; (freeIndex + numOfFreeEntries) < maxIndex && i < maxIndex; i++)
            /* get contiguous free slots */
            if(CATALOG_GET_INDEXINFOTBL(handle, v)[i].inUse == 0) {
                if(firstFreeEntryIndex == -1) {
                    firstFreeEntryIndex = i;
                    contiguousFreeEntryIndex = 1;
                }
                else {
                    contiguousFreeEntryIndex++;
                }
                if(contiguousFreeEntryIndex == numOfFreeEntries) {
                    found = 1;
                    break;
                }
            }
            else {
                firstFreeEntryIndex = -1;
                contiguousFreeEntryIndex = 0;
            }
        
        /* if free entry is empty */
        if(!found) {
            
            /* get contiguous free slots */
            for( i = 0;  numOfFreeEntries < freeIndex -1 &&  i < freeIndex - 1; i++)
                if(CATALOG_GET_INDEXINFOTBL(handle, v)[i].inUse == 0) {
                    if(firstFreeEntryIndex == -1) {
                        firstFreeEntryIndex = i;
                        contiguousFreeEntryIndex = 1;
                    }
                    else {
                        contiguousFreeEntryIndex++;
                    }
                    if(contiguousFreeEntryIndex == numOfFreeEntries) {
                        found = 1;
                        break;
                    }
                }
                else {
                    firstFreeEntryIndex = -1;
                    contiguousFreeEntryIndex = 0;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_INDEXINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].indexInfoTbl), sizeof(catalog_SysIndexesOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_INDEXINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_INDEXINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
                firstFreeEntryIndex = i;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = firstFreeEntryIndex;
        
        /* set valid flag */
        for(j = firstFreeEntryIndex; j < (firstFreeEntryIndex + numOfFreeEntries); j++) 
            CATALOG_GET_INDEXINFOTBL(handle, v)[j].inUse = 1;
        
        LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeIndexInfoEntryIndex = MIN(maxIndex - 1, firstFreeEntryIndex + numOfFreeEntries);
        break;
        
      case CATALOG_SUPERCLASSINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeSuperClassInfoEntryIndex;
        maxIndex = CATALOG_GET_SUPERCLASSINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; (freeIndex + numOfFreeEntries) < maxIndex && i < maxIndex; i++)
            /* get contiguous free slots */
            if(CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[i].inUse == 0) {
                if(firstFreeEntryIndex == -1) {
                    firstFreeEntryIndex = i;
                    contiguousFreeEntryIndex = 1;
                }
                else {
                    contiguousFreeEntryIndex++;
                }
                if(contiguousFreeEntryIndex == numOfFreeEntries) {
                    found = 1;
                    break;
                }
            }
            else {
                firstFreeEntryIndex = -1;
                contiguousFreeEntryIndex = 0;
            }
        
        /* if free entry is empty */
        if(!found) {
            
            /* get contiguous free slots */
            for( i = 0;  numOfFreeEntries < freeIndex -1 &&  i < freeIndex - 1; i++)
                if(CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[i].inUse == 0) {
                    if(firstFreeEntryIndex == -1) {
                        firstFreeEntryIndex = i;
                        contiguousFreeEntryIndex = 1;
                    }
                    else {
                        contiguousFreeEntryIndex++;
                    }
                    if(contiguousFreeEntryIndex == numOfFreeEntries) {
                        found = 1;
                        break;
                    }
                }
                else {
                    firstFreeEntryIndex = -1;
                    contiguousFreeEntryIndex = 0;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_SUPERCLASSINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].superClassInfoTbl), sizeof(catalog_SysSuperClassesOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_SUPERCLASSINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
                firstFreeEntryIndex = i;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = firstFreeEntryIndex;
        
        /* set valid flag */
        for(j = firstFreeEntryIndex; j < (firstFreeEntryIndex + numOfFreeEntries); j++) 
            CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[j].inUse = 1;
        
        LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeSuperClassInfoEntryIndex= MIN(maxIndex - 1, firstFreeEntryIndex + numOfFreeEntries);
        break;
        
      case CATALOG_SUBCLASSINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeSubClassInfoEntryIndex;
        maxIndex = CATALOG_GET_SUBCLASSINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; (freeIndex + numOfFreeEntries) < maxIndex && i < maxIndex; i++)
            /* get contiguous free slots */
            if(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[i].inUse == 0) {
                if(firstFreeEntryIndex == -1) {
                    firstFreeEntryIndex = i;
                    contiguousFreeEntryIndex = 1;
                }
                else {
                    contiguousFreeEntryIndex++;
                }
                if(contiguousFreeEntryIndex == numOfFreeEntries) {
                    found = 1;
                    break;
                }
            }
            else {
                firstFreeEntryIndex = -1;
                contiguousFreeEntryIndex = 0;
            }
        
        /* if free entry is empty */
        if(!found) {
            
            /* get contiguous free slots */
            for( i = 0;  numOfFreeEntries < freeIndex -1 &&  i < freeIndex - 1; i++)
                if(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[i].inUse == 0) {
                    if(firstFreeEntryIndex == -1) {
                        firstFreeEntryIndex = i;
                        contiguousFreeEntryIndex = 1;
                    }
                    else {
                        contiguousFreeEntryIndex++;
                    }
                    if(contiguousFreeEntryIndex == numOfFreeEntries) {
                        found = 1;
                        break;
                    }
                }
                else {
                    firstFreeEntryIndex = -1;
                    contiguousFreeEntryIndex = 0;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_SUBCLASSINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].subClassInfoTbl), sizeof(catalog_SysSubClassesOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_SUBCLASSINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_SUBCLASSINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
                firstFreeEntryIndex = i;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = firstFreeEntryIndex;
        
        /* set valid flag */
        for(j = firstFreeEntryIndex; j < (firstFreeEntryIndex + numOfFreeEntries); j++) 
            CATALOG_GET_SUBCLASSINFOTBL(handle, v)[j].inUse = 1;
        
        LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeSubClassInfoEntryIndex= MIN(maxIndex - 1, firstFreeEntryIndex + numOfFreeEntries);
        break;

      case CATALOG_RELATIONSHIPINFOTBL:
        freeIndex = LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeRelationshipInfoEntryIndex;
        maxIndex = CATALOG_GET_RELATIONSHIPINFOTBL_SIZE(handle, v);
        
        /* get free entry */
        for( i = freeIndex; (freeIndex + numOfFreeEntries) < maxIndex && i < maxIndex; i++)
            /* get contiguous free slots */
            if(CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].inUse == 0) {
                if(firstFreeEntryIndex == -1) {
                    firstFreeEntryIndex = i;
                    contiguousFreeEntryIndex = 1;
                }
                else {
                    contiguousFreeEntryIndex++;
                }
                if(contiguousFreeEntryIndex == numOfFreeEntries) {
                    found = 1;
                    break;
                }
            }
            else {
                firstFreeEntryIndex = -1;
                contiguousFreeEntryIndex = 0;
            }
        
        /* if free entry is empty */
        if(!found) {
            
            /* get contiguous free slots */
            for( i = 0;  numOfFreeEntries < freeIndex -1 &&  i < freeIndex - 1; i++)
                if(CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].inUse == 0) {
                    if(firstFreeEntryIndex == -1) {
                        firstFreeEntryIndex = i;
                        contiguousFreeEntryIndex = 1;
                    }
                    else {
                        contiguousFreeEntryIndex++;
                    }
                    if(contiguousFreeEntryIndex == numOfFreeEntries) {
                        found = 1;
                        break;
                    }
                }
                else {
                    firstFreeEntryIndex = -1;
                    contiguousFreeEntryIndex = 0;
                }
            
            if(!found) {
                oldSize = CATALOG_GET_RELATIONSHIPINFOTBL_SIZE(handle, v);
                if( LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].relationshipInfoTbl), sizeof(catalog_SysRelationshipOverlay)) < 0)
                    CATALOG_ERROR(handle, eMEMORYALLOCERR_CATALOG);
                else
                {
                    newSize = CATALOG_GET_RELATIONSHIPINFOTBL_SIZE(handle, v);
                    for(j = oldSize; j < newSize; j++)
                        CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[j].inUse = 0;
                }
                i = maxIndex;
                firstFreeEntryIndex = i;
            }
        }
        
        /* set free index and valid flag */
        *freeEntryIndex = firstFreeEntryIndex;
        
        /* set valid flag */
        for(j = firstFreeEntryIndex; j < (firstFreeEntryIndex + numOfFreeEntries); j++) 
            CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[j].inUse = 1;
        
        LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeRelationshipInfoEntryIndex= MIN(maxIndex - 1, firstFreeEntryIndex + numOfFreeEntries);
        break;
      default:
        CATALOG_ERROR(handle, eBADPARAMETER_CATALOG);
    }
    
    return eNOERROR;
}



