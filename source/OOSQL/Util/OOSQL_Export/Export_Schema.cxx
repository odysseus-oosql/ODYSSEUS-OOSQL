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

// Include Header file
#include "Export.hxx"


Four Export_ExportSchema (
    ExportConfig&           configuration,  // IN/OUT
    Array<ExportClassInfo>& classInfos)     // OUT
{
    Four            i;
    Four            e;    
    Array<String>   classNames;


    // I. Fetch class names
    e = export_FetchClassNames(configuration, classNames);    
    TOOL_CHECK_ERR(e);


    // II. Fetch class information
    for (i = 0; i < classNames.numberOfItems(); i++) 
    {
        ExportClassInfo classInfo;


        // 1. Fetch class infomation
        e = export_FetchClassInfo(configuration, classNames[i], classInfo);
        TOOL_CHECK_ERR(e);


        // 2. Check tablename and indexname's validity in configuration.byindexes
        e = export_CheckByIndex(configuration, classInfo);
        TOOL_CHECK_ERR(e);

        // 3. Put class information
        e = export_WriteClassInfo(configuration, classInfo);
        TOOL_CHECK_ERR(e);

        classInfos.add(classInfo);
    }


    // III. Put class names 
    e = export_WriteClassNames(configuration, classNames);
    TOOL_CHECK_ERR(e);


    // IV. Put Keyword Extractor information 
    e = export_FetchKeywordExtractorInfos(configuration);


    return eNOERROR;
}


Four export_FetchClassNames(
    ExportConfig&       configuration,  // IN/OUT
    Array<String>&      classNames)     // OUT
{
    Four    e;
    Four    i;


    if (configuration.full == TRUE) 
    {
        e = export_GetClassNames(configuration, classNames);
        TOOL_CHECK_ERR(e);
    }
    else 
    {       
        for (i = 0; i < configuration.tables.numberOfItems(); i++) {
            classNames.add(configuration.tables[i]);
        }
    }


    return eNOERROR;
}


Four export_FetchClassInfo(
    ExportConfig&       configuration,      // IN/OUT    
    const String&       className,          // IN
    ExportClassInfo&    classInfo)          // OUT
{
    Four        i, j;
    Four        e;

    // 1. Fetch className
    classInfo.className = className;

    // 2. Fetch classId
    e = export_FetchClassId(configuration, classInfo.className, classInfo.classId);
    TOOL_CHECK_ERR(e);

    // 3. Fetch classAttributes
    e = export_FetchClassAttributes(configuration, classInfo.className, classInfo.nAttrs, classInfo.attrsInfo);
    TOOL_CHECK_ERR(e);

    // 4. Fetch classMethods
    e = export_FetchClassMethods(configuration, classInfo.className, classInfo.nMethods, classInfo.methodsInfo);
    TOOL_CHECK_ERR(e);

    // 5. Fetch classSuperclasses
    e = export_FetchClassSuperclasses(configuration, classInfo.className, classInfo.nSuperclasses, classInfo.superclassesInfo);
    TOOL_CHECK_ERR(e);

    // 6. Fetch classIndexes
    e = export_FetchClassIndexes(configuration, classInfo.className, classInfo.nIndexes, classInfo.indexesInfo, classInfo.attrsInfo);
    TOOL_CHECK_ERR(e);
    

    // FOR DEBUG
    /*
    for (i=0; i < classInfo.nAttrs; i++) 
    {
        fprintf(stderr, "attrsInfo[%ld].columnNo         = %ld\n", i, classInfo.attrsInfo[i].columnNo);
        fprintf(stderr, "attrsInfo[%ld].complexType      = %ld\n", i, classInfo.attrsInfo[i].complexType);
        fprintf(stderr, "attrsInfo[%ld].colType          = %ld\n", i, classInfo.attrsInfo[i].colType);
        fprintf(stderr, "attrsInfo[%ld].length           = %ld\n", i, classInfo.attrsInfo[i].length);
        fprintf(stderr, "attrsInfo[%ld].colName          = %s\n", i, (const char *)classInfo.attrsInfo[i].colName);
        fprintf(stderr, "attrsInfo[%ld].inheritedFrom    = %ld\n", i, classInfo.attrsInfo[i].inheritedFrom);
        fprintf(stderr, "attrsInfo[%ld].domainId         = %ld\n", i, classInfo.attrsInfo[i].domainId);
        fprintf(stderr, "=================================================\n");
    }
    
    for (i=0; i < classInfo.nMethods; i++) 
    {
        fprintf(stderr, "methodsInfo[%ld].dirPath        = %s\n", i, (const char *)classInfo.methodsInfo[i].dirPath);
        fprintf(stderr, "methodsInfo[%ld].methodName     = %s\n", i, (const char *)classInfo.methodsInfo[i].methodName);
        fprintf(stderr, "methodsInfo[%ld].functionName   = %s\n", i, (const char *)classInfo.methodsInfo[i].functionName);
        fprintf(stderr, "methodsInfo[%ld].nArguments     = %ld\n", i, classInfo.methodsInfo[i].nArguments);
    
        for (j = 0; j < classInfo.methodsInfo[i].nArguments; j++) 
            fprintf(stderr, "methodsInfo[%ld].ArgumentsList[%ld]  = %ld\n", i, j, classInfo.methodsInfo[i].ArgumentsList[j]);

        fprintf(stderr, "methodsInfo[%ld].returnType     = %ld\n", i, classInfo.methodsInfo[i].returnType);
        fprintf(stderr, "methodsInfo[%ld].inheritedFrom  = %ld\n", i, classInfo.methodsInfo[i].inheritedFrom);
        fprintf(stderr, "=================================================\n");
    }

    for (i=0; i < classInfo.nSuperclasses; i++) 
    {
        fprintf(stderr, "superclassesInfo[%ld].superclassId      = %ld\n", i, classInfo.superclassesInfo[i].superclassId);
        fprintf(stderr, "superclassesInfo[%ld].superclassName    = %s\n", i, (const char *)classInfo.superclassesInfo[i].superclassName);
    }

    for (i=0; i < classInfo.nIndexes; i++) 
    {
        fprintf(stderr, "indexInfo[%ld].indexName    = %s\n", i, classInfo.indexesInfo[i].indexName);
        fprintf(stderr, "indexInfo[%ld].indexType    = %s\n", i, classInfo.indexesInfo[i].indexType);

        switch(classInfo.indexesInfo[i].indexType)
        {
        case LOM_INDEXTYPE_BTREE:
            fprintf(stderr, "btree(%ld %ld [ ",
                    classInfo.indexesInfo[i].btree.flag,
                    classInfo.indexesInfo[i].btree.nColumns);

            for(j = 0; j < classInfo.indexesInfo[i].btree.nColumns; j++)
            {
                fprintf(stderr, "(%ld %ld) ", 
                        classInfo.indexesInfo[i].btree.columns[j].colNo,
                        classInfo.indexesInfo[i].btree.columns[j].flag);
            }

            fprintf(stderr, "])");
            break;

        case LOM_INDEXTYPE_MLGF:
            fprintf(stderr, "mlgf(%ld %ld [ ",
                    classInfo.indexesInfo[i].mlgf.flag,
                    classInfo.indexesInfo[i].mlgf.nColumns);

            for(j = 0; j < classInfo.indexesInfo[i].mlgf.nColumns; j++)
            {
                fprintf(stderr, "%ld ", 
                        classInfo.indexesInfo[i].mlgf.colNo[j]);
            }

            fprintf(stderr, "] %ld)",
                    classInfo.indexesInfo[i].mlgf.extraDataLen);
            break;

        case LOM_INDEXTYPE_TEXT:
            break;

        default:
            TOOL_ERR(eUNHANDLED_TYPE_EXPORT);
            break;
        }
        fprintf(stderr, "\n");
    }
    */


    return eNOERROR;
}


Four export_FetchClassId (
    ExportConfig&           configuration,  // IN/OUT
    const String&           className,      // IN
    Four&                   classId)        // OUT
{
    Four    e;

    e = LOM_GetClassID(&configuration.handle.lomSystemHandle, configuration.volumeId, (char*)(const char *)className, &classId);
    LOM_CHECK_ERR(e);

    return eNOERROR;
}


Four export_FetchClassAttributes (
    ExportConfig&           configuration,  // IN/OUT
    const String&           className,      // IN
    Four&                   nAttrs,         // OUT
    Array<ExportAttrInfo>&  attrsInfo)      // OUT
{
    Four                            e;
    Four                            i;
    Four                            v;
    Four                            classId;
    Four                            idxForClassInfo;
    catalog_SysClassesOverlay       *ptrToSysClasses;
    catalog_SysAttributesOverlay    *ptrToSysAttributes;
    
    e = LOM_GetClassID(&configuration.handle.lomSystemHandle, configuration.volumeId, (char*)(const char *)className, &classId);
    LOM_CHECK_ERR(e);
        
    e = Catalog_GetClassInfo(&configuration.handle.lomSystemHandle, configuration.volumeId, classId, &idxForClassInfo);
    CATALOG_CHECK_ERR(e);

    v = Catalog_GetVolIndex(&configuration.handle.lomSystemHandle, configuration.volumeId);
    CATALOG_CHECK_ERR(v);

    // set physical pointer to in-memory catalog
    ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL((&configuration.handle.lomSystemHandle), v)[idxForClassInfo];
    ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL((&configuration.handle.lomSystemHandle), v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];


    // 주의! 첫번째 column에는 항상 logical id를 위한 column이 존재한다. in-memory catalog에서는 이에 대한 정보도 넘겨주나 
    // 이는 사용자가 알 필요가 없는 것이므로 무시한다.
    for (i = 0; i < CATALOG_GET_ATTRNUM(ptrToSysClasses); i++) {
        ExportAttrInfo      attrInfo;

        attrInfo.columnNo       = GET_USERLEVEL_COLNO(CATALOG_GET_ATTRCOLNO(&ptrToSysAttributes[i]));
        attrInfo.complexType    = CATALOG_GET_ATTRCOMPLEXTYPE(&ptrToSysAttributes[i]);
        attrInfo.colType        = CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[i]);
        attrInfo.length         = CATALOG_GET_ATTRLENGTH(&ptrToSysAttributes[i]);
        attrInfo.colName        = String(CATALOG_GET_ATTRNAME(&ptrToSysAttributes[i]));
        attrInfo.inheritedFrom  = CATALOG_GET_ATTRINHERITEDFROM(&ptrToSysAttributes[i]);
        attrInfo.domainId       = CATALOG_GET_ATTRDOMAIN(&ptrToSysAttributes[i]);

        attrInfo.idxOfIndexInfo = -1;   // initialize

		if (strcmp((const char*)attrInfo.colName, "_logicalId"))
		{
			if (attrInfo.columnNo == -1)	attrInfo.columnNo += 1;

        	attrsInfo.add(attrInfo);
		}
    }
    nAttrs = attrsInfo.numberOfItems();
    

    return eNOERROR;
}


Four export_FetchClassMethods (
    ExportConfig&               configuration,  // IN/OUT
    const String&               className,      // IN
    Four&                       nMethods,       // OUT
    Array<ExportMethodInfo>&    methodsInfo)    // OUT
{
    Four                            e;
    Four                            i, j;
    Four                            v;
    Four                            classId;
    Four                            idxForClassInfo;
    catalog_SysClassesOverlay       *ptrToSysClasses;
    catalog_SysMethodsOverlay       *ptrToSysMethods;
    

    e = LOM_GetClassID(&configuration.handle.lomSystemHandle, configuration.volumeId, (char*)(const char *)className, &classId);
    LOM_CHECK_ERR(e);
    
    e = Catalog_GetClassInfo(&configuration.handle.lomSystemHandle, configuration.volumeId, classId, &idxForClassInfo);
    CATALOG_CHECK_ERR(e);

    v = Catalog_GetVolIndex(&configuration.handle.lomSystemHandle, configuration.volumeId);
    CATALOG_CHECK_ERR(v);

    // set physical pointer to in-memory catalog
    ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL((&configuration.handle.lomSystemHandle), v)[idxForClassInfo];
    ptrToSysMethods = &CATALOG_GET_METHODINFOTBL((&configuration.handle.lomSystemHandle), v)[CATALOG_GET_METHODINFOTBL_INDEX(ptrToSysClasses)];


    for (i = 0; i < CATALOG_GET_METHODNUM(ptrToSysClasses); i++) {
        ExportMethodInfo        methodInfo;

        methodInfo.dirPath       = String(CATALOG_GET_METHODDIRPATH(&ptrToSysMethods[i]));
        methodInfo.methodName    = String(CATALOG_GET_METHODNAME(&ptrToSysMethods[i]));
        methodInfo.functionName  = String(CATALOG_GET_METHODFUNCTIONNAME(&ptrToSysMethods[i]));
        methodInfo.nArguments    = CATALOG_GET_METHODNARGUMENTS(&ptrToSysMethods[i]);

        for (j = 0; j < methodInfo.nArguments; j++)
            methodInfo.ArgumentsList.add(CATALOG_GET_METHODARGUMENTLIST(&ptrToSysMethods[i])[j]);

        methodInfo.returnType    = CATALOG_GET_METHODRETURNTYPE(&ptrToSysMethods[i]);
        methodInfo.inheritedFrom = CATALOG_GET_METHODINHERITEDFROM(&ptrToSysMethods[i]);

        methodsInfo.add(methodInfo);
    }
    nMethods = methodsInfo.numberOfItems();


    return eNOERROR;
}



Four export_FetchClassSuperclasses (
    ExportConfig&                   configuration,      // IN/OUT
    const String&                   className,          // IN
    Four&                           nSuperclasses,      // OUT
    Array<ExportSuperclassInfo>&    superclassesInfo)   // OUT
{
    Four                            e;
    Four                            i;
    Four                            v;
    Four                            classId;
    Four                            idxForClassInfo;
    char                            superclassName[LOM_MAXCLASSNAME];
    catalog_SysClassesOverlay       *ptrToSysClasses;
    catalog_SysSuperClassesOverlay  *ptrToSysSuperclasses;
    
    e = LOM_GetClassID(&configuration.handle.lomSystemHandle, configuration.volumeId, (char*)(const char *)className, &classId);
    LOM_CHECK_ERR(e);
    
    e = Catalog_GetClassInfo(&configuration.handle.lomSystemHandle, configuration.volumeId, classId, &idxForClassInfo);
    CATALOG_CHECK_ERR(e);

    v = Catalog_GetVolIndex(&configuration.handle.lomSystemHandle, configuration.volumeId);
    CATALOG_CHECK_ERR(v);

    // set physical pointer to in-memory catalog
    ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL((&configuration.handle.lomSystemHandle), v)[idxForClassInfo];
    ptrToSysSuperclasses = &CATALOG_GET_SUPERCLASSINFOTBL((&configuration.handle.lomSystemHandle), v)[CATALOG_GET_SUPERCLASSINFOTBL_INDEX(ptrToSysClasses)];


    for (i = 0; i < CATALOG_GET_SUPERCLASSNUM(ptrToSysClasses); i++) {
        ExportSuperclassInfo        superclassInfo;

        superclassInfo.superclassId = CATALOG_GET_SUPERCLASSEID(&ptrToSysSuperclasses[i]);

        e = LOM_GetClassName(&configuration.handle.lomSystemHandle, configuration.volumeId, superclassInfo.superclassId, superclassName);
        LOM_CHECK_ERR(e);
        superclassInfo.superclassName = superclassName; 

        superclassesInfo.add(superclassInfo);
    }
    nSuperclasses = superclassesInfo.numberOfItems();

    
    return eNOERROR;
}



Four export_FetchClassIndexes (
    ExportConfig&               configuration,  // IN/OUT
    const String&               className,      // IN
    Four&                       nIndexes,       // OUT
    Array<ExportIndexInfo>&     indexesInfo,    // OUT
    Array<ExportAttrInfo>&      attrsInfo)      // IN/OUT
{
    Four                            e;
    Four                            i, j;
    Four                            v;
    Four                            classId;
    Four                            idxForClassInfo;
    catalog_SysClassesOverlay       *ptrToSysClasses;
    catalog_SysIndexesOverlay       *ptrToSysIndexes;
    ExportIndexInfo                 indexInfo;


    e = LOM_GetClassID(&configuration.handle.lomSystemHandle, configuration.volumeId, (char*)(const char *)className, &classId);
    LOM_CHECK_ERR(e);

    e = Catalog_GetClassInfo(&configuration.handle.lomSystemHandle, configuration.volumeId, classId, &idxForClassInfo);
    CATALOG_CHECK_ERR(e);

    v = Catalog_GetVolIndex(&configuration.handle.lomSystemHandle, configuration.volumeId);
    CATALOG_CHECK_ERR(v);

    // set physical pointer to in-memory catalog
    ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL((&configuration.handle.lomSystemHandle), v)[idxForClassInfo];

    
    if(CATALOG_GET_INDEXINFOTBL_INDEX(ptrToSysClasses) >= 0)
    {
        ptrToSysIndexes = &CATALOG_GET_INDEXINFOTBL((&configuration.handle.lomSystemHandle), v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrToSysClasses)];

        for(i = 0; i < CATALOG_GET_INDEXNUM(ptrToSysClasses); i++)
        {
            ExportIndexInfo         indexInfo;
            KeyDesc                 *btreeKeyDesc;
            MLGF_KeyDesc            *mlgfKeyDesc;
            InvertedIndexDesc       *invertedIndexKeyDesc;

            if(CATALOG_GET_INDEXTYPE(&ptrToSysIndexes[i]) == LOM_INDEXTYPE_BTREE &&
               CATALOG_GET_INDEXCOLNO(&ptrToSysIndexes[i])[0] == 0)
            {
                // logical column에 만들어진 btree index는 무시한다.
                continue;
            }

            indexInfo.indexName = CATALOG_GET_INDEXNAME(&ptrToSysIndexes[i]);
            indexInfo.indexType = CATALOG_GET_INDEXTYPE(&ptrToSysIndexes[i]);
            indexInfo.iid       = CATALOG_GET_INDEXID(&ptrToSysIndexes[i]);

            switch(indexInfo.indexType)
            {
            case LOM_INDEXTYPE_BTREE:
                btreeKeyDesc = &CATALOG_GET_INDEXKEYDESC(&ptrToSysIndexes[i]).btree;

                indexInfo.btree.flag     = btreeKeyDesc->flag;
                indexInfo.btree.nColumns = btreeKeyDesc->nparts;
                for(j = 0; j < btreeKeyDesc->nparts; j++)
                {
                    Exportcolumn columnInfo;

                    columnInfo.colNo = GET_USERLEVEL_COLNO(CATALOG_GET_INDEXCOLNO(&ptrToSysIndexes[i])[j]);
                    columnInfo.flag  = KEYINFO_COL_ASC;

                    indexInfo.btree.columns.add(columnInfo);
                }

                break;

            case LOM_INDEXTYPE_MLGF:
                mlgfKeyDesc = &CATALOG_GET_INDEXKEYDESC(&ptrToSysIndexes[i]).mlgf;

                indexInfo.mlgf.flag     = mlgfKeyDesc->flag;
                indexInfo.mlgf.nColumns = mlgfKeyDesc->nKeys;
                for (j = 0; j < mlgfKeyDesc->nKeys; j++)
                {
                    indexInfo.mlgf.colNo.add(GET_USERLEVEL_COLNO(CATALOG_GET_INDEXCOLNO(&ptrToSysIndexes[i])[j]));
                }
                indexInfo.mlgf.extraDataLen = mlgfKeyDesc->extraDataLen;

                break;

            case LOM_INDEXTYPE_TEXT:
                // User Defined Posting을 처리하기 위해서는 TEXT INDEX정보도 export해야 한다.
                invertedIndexKeyDesc = &CATALOG_GET_INDEXKEYDESC(&ptrToSysIndexes[i]).invertedIndex;

                indexInfo.invertedIndex.keywordIndex        = invertedIndexKeyDesc->keywordIndex;
                indexInfo.invertedIndex.reverseKeywordIndex = invertedIndexKeyDesc->reverseKeywordIndex;
                indexInfo.invertedIndex.docIdIndex          = invertedIndexKeyDesc->docIdIndex;
                indexInfo.invertedIndex.invertedIndexName   = invertedIndexKeyDesc->invertedIndexName;
                indexInfo.invertedIndex.docIdIndexTableName = invertedIndexKeyDesc->docIdIndexTableName;
                indexInfo.invertedIndex.postingInfo.isContainingTupleID = invertedIndexKeyDesc->postingInfo.isContainingTupleID;
                indexInfo.invertedIndex.postingInfo.isContainingSentenceAndWordNum = invertedIndexKeyDesc->postingInfo.isContainingSentenceAndWordNum;
                indexInfo.invertedIndex.postingInfo.isContainingByteOffset = invertedIndexKeyDesc->postingInfo.isContainingByteOffset;
                indexInfo.invertedIndex.postingInfo.nEmbeddedAttributes = invertedIndexKeyDesc->postingInfo.nEmbeddedAttributes;

                for (j = 0; j < invertedIndexKeyDesc->postingInfo.nEmbeddedAttributes; j++)
                {
                    indexInfo.invertedIndex.postingInfo.embeddedAttrNo.add(invertedIndexKeyDesc->postingInfo.embeddedAttrNo[j]);
                    indexInfo.invertedIndex.postingInfo.embeddedAttrOffset.add(invertedIndexKeyDesc->postingInfo.embeddedAttrOffset[j]);
                    indexInfo.invertedIndex.postingInfo.embeddedAttrVarColNo.add(invertedIndexKeyDesc->postingInfo.embeddedAttrVarColNo[j]); 
                }

                // index가 text type일 때에만, 해당 ExportIndexInfo의 idx를 지정해준다. 
                indexInfo.invertedIndex.colName = attrsInfo[CATALOG_GET_INDEXCOLNO(&ptrToSysIndexes[i])[0]-1].colName;
                attrsInfo[CATALOG_GET_INDEXCOLNO(&ptrToSysIndexes[i])[0]-1].idxOfIndexInfo = i-1;
                break;

            default:
                // 처리 못하는 indextype이므로 에러를 출력한다.     
                TOOL_ERR(eUNHANDLED_TYPE_EXPORT);
                break;
            }

            /* User Defined Posting을 처리하기 위해서는 TEXT INDEX정보도 필요하다  
            if (indexInfo.indexType != LOM_INDEXTYPE_TEXT)
            */
            indexesInfo.add(indexInfo);
        }
    }
    nIndexes = indexesInfo.numberOfItems();


    return eNOERROR;
}



Four export_WriteClassInfo (
    ExportConfig&           configuration,  // IN/OUT
    const ExportClassInfo&  classInfo)      // IN
{
    Four    i, j;
    Four    e;
    FILE    *fp;
    char    fileName[MAXFILENAME];

    // I-1. make data file name 
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, (const char *)classInfo.className);
    strcat(fileName, ".scm");

    // I-2. Open data file to be exported
    fp = Util_fopen(fileName, "w");
    if (fp == NULL) 
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
    }


    // II. write classInfo.className
    e = fprintf(fp, "# class name\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

    e = fprintf(fp, "%s\n", (const char *)classInfo.className);
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);


    // III. write classInfo.classId
    e = fprintf(fp, "# class ID\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

    e = fprintf(fp, "%ld\n", (const char *)classInfo.classId);
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);


    // IV. write attribute Info
    e = fprintf(fp, "# class attributes\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

    for (i = 0; i < classInfo.nAttrs; i++) 
    {
        e = fprintf(fp, "%ld %ld %ld %ld %s %ld %ld\n", 
                    classInfo.attrsInfo[i].columnNo,
                    classInfo.attrsInfo[i].complexType,
                    classInfo.attrsInfo[i].colType,
                    classInfo.attrsInfo[i].length,
                    (const char *)classInfo.attrsInfo[i].colName,
                    classInfo.attrsInfo[i].inheritedFrom,
                    classInfo.attrsInfo[i].domainId);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }


    // V. write method Info
    e = fprintf(fp, "# class methods\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

    for (i = 0; i < classInfo.nMethods; i++) 
    {
        e = fprintf(fp, "%s %s %s %ld [ ",
                    (const char *)classInfo.methodsInfo[i].dirPath,
                    (const char *)classInfo.methodsInfo[i].methodName,
                    (const char *)classInfo.methodsInfo[i].functionName,
                    classInfo.methodsInfo[i].nArguments);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        for (j = 0; j < classInfo.methodsInfo[i].nArguments; j++) 
        {
            e = fprintf(fp, "%ld ", classInfo.methodsInfo[i].ArgumentsList[j]);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        }

        e = fprintf(fp, "] %ld %ld\n",
                    classInfo.methodsInfo[i].returnType,
                    classInfo.methodsInfo[i].inheritedFrom);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }


    // VI. write superclass Info
    e = fprintf(fp, "# class's superclasses\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

    for (i = 0; i < classInfo.nSuperclasses; i++) 
    {
        e = fprintf(fp, "%ld %s\n", 
                    classInfo.superclassesInfo[i].superclassId,
                    (const char *)classInfo.superclassesInfo[i].superclassName);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }


    // VII. write index Info
    e = fprintf(fp, "# class indexes\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);


    for (i = 0; i < classInfo.nIndexes; i++) 
    {
        e = fprintf(fp, "%s %ld ",
            (const char *)classInfo.indexesInfo[i].indexName,
            classInfo.indexesInfo[i].indexType);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        // There is no need to export LOM_IndexID

        switch(classInfo.indexesInfo[i].indexType)
        {
        case LOM_INDEXTYPE_BTREE:
            e = fprintf(fp, "btree(%ld %ld [ ",
                        classInfo.indexesInfo[i].btree.flag,
                        classInfo.indexesInfo[i].btree.nColumns);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

            for(j = 0; j < classInfo.indexesInfo[i].btree.nColumns; j++)
            {
                e = fprintf(fp, "(%ld %ld) ", 
                            classInfo.indexesInfo[i].btree.columns[j].colNo,
                            classInfo.indexesInfo[i].btree.columns[j].flag);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            }

            e = fprintf(fp, "])");
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_INDEXTYPE_MLGF:
            e = fprintf(fp, "mlgf(%ld %ld [ ",
                        classInfo.indexesInfo[i].mlgf.flag,
                        classInfo.indexesInfo[i].mlgf.nColumns);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

            for(j = 0; j < classInfo.indexesInfo[i].mlgf.nColumns; j++)
            {
                e = fprintf(fp, "%ld ", 
                            classInfo.indexesInfo[i].mlgf.colNo[j]);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            }

            e = fprintf(fp, "] %ld)",
                        classInfo.indexesInfo[i].mlgf.extraDataLen);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_INDEXTYPE_TEXT:
            // IndexID나 invertedIndexName, docIdIndexTableName는 export할 필요 없음
            e = fprintf(fp, "text(%s %ld %ld %ld %ld [ ",
                        (char *)(const char *)classInfo.indexesInfo[i].invertedIndex.colName,
                        classInfo.indexesInfo[i].invertedIndex.postingInfo.isContainingTupleID,
                        classInfo.indexesInfo[i].invertedIndex.postingInfo.isContainingSentenceAndWordNum,
                        classInfo.indexesInfo[i].invertedIndex.postingInfo.isContainingByteOffset,
                        classInfo.indexesInfo[i].invertedIndex.postingInfo.nEmbeddedAttributes);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

            for (j = 0; j < classInfo.indexesInfo[i].invertedIndex.postingInfo.nEmbeddedAttributes; j++)
            {
                e = fprintf(fp, "%ld ", 
                            classInfo.indexesInfo[i].invertedIndex.postingInfo.embeddedAttrNo[j]);   
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            }

            e = fprintf(fp, "])");
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

            break;

        default:
            TOOL_ERR(eUNHANDLED_TYPE_EXPORT);
            break;
        }

        e = fprintf(fp, "\n");
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }

    // Close data file to be exported
    e = Util_fclose(fp);
    if (e == EOF) 
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
    }

    return eNOERROR;
}


Four export_WriteClassNames(
    ExportConfig&           configuration,  // IN/OUT
    const Array<String>&    classNames)     // OUT
{
    Four    i;
    Four    e;
    FILE    *fp;
    char    fileName[MAXFILENAME];

    // make data file name 
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, TABLEFILENAME);

    // Open data file to be exported
    fp = Util_fopen(fileName, "w");
    if (fp == NULL) {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
    }

    // write table names
    for (i = 0; i < classNames.numberOfItems(); i++) {
        e = fprintf(fp, "%s\n", (const char *)classNames[i]);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }

    // Close data file to be exported
    e = Util_fclose(fp);
    if (e == EOF) {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
    }

    return eNOERROR;
}


Four export_CheckByIndex(
    ExportConfig&       configuration,  // IN/OUT
    ExportClassInfo&    classInfo)      // IN/OUT
{
    Four    i;

    for (i = 0; i < configuration.byIndexes.numberOfItems(); i++)
    {
        if (classInfo.className == configuration.byIndexes[i].table) 
        {
            classInfo.byindexName = configuration.byIndexes[i].index;
            break;
        }
    }

    if (i == configuration.byIndexes.numberOfItems())
    {
        classInfo.byindexName = "";
    }

    return eNOERROR;
}



Four export_FetchKeywordExtractorInfos(
    ExportConfig&                   configuration)  // IN
{
    Four                            e;
    Four                            i;
    Array<ExportInstalledKEInfo>    installedKEInfos;
    Array<ExportSetupedKEInfo>      setupedKEInfos;
    FILE                            *fp;
    char                            fileName[MAXFILENAME];


    // I-1. make file name
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, TEXTKEYWORDEXT_INFOFILENAME);


    // I-2. Open file
    fp = Util_fopen(fileName, "w");
    if (fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
    }


    // I-3. Get installed keyword extractor info
    e = export_GetInstalledKEInfo(configuration, installedKEInfos);
    TOOL_CHECK_ERR(e);


    // I-4. Write installed keyword extractor info
    for (i = 0; i < installedKEInfos.numberOfItems(); i++)
    {
        // DEBUG
        /*
        fprintf(stderr, "|%s|\n", (const char *)installedKEInfos[i].keywordExtractorName);
        fprintf(stderr, "|%ld|\n", installedKEInfos[i].version); 
        fprintf(stderr, "|%s|\n", (const char *)installedKEInfos[i].keywordExtractorFilePath);
        fprintf(stderr, "|%s|\n", (const char *)installedKEInfos[i].keywordExtractorFunctionName);
        fprintf(stderr, "|%s|\n", (const char *)installedKEInfos[i].getNextPostingInfoFunctionName);
        fprintf(stderr, "|%s|\n", (const char *)installedKEInfos[i].finalizeKeywordExtractorFunctionName);
        fprintf(stderr, "|%ld|\n", installedKEInfos[i].keywordExtractorNo);
        */
        e = fprintf(fp, "%s\n", (const char *)installedKEInfos[i].keywordExtractorName);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%ld\n", installedKEInfos[i].version); 
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%s\n", (const char *)installedKEInfos[i].keywordExtractorFilePath);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%s\n", (const char *)installedKEInfos[i].keywordExtractorFunctionName);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%s\n", (const char *)installedKEInfos[i].getNextPostingInfoFunctionName);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%s\n", (const char *)installedKEInfos[i].finalizeKeywordExtractorFunctionName);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%ld\n", installedKEInfos[i].keywordExtractorNo);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fprintf(fp, "\n");
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }


    // I-5. Close file
    e = Util_fclose(fp);
    if (e == EOF)
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
    }




    // II-1. make file name
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, TEXTPREFERENCE_INFOFILENAME);


    // II-2. Open file
    fp = Util_fopen(fileName, "w");
    if (fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
    }


    // II-3. Get installed keyword extractor info
    e = export_GetSetupedKEInfo(configuration, setupedKEInfos);
    TOOL_CHECK_ERR(e);


    // II-4. Write installed keyword extractor info
    for (i = 0; i < setupedKEInfos.numberOfItems(); i++)
    {
        // DEBUG
        /*
        fprintf(stderr, "|%ld|\n", setupedKEInfos[i].classId); 
        fprintf(stderr, "|%ld|\n", setupedKEInfos[i].colNo);
        fprintf(stderr, "|%ld|\n", setupedKEInfos[i].filterNo);
        fprintf(stderr, "|%ld|\n", setupedKEInfos[i].keywordExtractorNo);
        fprintf(stderr, "|%ld|\n", setupedKEInfos[i].stemizerNo);
        */
        e = fprintf(fp, "%ld\n", setupedKEInfos[i].classId); 
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%ld\n", setupedKEInfos[i].colNo);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%ld\n", setupedKEInfos[i].filterNo);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%ld\n", setupedKEInfos[i].keywordExtractorNo);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        e = fprintf(fp, "%ld\n", setupedKEInfos[i].stemizerNo);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fprintf(fp, "\n");
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }


    // II-5. Close file
    e = Util_fclose(fp);
    if (e == EOF)
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
    }


    return eNOERROR;
}
