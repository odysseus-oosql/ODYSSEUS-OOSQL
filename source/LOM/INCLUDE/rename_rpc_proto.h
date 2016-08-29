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

#ifndef _RENAME_RPC_PROTO_H
#define _RENAME_RPC_PROTO_H

/* server thread creation and destruction */
#define Server_Ping	ping_server_1
#define Server_LOM_CreateThread server_lom_createthread_1
#define Server_LOM_DestroyThread server_lom_destroythread_1

/* database management */
#define Server_LOM_Mount server_lom_mount_1
#define Server_LOM_Dismount server_lom_dismount_1

/* transaction management */
#define Server_LOM_BeginTrans server_lom_begintrans_1
#define	Server_LOM_CommitTrans server_lom_committrans_1
#define Server_LOM_AbortTrans server_lom_aborttrans_1

/* index management */
#define Server_LOM_AddIndex server_lom_addindex_1
#define Server_LOM_DropIndex server_lom_dropindex_1

/* class management */
#define Server_LOM_OpenClass server_lom_openclass_1
#define Server_LOM_CloseClass server_lom_closeclass_1

/* scan management */
#define Server_LOM_CloseScan server_lom_closescan_1
#define	Server_LOM_OpenIndexScan server_lom_openindexscan_1
#define Server_LOM_OpenSeqScan server_lom_openseqscan_1
#define Server_LOM_NextObject server_lom_nextobject_1

/* schema management */
#define Server_LOM_CreateClass server_lom_createclass_1
#define Server_LOM_DestroyClass server_lom_destroyclass_1
#define Server_LOM_GetNewClassId server_lom_getnewclassid_1
#define Server_LOM_GetClassId server_lom_getclassid_1
#define Server_LOM_GetClassName server_lom_getclassname_1
#define Server_LOM_GetSubClasses server_lom_getsubclasses_1

/* object management */
#define Server_LOM_CreateObject server_lom_createobject_1
#define Server_LOM_DestroyObject server_lom_destroyobject_1
#define Server_LOM_UpdateObject server_lom_updateobject_1
#define Server_LOM_FetchObject server_lom_fetchobject_1
#define Server_LOM_FetchObject2 server_lom_fetchobject2_1

/* whole object */
#define Server_LOM_CreateWholeObject server_lom_createwholeobject_1
#define Server_LOM_UpdateWholeObject server_lom_updatewholeobject_1
#define Server_LOM_FetchWholeObject server_lom_fetchowholebject_1

/* large object management */
#define Server_LOM_CreateLargeObject server_lom_createlargeobject_1
#define Server_LOM_UpdateLargeObject server_lom_updatelargeobject_1
#define Server_LOM_FetchLargeObject server_lom_fetchlargeobject_1

/* set management */
#define Server_LOM_Set_Create server_lom_set_create_1
#define Server_LOM_Set_Destroy server_lom_set_destroy_1
#define Server_LOM_Set_InsertElements server_lom_set_insertelements_1
#define Server_LOM_Set_DeleteElements server_lom_set_deleteelements_1
#define Server_LOM_Set_IsMember server_lom_set_ismember_1
#define Server_LOM_Set_Scan_Open server_lom_set_scan_open_1
#define Server_LOM_Set_Scan_Close server_lom_set_scan_close_1
#define Server_LOM_Set_Scan_Next server_lom_set_scan_next_1
#define Server_LOM_Set_Scan_Insert server_lom_set_scan_insert_1
#define Server_LOM_Set_Scan_Delete server_lom_set_scan_delete_1

/* relationship management */
#define Server_LOM_RS_Create server_lom_rs_create_1
#define Server_LOM_RS_Destroy server_lom_rs_destroy_1
#define Server_LOM_RS_CreateInstance server_lom_rs_createinstance_1
#define Server_LOM_RS_DestroyInstance server_lom_rs_destroyinstance_1
#define Server_LOM_RS_GetId server_lom_rs_getid_1
#define Server_LOM_RS_OpenScan server_lom_rs_openscan_1
#define Server_LOM_RS_CloseScan server_lom_rs_closescan_1
#define Server_LOM_RS_NextInstances server_lom_rs_nextinstances_1

/* text management */
#define Server_LOM_Text_CreateContent server_lom_text_createcontent_1
#define Server_LOM_Text_DestroyContent server_lom_text_destroycontent_1
#define Server_LOM_Text_FetchContent server_lom_text_fetchcontent_1
#define Server_LOM_Text_UpdateContent server_lom_text_updatecontent_1
#define Server_LOM_Text_MakeIndex server_lom_text_makeindex_1
#define Server_LOM_Text_GetIndexID server_lom_text_getindexid_1
#define Server_LOM_Text_GetDescriptor server_lom_text_getdescriptor_1
#define Server_LOM_Text_OpenIndexScan server_lom_text_openindexscan_1
#define Server_LOM_Text_Scan_Open server_lom_text_scan_open_1
#define Server_LOM_Text_Scan_Close server_lom_text_scan_close_1
#define Server_LOM_Text_Scan_NextPosting server_lom_text_scan_nextposting_1
#define Server_LOM_Text_NextPostings server_lom_text_nextpostings_1
#define Server_LOM_Text_GetCursorKeyword server_lom_text_getcursorkeyword_1
#define Server_LOM_Text_AddFilter server_lom_text_addfilter_1
#define Server_LOM_Text_DropFilter server_lom_text_dropfilter_1
#define Server_LOM_Text_GetFilterNo server_lom_text_getfilterno_1
#define Server_LOM_Text_SetFilter server_lom_text_setfilter_1
#define Server_LOM_Text_GetFilterInfo server_lom_text_getfilterinfo_1
#define Server_LOM_Text_GetCurrentFilterNo server_lom_text_getcurrentfilterno_1
#define Server_LOM_Text_ResetFilter server_lom_text_resetfilter_1
#define Server_LOM_Text_AddKeywordExtractor server_lom_text_addkeywordextractor_1
#define Server_LOM_Text_AddDefaultKeywordExtractor server_lom_text_adddefaultkeywordextractor_1
#define Server_LOM_Text_DropKeywordExtractor server_lom_text_dropkeywordextractor_1
#define Server_LOM_Text_GetKeywordExtractorNo server_lom_text_getkeywordextractorno_1
#define Server_LOM_Text_SetKeywordExtractor server_lom_text_setkeywordextractor_1
#define Server_LOM_Text_GetKeywordExtractorInfo server_lom_text_getkeywordextractorinfo_1
#define Server_LOM_Text_GetCurrentKeywordExtractorNo server_lom_text_getcurrentkeywordextractorno_1
#define Server_LOM_Text_ResetKeywordExtractor server_lom_text_resetkeywordextractor_1

/* query processing */
#define Server_OOSQL_CreateSystemHandle server_oosql_createsystemhandle_1
#define Server_OOSQL_DestroySystemHandle server_oosql_destroysystemhandle_1
#define Server_OOSQL_AllocHandle server_oosql_allochandle_1
#define Server_OOSQL_FreeHandle server_oosql_freehandle_1
#define Server_OOSQL_Mount server_oosql_mount_1
#define Server_OOSQL_Dismount server_oosql_dismount_1
#define Server_OOSQL_SetUserDefaultVolumeID server_oosql_setuserdefaultvolumeid_1
#define Server_OOSQL_GetUserDefaultVolumeID server_oosql_getuserdefaultvolumeid_1
#define Server_OOSQL_GetVolumeID server_oosql_getvolumeid_1
#define Server_OOSQL_MountDB server_oosql_mountdb_1
#define Server_OOSQL_DismountDB server_oosql_dismountdb_1
#define Server_OOSQL_TransBegin server_oosql_transbegin_1
#define Server_OOSQL_TransCommit server_oosql_transcommit_1
#define Server_OOSQL_TransAbort server_oosql_transabort_1
#define Server_OOSQL_Prepare server_oosql_prepare_1
#define Server_OOSQL_Execute server_oosql_execute_1
#define Server_OOSQL_ExecDirect server_oosql_execdirect_1
#define Server_OOSQL_Next server_oosql_next_1
#define Server_OOSQL_GetData server_oosql_getdata_1
#define Server_OOSQL_PutData server_oosql_putdata_1
#define Server_OOSQL_GetOID server_oosql_getoid_1
#define Server_OOSQL_GetNumResultCols server_oosql_getnumresultcols_1
#define Server_OOSQL_GetResultColName server_oosql_getresultcolname_1
#define Server_OOSQL_GetResultColType server_oosql_getresultcoltype_1
#define Server_OOSQL_GetResultColLength server_oosql_getresultcollength_1
#define Server_OOSQL_GetErrorMessage server_oosql_geterrormessage_1
#define Server_OOSQL_OIDToOIDString server_oosql_oidtooidstring_1
#define Server_OOSQL_Text_AddKeywordExtractor server_oosql_text_addkeywordextractor_1
#define Server_OOSQL_Text_AddDefaultKeywordExtractor server_oosql_text_adddefaultkeywordextractor_1
#define Server_OOSQL_Text_DropKeywordExtractor server_oosql_text_dropkeywordextractor_1
#define Server_OOSQL_Text_SetKeywordExtractor server_oosql_text_setkeywordextractor_1
#define Server_OOSQL_Text_AddFilter server_oosql_text_addfilter_1
#define Server_OOSQL_Text_DropFilter server_oosql_text_dropfilter_1
#define Server_OOSQL_Text_SetFilter server_oosql_text_setfilter_1
#define Server_OOSQL_Text_MakeIndex server_oosql_text_makeindex_1

#define Server_ReleaseConnectionAndQuit server_releaseconnectionandquit_1

#define Broker_Connect broker_connect_1
#define Broker_Disconnect broker_disconnect_1
#define Server_GetMachineType server_getmachinetype_1

/* Catalog Manager Interface */
#define Server_Catalog_GetClassInfo server_catalog_getclassinfo_1

/* Named Object Management */
#define Server_LOM_OpenNamedObjectTable	server_lom_opennamedobjecttable_1
#define Server_LOM_CloseNamedObjectTable server_lom_closenamedobjecttable_1
#define Server_LOM_SetObjectName server_lom_setobjectname_1
#define Server_LOM_LookUpNamedObject server_lom_lookupnamedobject_1
#define Server_LOM_ResetObjectName server_lom_resetobjectname_1
#define Server_LOM_RenameNamedObject server_lom_renamenamedobject_1
#define Server_LOM_GetObjectName server_lom_getobjectname_1

/* Schema Definition Plan Executor */
#define Server_SDP_Execute server_sdp_execute_1

/* Collection type in LOM */
#define Server_LOM_ColSet_Create server_lom_colset_create_1
#define Server_LOM_ColSet_Destroy server_lom_colset_destroy_1
#define Server_LOM_ColSet_Assign  server_lom_colset_assign_1
#define Server_LOM_ColSet_GetN_Elements server_lom_colset_getn_elements_1
#define Server_LOM_ColSet_InsertElements server_lom_colset_insertelements_1
#define Server_LOM_ColSet_DeleteElements server_lom_colset_deleteelements_1
#define Server_LOM_ColSet_DeleteAll server_lom_colset_deleteall_1
#define Server_LOM_ColSet_IsMember server_lom_colset_ismember_1
#define Server_LOM_ColSet_IsEqual server_lom_colset_isequal_1
#define Server_LOM_ColSet_IsSubset server_lom_colset_issubset_1
#define Server_LOM_ColSet_Union server_lom_colset_union_1
#define Server_LOM_ColSet_Intersect server_lom_colset_intersect_1
#define Server_LOM_ColSet_Difference server_lom_colset_difference_1
#define Server_LOM_ColSet_UnionWith server_lom_colset_unionwith_1
#define Server_LOM_ColSet_IntersectWith server_lom_colset_intersectwith_1
#define Server_LOM_ColSet_DifferenceWith server_lom_colset_differencewith_1
#define Server_LOM_ColSet_Scan_Open server_lom_colset_scan_open_1
#define Server_LOM_ColSet_Scan_Close server_lom_colset_scan_close_1
#define Server_LOM_ColSet_Scan_NextElements server_lom_colset_scan_nextelements_1
#define Server_LOM_ColSet_Scan_GetSizeOfNextElements server_lom_colset_scan_getsizeofnextelements_1
#define Server_LOM_ColSet_Scan_InsertElements server_lom_colset_scan_insertelements_1
#define Server_LOM_ColSet_Scan_DeleteElements server_lom_colset_scan_deleteelements_1

/* Bag-related interfaces */
#define Server_LOM_ColBag_Create server_lom_colbag_create_1
#define Server_LOM_ColBag_Destroy server_lom_colbag_destroy_1
#define Server_LOM_ColBag_Assign  server_lom_colbag_assign_1
#define Server_LOM_ColBag_GetN_Elements server_lom_colbag_getn_elements_1
#define Server_LOM_ColBag_InsertElements server_lom_colbag_insertelements_1
#define Server_LOM_ColBag_DeleteElements server_lom_colbag_deleteelements_1
#define Server_LOM_ColBag_DeleteAll server_lom_colbag_deleteall_1
#define Server_LOM_ColBag_IsMember server_lom_colbag_ismember_1
#define Server_LOM_ColBag_IsEqual server_lom_colbag_isequal_1
#define Server_LOM_ColBag_IsSubset server_lom_colbag_issubset_1
#define Server_LOM_ColBag_Union server_lom_colbag_union_1
#define Server_LOM_ColBag_Intersect server_lom_colbag_intersect_1
#define Server_LOM_ColBag_Difference server_lom_colbag_difference_1
#define Server_LOM_ColBag_UnionWith server_lom_colbag_unionwith_1
#define Server_LOM_ColBag_IntersectWith server_lom_colbag_intersectwith_1
#define Server_LOM_ColBag_DifferenceWith server_lom_colbag_differencewith_1
#define Server_LOM_ColBag_Scan_Open server_lom_colbag_scan_open_1
#define Server_LOM_ColBag_Scan_Close server_lom_colbag_scan_close_1
#define Server_LOM_ColBag_Scan_NextElements server_lom_colbag_scan_nextelements_1
#define Server_LOM_ColBag_Scan_GetSizeOfNextElements server_lom_colbag_scan_getsizeofnextelements_1
#define Server_LOM_ColBag_Scan_InsertElements server_lom_colbag_scan_insertelements_1
#define Server_LOM_ColBag_Scan_DeleteElements server_lom_colbag_scan_deleteelements_1

/* List-related interfaces */
#define Server_LOM_ColList_Create server_lom_collist_create_1
#define Server_LOM_ColList_Destroy server_lom_collist_destroy_1
#define Server_LOM_ColList_Assign  server_lom_collist_assign_1
#define Server_LOM_ColList_GetN_Elements server_lom_collist_getn_elements_1
#define Server_LOM_ColList_InsertElements server_lom_collist_insertelements_1
#define Server_LOM_ColList_DeleteElements server_lom_collist_deleteelements_1
#define Server_LOM_ColList_DeleteAll server_lom_collist_deleteall_1
#define Server_LOM_ColList_AppendElements server_lom_collist_appendelements_1
#define Server_LOM_ColList_RetrieveElements server_lom_collist_retrieveelements_1
#define Server_LOM_ColList_UpdateElements server_lom_collist_updateelements_1
#define Server_LOM_ColList_Concatenate server_lom_collist_concatenate_1
#define Server_LOM_ColList_Resize server_lom_collist_resize_1
#define Server_LOM_ColList_IsMember server_lom_collist_ismember_1
#define Server_LOM_ColList_IsEqual server_lom_collist_isequal_1
#define Server_LOM_ColList_Scan_Open server_lom_collist_scan_open_1
#define Server_LOM_ColList_Scan_Close server_lom_collist_scan_close_1
#define Server_LOM_ColList_Scan_NextElements server_lom_collist_scan_nextelements_1
#define Server_LOM_ColList_Scan_GetSizeOfNextElements server_lom_collist_scan_getsizeofnextelements_1
#define Server_LOM_ColList_Scan_InsertElements server_lom_collist_scan_insertelements_1
#define Server_LOM_ColList_Scan_DeleteElements server_lom_collist_scan_deleteelements_1


/* LOM ODMG Collection Interfaces */
#define Server_LOM_ODMG_Col_CreateData server_lom_odmg_col_createdata_1
#define Server_LOM_ODMG_Col_DestroyData server_lom_odmg_col_destroydata_1
#define Server_LOM_ODMG_Col_GetDescriptor server_lom_odmg_col_getdescriptor_1
#define Server_LOM_ODMG_Col_SetDescriptor server_lom_odmg_col_setdescriptor_1
#define Server_LOM_ODMG_Col_Assign server_lom_odmg_col_assign_1
#define Server_LOM_ODMG_Col_AssignElements server_lom_odmg_col_assignelements_1
#define Server_LOM_ODMG_Col_InsertElements server_lom_odmg_col_insertelements_1
#define Server_LOM_ODMG_Col_DeleteElements server_lom_odmg_col_deleteelements_1
#define Server_LOM_ODMG_Col_DeleteAll server_lom_odmg_col_deleteall_1
#define Server_LOM_ODMG_Col_IsMember server_lom_odmg_col_ismember_1
#define Server_LOM_ODMG_Col_IsEqual server_lom_odmg_col_isequal_1
#define Server_LOM_ODMG_Col_IsSubset server_lom_odmg_col_issubset_1
#define Server_LOM_ODMG_Col_Union server_lom_odmg_col_union_1
#define Server_LOM_ODMG_Col_Intersect server_lom_odmg_col_intersect_1
#define Server_LOM_ODMG_Col_Difference server_lom_odmg_col_difference_1
#define Server_LOM_ODMG_Col_UnionWith server_lom_odmg_col_unionwith_1
#define Server_LOM_ODMG_Col_IntersectWith server_lom_odmg_col_intersectwith_1
#define Server_LOM_ODMG_Col_DifferenceWith server_lom_odmg_col_differencewith_1
#define Server_LOM_ODMG_Col_AppendElements server_lom_odmg_col_appendelements_1
#define Server_LOM_ODMG_Col_RetrieveElements server_lom_odmg_col_retrieveelements_1
#define Server_LOM_ODMG_Col_UpdateElements server_lom_odmg_col_updateelements_1
#define Server_LOM_ODMG_Col_Concatenate server_lom_odmg_col_concatenate_1
#define Server_LOM_ODMG_Col_Resize server_lom_odmg_col_resize_1
#define Server_LOM_ODMG_Col_Scan_Open server_lom_odmg_col_scan_open_1
#define Server_LOM_ODMG_Col_Scan_Close server_lom_odmg_col_scan_close_1
#define Server_LOM_ODMG_Col_Scan_NextElements server_lom_odmg_col_scan_nextelements_1

#endif
