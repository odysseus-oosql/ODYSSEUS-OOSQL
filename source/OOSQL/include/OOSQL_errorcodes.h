/*
 * Macro Definitions
 */
#define OOSQL_ERR_ENCODE_ERROR_CODE(base,no)        ( -1 * (((base) << 16) + no) )
#define OOSQL_ERR_GET_BASE_FROM_ERROR_CODE(code)    ( (((code) * -1) >> 16) & 0x0000FFFF )
#define OOSQL_ERR_GET_NO_FROM_ERROR_CODE(code)      ( ((code) * -1) & 0x0000FFFF )


/*
 * Error Base Definitions
 */
#define UNIX_ERR_BASE                  0
#define OOSQL_GENERAL_ERR_BASE         1
#define OOSQL_UTIL_ERR_BASE            2
#define OOSQL_RDSM_ERR_BASE            3
#define OOSQL_BFM_ERR_BASE             4
#define OOSQL_LOT_ERR_BASE             5
#define OOSQL_OM_ERR_BASE              6
#define OOSQL_BTM_ERR_BASE             7
#define OOSQL_SM_ERR_BASE              8
#define OOSQL_SHM_ERR_BASE             9
#define OOSQL_TM_ERR_BASE              10
#define OOSQL_LM_ERR_BASE              11
#define OOSQL_RM_ERR_BASE              12
#define OOSQL_LRDS_ERR_BASE            13
#define OOSQL_INTERNAL1_ERR_BASE       14
#define OOSQL_INTERNAL2_ERR_BASE       15
#define OOSQL_INTERNAL3_ERR_BASE       16
#define OOSQL_INTERNAL4_ERR_BASE       17
#define OOSQL_INTERNAL5_ERR_BASE       18
#define OOSQL_COMMON_ERR_BASE          19
#define OOSQL_COMPILER_ERR_BASE        20
#define OOSQL_EXECUTOR_ERR_BASE        21
#define OOSQL_STORAGEMANAGER_ERR_BASE  22
#define OOSQL_QPMM_ERR_BASE            23
#define OOSQL_API_ERR_BASE             24
#define OOSQL_SERVER_ERR_BASE          25
#define OOSQL_TOOL_UTIL_ERR_BASE       26
#define OOSQL_DBM_ERR_BASE             27
#define OOSQL_SLIMDOWN_ERR_BASE        28
#define OOSQL_CLIENT_ERR_BASE          29
#define OOSQL_SERVER_ERR_SOCKET_BASE   30
#define OOSQL_BROKER_ERR_BASE          31

#define OOSQL_NUM_OF_ERROR_BASES       32


/*
 * Error Definitions for OOSQL_GENERAL_ERR_BASE
 */
#define eINTERNAL_GENERAL_DUMMY        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_GENERAL_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_GENERAL_ERR_BASE 1


/*
 * Error Definitions for OOSQL_UTIL_ERR_BASE
 */
#define eINTERNAL_UTIL_DUMMY           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_UTIL_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_UTIL_ERR_BASE 1


/*
 * Error Definitions for OOSQL_RDSM_ERR_BASE
 */
#define eINTERNAL_RDSM_DUMMY           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_RDSM_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_RDSM_ERR_BASE 1


/*
 * Error Definitions for OOSQL_BFM_ERR_BASE
 */
#define eINTERNAL_BFM_DUMMY            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BFM_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_BFM_ERR_BASE  1


/*
 * Error Definitions for OOSQL_LOT_ERR_BASE
 */
#define eINTERNAL_LOT_DUMMY            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_LOT_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_LOT_ERR_BASE  1


/*
 * Error Definitions for OOSQL_OM_ERR_BASE
 */
#define eINTERNAL_OM_DUMMY             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_OM_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_OM_ERR_BASE   1


/*
 * Error Definitions for OOSQL_BTM_ERR_BASE
 */
#define eINTERNAL_BTM_DUMMY            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BTM_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_BTM_ERR_BASE  1


/*
 * Error Definitions for OOSQL_SM_ERR_BASE
 */
#define eINTERNAL_SM_DUMMY             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SM_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_SM_ERR_BASE   1


/*
 * Error Definitions for OOSQL_SHM_ERR_BASE
 */
#define eINTERNAL_SHM_DUMMY            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SHM_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_SHM_ERR_BASE  1


/*
 * Error Definitions for OOSQL_TM_ERR_BASE
 */
#define eINTERNAL_TM_DUMMY             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TM_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_TM_ERR_BASE   1


/*
 * Error Definitions for OOSQL_LM_ERR_BASE
 */
#define eINTERNAL_LM_DUMMY             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_LM_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_LM_ERR_BASE   1


/*
 * Error Definitions for OOSQL_RM_ERR_BASE
 */
#define eINTERNAL_RM_DUMMY             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_RM_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_RM_ERR_BASE   1


/*
 * Error Definitions for OOSQL_LRDS_ERR_BASE
 */
#define eINTERNAL_LRDS_DUMMY           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_LRDS_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_LRDS_ERR_BASE 1


/*
 * Error Definitions for OOSQL_INTERNAL1_ERR_BASE
 */
#define eINTERNAL1_DUMMY               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_INTERNAL1_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_INTERNAL1_ERR_BASE 1


/*
 * Error Definitions for OOSQL_INTERNAL2_ERR_BASE
 */
#define eINTERNAL2_DUMMY               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_INTERNAL2_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_INTERNAL2_ERR_BASE 1


/*
 * Error Definitions for OOSQL_INTERNAL3_ERR_BASE
 */
#define eINTERNAL3_DUMMY               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_INTERNAL3_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_INTERNAL3_ERR_BASE 1


/*
 * Error Definitions for OOSQL_INTERNAL4_ERR_BASE
 */
#define eINTERNAL4_DUMMY               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_INTERNAL4_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_INTERNAL4_ERR_BASE 1


/*
 * Error Definitions for OOSQL_INTERNAL5_ERR_BASE
 */
#define eINTERNAL5_DUMMY               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_INTERNAL5_ERR_BASE,0)
#define NUM_ERRORS_OOSQL_INTERNAL5_ERR_BASE 1


/*
 * Error Definitions for OOSQL_COMMON_ERR_BASE
 */
#define eBADPARAMETER_OOSQL            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMMON_ERR_BASE,0)
#define eNOTIMPLEMENTED_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMMON_ERR_BASE,1)
#define eUNHANDLED_CASE_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMMON_ERR_BASE,2)
#define eINVALID_PATHEXPR_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMMON_ERR_BASE,3)
#define eINTERNALERROR_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMMON_ERR_BASE,4)
#define eVOLUMELOCKBLOCK_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMMON_ERR_BASE,5)
#define eDEADLOCK_OOSQL                OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMMON_ERR_BASE,6)
#define eINVALIDLICENSE_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMMON_ERR_BASE,7)
#define NUM_ERRORS_OOSQL_COMMON_ERR_BASE 8


/*
 * Error Definitions for OOSQL_COMPILER_ERR_BASE
 */
#define eSYNTAX_ERROR_OOSQL            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,0)
#define eINVALIDSELECT_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,1)
#define ePARSE_ERROR_OOSQL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,2)
#define eINVALIDAST_OOSQL              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,3)
#define eMEMORYALLOCERR_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,4)
#define eBADASTNODE_OOSQL              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,5)
#define eTYPE_ERROR_OOSQL              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,6)
#define eNOTFOUND_DICTIONARY           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,7)
#define eCANTSHRINKMORE_NAMESTACK      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,8)
#define eMUSTSPECIFYFROM_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,9)
#define eHAVINGWITHOUTGROUPBY_OOSQL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,10)
#define eEXPRERR_HAVING_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,11)
#define eTARGETNOTPERSISTENT_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,12)
#define eSEQUENCENOTDEFINED_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,13)
#define eSEQUENCEALREADYDEFINED_OOSQL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,14)
#define eNOTMATCHMETHODPARAMETER_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,15)
#define eCOLLECTIONELEMENT_ERROR_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,16)
#define eFUNCTIONPARAMETER_ERROR_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,17)
#define eCLASSNOTDEFINED_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,18)
#define eATTRNOTDEFINED_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,19)
#define eATTRALREADYDEFINED_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,20)
#define eCANTADDCOLUMN_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,21)
#define eCANTALTERTABLE_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,22)
#define eCANTDROPCOLUMN_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,23)
#define eCLASSNOTINTARGETLIST_OOSQL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,24)
#define eSTRINGSIZE_OVERFLOW_SIMPLESTRING OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,25)
#define eBOUNDARY_OVERFLOW_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,26)
#define eNULLPOINTACCESS_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,27)
#define eNOT_PATHEXPR_ID_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,28)
#define eBINDING_AMBIGUITY_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,29)
#define eNOT_FOUND_OOSQL               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,30)
#define eDUPLICATED_KEY_DICTIONARY     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,31)
#define eSAME_ALIASNAME_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,32)
#define eEXPRERR_WHERE_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,33)
#define eAGGR_AND_PATH_IN_SELECT_WITH_NO_GROUPBY OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,34)
#define eAGGRFUNC_ARGUMENT_ERR_OOSQL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,35)
#define eWEIGHT_FUNCTION_ERR_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,36)
#define eEXT_BOOL_EXPR_ERR_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,37)
#define eSTRINGBUFFER_OVERFLOW_OOSQL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,38)
#define eNOMATCHING_COLUMN_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,39)
#define eNOMATCHING_COLUMNLIST_VALUELIST_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,40)
#define eMOREVALUELIST_THAN_COLUMNS_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,41)
#define eDUPLCATED_CLASS_DEFINITION_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,42)
#define eNOSUCH_SUPERCLASS_EXIST_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,43)
#define eNOTSUPPORTED_ATTR_TYPE_OOSQL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,44)
#define eUNDEFINED_FUNCTIONID_USED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,45)
#define eBAD_DATE_STRING_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,46)
#define eBAD_TIME_STRING_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,47)
#define eBAD_TEXT_DOMAIN_OPTION_OOSQL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,48)
#define eUNSUPPORTED_LANGUAGE_OOSQL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,49)
#define eUNSUPPORTED_PARAMETER_STYLE_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,50)
#define eUNSUPPORTED_PROCEDURE_PARAMETER_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,51)
#define eUNDEFINED_FUNCTION_USED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,52)
#define eGETSTEMIZER_ERROR_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,53)
#define eNO_MATCHING_PATH_IN_SELECT_WITH_GROUPBY OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,54)
#define eUNSUPPORTED_SCANDIRECTION_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,55)
#define eNOTENABLED_TEMPORARYTABLE_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,56)
#define eTOOMANYATTR_OOSQL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,57)
#define eTOOLONGID_OOSQL               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,58)
#define eTOOMANYATTRINDEX_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_COMPILER_ERR_BASE,59)
#define NUM_ERRORS_OOSQL_COMPILER_ERR_BASE 60


/*
 * Error Definitions for OOSQL_EXECUTOR_ERR_BASE
 */
#define eOUTOFMEMORY_OOSQL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,0)
#define eINVALID_OOSQLHANDLE_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,1)
#define eNEGATIVE_ELEMSIZE_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,2)
#define eINSUFFICIENT_BUFSIZE_OOSQL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,3)
#define eINSUFFICIENT_RESINFOSIZE_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,4)
#define eUNINITIALIZED_STRUCT_OOSQL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,5)
#define eNOTREADY_OOSQL                OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,6)
#define ePREPARATION_FAILED_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,7)
#define eNOTCOMPILED_OOSQL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,8)
#define eFETCHEDALL_OOSQL              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,9)
#define eNOTIMPLEMENTED_BACKWARDTRAVERSAL_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,10)
#define eNOTIMPLEMENTED_SORTMERGE_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,11)
#define eNOTIMPLEMENTED_HASHJOIN_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,12)
#define eNOTIMPLEMENTED_ABNORMALPATHEXPR_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,13)
#define eNOTIMPLEMENTED_SETOP_OOSQL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,14)
#define eNOTIMPLEMENTED_ARITHOP_OOSQL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,15)
#define eNOTIMPLEMENTED_METHOD_OOSQL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,16)
#define eNOTIMPLEMENTED_CONSEXPR_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,17)
#define eNOTIMPLEMENTED_FETCHPREV_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,18)
#define eNOTIMPLEMENTED_FETCHLAST_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,19)
#define eNOTIMPLEMENTED_OBJECTPROJ_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,20)
#define eNOTIMPLEMENTED_FUNCPROJ_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,21)
#define eNOTIMPLEMENTED_BOOL_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,22)
#define eNOTIMPLEMENTED_NIL_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,23)
#define eNOTIMPLEMENTED_MBR_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,24)
#define eNOTIMPLEMENTED_ARGUMENT_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,25)
#define eNOTIMPLEMENTED_VALUEPROJ_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,26)
#define eNOTIMPLEMENTED_EXPRPROJ_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,27)
#define eNOTIMPLEMENTED_TEXTINDEXOR_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,28)
#define eNOTIMPLEMENTED_BTREEINDEX_ANDOR_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,29)
#define eNOTIMPLEMENTED_MLGFINDEXSCAN_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,30)
#define eNOTIMPLEMENTED_OIDSETSCAN_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,31)
#define eINVALID_CNF_OOSQL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,32)
#define eILLEGAL_OP_OOSQL              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,33)
#define eINCOMPATIBLE_TYPE_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,34)
#define eINVALID_EXPRESSION_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,35)
#define eINVALID_VALUE_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,36)
#define eINVALID_COLTYPE_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,37)
#define eINVALID_OPERAND_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,38)
#define eDANGLINGREFERENCE_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,39)
#define eSCAN_NOTFOUND_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,40)
#define eOCN_NOTFOUND_OOSQL            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,41)
#define eQPMM_INIT_FAILED_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,42)
#define eQPMM_REINIT_FAILED_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,43)
#define eQPMM_FINAL_FAILED_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,44)
#define eINVALID_CLASSKIND_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,45)
#define eINVALID_ACCESSMETHOD_OOSQL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,46)
#define eINVALID_PROJECTION_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,47)
#define eINVALID_PLANNO_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,48)
#define eINVALID_COLNO_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,49)
#define eINVALID_METHODNO_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,50)
#define eINVALID_POOLINDEX_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,51)
#define eINVALID_AGGRFUNC_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,52)
#define eNULL_ACCESSPLAN_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,53)
#define eINVALID_JOINMETHOD_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,54)
#define eINCOMPATIBLE_BOOLCONST_OOSQL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,55)
#define eINVALID_BOOLEXP_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,56)
#define eCLASS_NOTOPENED_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,57)
#define eSCANOPEN_FAILED_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,58)
#define eINTERNAL_INVALIDSTATUS_OOSQL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,59)
#define eINTERNAL_INCORRECTEXECSEQUENCE_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,60)
#define eINVALID_INDEXINFONODE_OOSQL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,61)
#define eINVALID_TEMPFILENUM_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,62)
#define eTEMPFILE_ALREADYCREATED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,63)
#define eTEMPFILE_NOTCREATED_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,64)
#define eTEMPFILE_SAVEDALL_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,65)
#define eBUFFER_ALREADYFREED_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,66)
#define eINCONSISTENT_ACCESSPLAN_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,67)
#define eINVALID_OPERATOR_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,68)
#define eINVALID_CONTROLSTATUS_OOSQL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,69)
#define eINFANTBUFFER_EMPTY_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,70)
#define eINFANTBUFFER_FULL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,71)
#define eINVALID_SCANID_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,72)
#define eSCANALREADYOPENED_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,73)
#define eSCANALREADYCLOSED_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,74)
#define eSORT_NOT_PREPARED_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,75)
#define eNULL_POINTER_OOSQL            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,76)
#define eINVALID_FUNC_OOSQL            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,77)
#define eINVALID_CASE_OOSQL            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,78)
#define ePOSTINGBUFFER_EMPTY_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,79)
#define eTEXTIR_EVALBUF_ALREADYMARKED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,80)
#define eNEEDMORESORTBUFFERMEMORY_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,81)
#define eNORESULTS_OOSQL               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,82)
#define eNOSUCH_FUNCTION_EXIST_OOSQL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,83)
#define eNOSUCH_PROCEDURE_EXIST_OOSQL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,84)
#define eCANT_EXECUTE_GETOID_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,85)
#define eINVALID_API_CALL_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,86)
#define eMULTIPLERESULTBUFFER_FULL_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,87)
#define eDUPLICATED_ELEMENTS_ARE_INSERTED_SET_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,88)
#define eINVALID_OID_STRING            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_EXECUTOR_ERR_BASE,89)
#define NUM_ERRORS_OOSQL_EXECUTOR_ERR_BASE 90


/*
 * Error Definitions for OOSQL_STORAGEMANAGER_ERR_BASE
 */
#define eCLASSDUPLICATED_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,0)
#define eINDEXNOTDEFINED_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,1)
#define eINDEXDUPLICATED_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,2)
#define eDEVICEOPENFAIL_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,3)
#define eEXTERNALFUNCTION_DLCLOSE_FAIL_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,4)
#define eEXTERNALFUNCTION_DLOPEN_FAIL_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,5)
#define eEXTERNALFUNCTION_DLSYM_FAIL_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,6)
#define eTOO_MANY_PARAMS_IN_FUNC_CALL_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,7)
#define eCANT_EXECUTE_MSG_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,8)
#define ePIPE_ALREADY_OPENED_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,9)
#define ePIPE_ALREADY_CLOSED_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,10)
#define ePIPE_CLOSE_ERROR_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,11)
#define ePIPE_OPEN_ERROR_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,12)
#define ePIPE_READ_ERROR_OOSQL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,13)
#define ePIPE_WRITE_ERROR_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,14)
#define eCREATE_PROCESS_ERROR_OOSQL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,15)
#define ePIPE_BAD_MESSAGE_OOSQL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,16)
#define eBIGGERPOSTINGBUFFERNEEDED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,17)
#define eLARGETEMPOBJECT_ALREADY_CREATED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,18)
#define eLARGETEMPOBJECT_NOT_CREATED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,19)
#define eRESIZE_MEMORY_BLOCK_FAIL_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,20)
#define eBTREE_KEYLENGTH_EXCESS_OOSQL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_STORAGEMANAGER_ERR_BASE,21)
#define NUM_ERRORS_OOSQL_STORAGEMANAGER_ERR_BASE 22


/*
 * Error Definitions for OOSQL_QPMM_ERR_BASE
 */
#define eBADPARAMETER_QPMM             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,0)
#define eCANNOTINIT_QPMM               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,1)
#define eCANNOTDOUBLE_QPMM             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,2)
#define eFAILTOREPLACEMENT_QPMM        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,3)
#define eNOTVALIDADDRESS_QPMM          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,4)
#define eSHORTOFMEMORY_QPMM            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,5)
#define eALREAYEXISTINCATAHASH_QPMM    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,6)
#define eINVALIDCATALOG_QPMM           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,7)
#define eINSERTFAIL_QPMM               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,8)
#define eSEARCHFAIL_QPMM               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,9)
#define eINTERNAL_QPMM                 OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_QPMM_ERR_BASE,10)
#define NUM_ERRORS_OOSQL_QPMM_ERR_BASE 11


/*
 * Error Definitions for OOSQL_API_ERR_BASE
 */
#define eOUTOFHANDLE_OOSQL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,0)
#define eBADQUERYSTRING_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,1)
#define eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,2)
#define eDATABASE_ALREADY_MOUNTED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,3)
#define eNO_SUCH_VOLUME_MOUNTED_OOSQL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,4)
#define eTOOMANYVOLS_OOSQL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,5)
#define eVOLUME_TITLE_TOO_LONG_OOSQL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,6)
#define eDATABASENAME_TOO_LONG_OOSQL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,7)
#define eNOTESTABLISHEDCONNECTION_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,8)
#define eNOTTEXTCOLUMN_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,9)
#define eNULLCOLLECTION_OOSQL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,10)
#define eNOTNULLCOLLECTION_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,11)
#define eFAILTOCONNECT_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,12)
#define eBADCONNECTION_OOSQL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_API_ERR_BASE,13)
#define NUM_ERRORS_OOSQL_API_ERR_BASE  14


/*
 * Error Definitions for OOSQL_SERVER_ERR_BASE
 */
#define eNOTYETIMPLEMENTED_SERVER      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_BASE,0)
#define eINVALIDENVIRONMENT_SERVER     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_BASE,1)
#define eOUTOFMEMORY_SERVER            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_BASE,2)
#define eINTERNAL_SERVER               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_BASE,3)
#define NUM_ERRORS_OOSQL_SERVER_ERR_BASE 4


/*
 * Error Definitions for OOSQL_TOOL_UTIL_ERR_BASE
 */
#define eNOLOGICALIDINDEX_UTIL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,0)
#define eOUTOFMEMORY_UTIL              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,1)
#define eNOBYTEOFFSETINFO_UTIL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,2)
#define eBADPOSTINGFORMAT_UTIL         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,3)
#define eBADCONTENTFILEFORMAT_UTIL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,4)
#define eDATAINCONSISTENCY_UTIL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,5)
#define eWRITEFILEFAIL_UTIL            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,6)
#define eTOOLARGELOGICALID_UTIL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,7)
#define eNOMAPPINGEXIST_UTIL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,8)
#define eNOCLASSINFO_UTIL              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,9)
#define eNOATTRINFO_UTIL               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,10)
#define eNORIGHTPARENTHESIS_UTIL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,11)
#define eUNIXFILEOPENERROR_UTIL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,12)
#define eUNKNOWNCOMMAND_UTIL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,13)
#define eILLEGALUSEOFBACKSLASH_UTIL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,14)
#define eILLEGALUSEOFDOUBLEQUOTE_UTIL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,15)
#define eILLEGALUSEOFSINGLEQUOTE_UTIL  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,16)
#define eILLEGALUSEOFRIGHTBRACE_UTIL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,17)
#define eILLEGALUSEOFLEFTBRACE_UTIL    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,18)
#define eINCONSISTENTDATATYPE_UTIL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,19)
#define eTEMPDIRNOTDEFINED_UTIL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,20)
#define eUNIXSTATFAIL_UTIL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,21)
#define eUNIXFILEREADERROR_UTIL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,22)
#define eSETNOTIMPLEMENTED_UTIL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,23)
#define eINVALIDPOSTING_UTIL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,24)
#define eUNIXFILEWRITEERROR_UTIL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,25)
#define eNOSUCHATTRIBUTE_UTIL          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,26)
#define eBADPARAMETER_UTIL             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,27)
#define eINTERNAL_ERROR_UTIL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,28)
#define eUNHANDLED_CASE_UTIL           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,29)
#define eTOOLARGENPOSOTION_UTIL        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_TOOL_UTIL_ERR_BASE,30)
#define NUM_ERRORS_OOSQL_TOOL_UTIL_ERR_BASE 31


/*
 * Error Definitions for OOSQL_DBM_ERR_BASE
 */
#define eBADPARAMETER_DBM              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,0)
#define eCANNOTOPENFILE_DBM            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,1)
#define eCANNOTLOCKFILE_DBM            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,2)
#define eCANNOTUNLOCKFILE_DBM          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,3)
#define eBADFILEFORMAT_DBM             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,4)
#define eMEMORYALLOCERROR_DBM          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,5)
#define eCANNOTWRITEFILE_DBM           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,6)
#define eCANNOTREMOVEFILE_DBM          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,7)
#define eNOSUCHDB_DBM                  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,8)
#define eNOSUCHVOLUME_DBM              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,9)
#define eCANNOTCREATEFILE_DBM          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,10)
#define eBACKUPFILEEXIST_DBM           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,11)
#define eNOSUCHDEVICE_DBM              OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,12)
#define eDBNAMEDUPLICATED_DBM          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,13)
#define eBADVOLUMEID_DBM               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,14)
#define eVOLUMENAMEDUPLICATED_DBM      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,15)
#define eVOLUMEIDDUPLICATED_DBM        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,16)
#define eBADDEVICEID_DBM               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,17)
#define eDEVICENAMEDUPLICATED_DBM      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,18)
#define eDEVICEIDDUPLICATED_DBM        OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,19)
#define eNOVOLUMEEXISTINDB_DBM         OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,20)
#define eEXECERROR_DBM                 OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,21)
#define eNEEDMOREBUFFER_DBM            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,22)
#define eSMALLBUFFER_DBM               OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,23)
#define eINVALIDOPTION_DBM             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,24)
#define eENVNOTDEFINED_DBM             OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,25)
#define eCANNOTRENAMEFILE_DBM          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_DBM_ERR_BASE,26)
#define NUM_ERRORS_OOSQL_DBM_ERR_BASE  27


/*
 * Error Definitions for OOSQL_SLIMDOWN_ERR_BASE
 */
#define eTEXTIR_NOTENABLED_OOSQL       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SLIMDOWN_ERR_BASE,0)
#define eBULKLOAD_NOTENABLED_OOSQL     OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SLIMDOWN_ERR_BASE,1)
#define eBULKDELETE_NOTENABLED_OOSQL   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SLIMDOWN_ERR_BASE,2)
#define eCOLLECTIONTYPE_NOTENABLED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SLIMDOWN_ERR_BASE,3)
#define eRELATIONSHIP_NOTENABLED_OOSQL OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SLIMDOWN_ERR_BASE,4)
#define eOPENGIS_NOTENABLED_OOSQL      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SLIMDOWN_ERR_BASE,5)
#define NUM_ERRORS_OOSQL_SLIMDOWN_ERR_BASE 6


/*
 * Error Definitions for OOSQL_CLIENT_ERR_BASE
 */
#define eBADPARAMETER_CLIENT           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,0)
#define eOUTOFMEMORY_CLIENT            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,1)
#define eFAILTOCONNECTTOBROKER_CLIENT  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,2)
#define eFAILTOCONNECTTOSERVER_CLIENT  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,3)
#define eFAILTODISCONNECTFROMBROKER_CLIENT OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,4)
#define eFAILTODISCONNECFROMSERVER_CLIENT OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,5)
#define eFAILTOENCODEMESSAGE_CLIENT    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,6)
#define eFAILTODECODEMESSAGE_CLIENT    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,7)
#define eFAILTOCONNECT_CLIENT          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,8)
#define eNOTESTABLISHEDCONNECTION_CLIENT OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,9)
#define eTIMEOUTCALLSERVERFUNCTION_CLIENT OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,10)
#define eUNDEFINEDMESSAGE_CLIENT       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,11)
#define eMISMATCHEDMESSAGE_CLIENT      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,12)
#define eFAILTOSENDMESSAGE_CLIENT      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,13)
#define eFAILTORECEIVEMESSAGE_CLIENT   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_CLIENT_ERR_BASE,14)
#define NUM_ERRORS_OOSQL_CLIENT_ERR_BASE 15


/*
 * Error Definitions for OOSQL_SERVER_ERR_SOCKET_BASE
 */
#define eBADPARAMETER_SERVER           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,0)
#define eOUTOFMEMORY_SERVER            OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,1)
#define eFAILTOENCODEMESSAGE_SERVER    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,2)
#define eFAILTODECODEMESSAGE_SERVER    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,3)
#define eFAILTOCONNECTTOBROKER_SERVER  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,4)
#define eFAILTODISCONNECTFROMBROKER_SERVER OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,5)
#define eNOTESTABLISHEDCONNECTION_SERVER OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,6)
#define eUNDEFINEDMESSAGE_SERVER       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,7)
#define eMISMATCHEDMESSAGE_SERVER      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,8)
#define eFAILTOSENDMESSAGE_SERVER      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,9)
#define eFAILTORECEIVEMESSAGE_SERVER   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_SERVER_ERR_SOCKET_BASE,10)
#define NUM_ERRORS_OOSQL_SERVER_ERR_SOCKET_BASE 11


/*
 * Error Definitions for OOSQL_BROKER_ERR_BASE
 */
#define eBADPARAMETER_BROKER           OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,0)
#define eCONFIGURATION_BROKER          OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,1)
#define eCANNOTFORKPROCESS_BROKER      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,2)
#define eCANTNOTOPENSERVERSETUPFILE_BROKER OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,3)
#define eFAILTOCONNECTTOSERVER_BROKER  OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,4)
#define eFAILTODISCONNECTFROMSERVER_BROKER OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,5)
#define eNOTESTABLISHEDCONNECTION_BROKER OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,6)
#define eUNDEFINEDMESSAGE_BROKER       OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,7)
#define eMISMATCHEDMESSAGE_BROKER      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,8)
#define eFAILTOSENDMESSAGE_BROKER      OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,9)
#define eFAILTORECEIVEMESSAGE_BROKER   OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,10)
#define eFAILTOENCODEMESSAGE_BROKER    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,11)
#define eFAILTODECODEMESSAGE_BROKER    OOSQL_ERR_ENCODE_ERROR_CODE(OOSQL_BROKER_ERR_BASE,12)
#define NUM_ERRORS_OOSQL_BROKER_ERR_BASE 13