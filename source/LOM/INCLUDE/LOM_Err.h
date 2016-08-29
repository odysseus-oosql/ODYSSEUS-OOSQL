
#define   eBADPARAMETER_LOM  -7000	  /*  Bad Parameter (in LOM)  */
#define   eTOOMANYVOLS_LOM  -7001	  /*  Too Many Volumes Mounted (in LOM)  */
#define   eTOOMANYOPENRELS_LOM  -7002	  /*  Too Many Relations Opened (in LOM)  */
#define   eVOLUMENOTMOUNTED_LOM  -7003	  /*  Volume Not Mounted (in LOM)  */
#define   eCATALOGNOTFOUND_LOM  -7004	  /*  Can Not Found Catalog Table (in LOM)  */
#define   eRELATIONNOTFOUND_LOM  -7005	  /*  Relation Not Found (in LOM)  */
#define   eINDEXNOTFOUND_LOM  -7006	  /*  Index Not Found (in LOM)  */
#define   eNUMOFCOLUMNSMISMATCH_LOM  -7007	  /*  Number of Columns Different (in LOM)  */
#define   eNUMOFINDEXESMISMATCH_LOM  -7008	  /*  Number of Indexes Different (in LOM)  */
#define   eFETCHERROR_LOM  -7009	  /*  Unexpected Fetch Length (in LOM)  */
#define   eMEMORYALLOCERR_LOM  -7010	  /*  Memory Allocation Error (in LOM)  */
#define   eCOLUMNVALUEEXPECTED_LOM  -7011	  /*  Column Value Expected (in LOM)  */
#define   eWRONGCOLUMNVALUE_LOM  -7012	  /*  Wrong Column Value (in LOM)  */
#define   eTOOLARGELENGTH_LOM  -7013	  /*  Too Large Length of Column Value (in LOM)  */
#define   eRELATIONDUPLICATED_LOM  -7014	  /*  Relation Definitin Duplicated (in LOM)  */
#define   eINDEXDUPLICATED_LOM  -7015	  /*  Index Definition Duplicated (in LOM)  */
#define   eINVALIDCURRENTTUPLE_LOM  -7016	  /*  Current TupleID is Invalid (in LOM)  */
#define   eOPENEDRELATION_LOM  -7017	  /*  Already Opened Relation (in LOM)  */
#define   eTOOLONGKEY_LOM  -7018	  /*  Key Length Too Long (in LOM)  */
#define   eLOCKREQUESTFAIL_LOM  -7019	  /*  Lock Request Failed (in LOM)  */
#define   eNOSUCHCLASS_LOM  -7020	  /*  no such class in lom (in LOM) */
#define   eNOTALLOWDEDTYPE_LOM  -7021   /*  not allowded type in lom (in LOM) */ 
#define   eBADOIDRELATIONSHIPINSTANCE_LOM  -7022	  /*  bad oid in relationship instance (in LOM) */
#define   eRELATIONSHIPSCANCARDINALITY_LOM  -7023   /*  cannot open relationship scan because of cardinality (in LOM) */
#define   eRELATIONSHIPSCANDIRECTION_LOM  -7024   /*  cannot open relationship scan because of direction (in LOM) */
#define   eBADATTRRELATIONSHIP_LOM  -7025   /*  attribute type and relationship cardinality are not match (in LOM) */
#define   eMEMORYFREEERR_LOM  -7026	  /*  Memory Free Error (in LOM)  */
#define   eNOSUCHRELATIONSHIP_LOM  -7027	  /*  no such relationship in lom (in LOM) */
#define   eMULTIFULINHERITANCE_LOM  -7028	  /*  its not supported in GEOM (in GEOM)' */
#define   eINTERNAL_LOM  -7029	  /*  Internal Error */
#define   eCANNOTUPDATECONTENTWHENINDEXISBUILT_LOM  -7030	  /*  Cannot update content when index is built */
#define   eOUTOFMEMORY_LOM  -7031	  /*  Out of memory */
#define   eBIGGERPOSTINGBUFFERNEEDED_LOM  -7032	  /*  Insufficient memory buffer */
#define   eNOTIMPLEMENTED_LOM  -7033	  /*  Not implemented yet */
#define   eNOTALLOWEDNULLTEXTCOLUMNCASEOFBATCHINDEXBUILD_LOM  -7034	  /*  not allowed null-text column when batch index is built */
#define   eNOSUCHINDEXENTRY_LOM  -7035	  /*  No such index built on the given class */
#define   eMOREMEMORYREQUIRED_LOM  -7036	  /*  You shoud need more sufficient memory */
#define   eNOSUCHINDEX_LOM  -7037	  /*  No such index defined on the given class */
#define   eCOUNTERDUPLICATED_LOM  -7038	  /*  The given named counter is already created */
#define   eFAILTOOPENUNIXFILE_LOM  -7039	  /*  fail to open unix file */
#define   eFAILTOCLOSEUNIXFILE_LOM  -7040	  /*  fail to close unix file */
#define   eFAILTODESTROYUNIXFILE_LOM  -7041	  /*  fail to delete unix file */
#define   eCONFIGURATION_LOM  -7042	  /*  You must check your configuration. You should set several environment variables including O_SYSTEM_PATH */
#define   eNAMEDOBJECTDUPLICATED_LOM  -7043  /* the given name already exists */
#define   eNAMEDOBJECTNOTFOUND_LOM  -7044  /* No such named object found */
#define   eCONNECTIONALREADY_LOM  -7045  /* You have already connected the given host */
#define   eBADCONNECTION_LOM  -7046  /* bad connection between client and server */
#define   eFAILTOCONNECT_LOM  -7047  /* fail to connect to server */
#define   eNOTESTABLISHEDCONNECTION_LOM  -7048  /* You even didnot connect to server yet */
#define   eFAILTODISCONNECT_LOM  -7049  /* fail to disconnect from the server */
#define   eNOTYETIMPLEMENTED_LOM  -7050  /* not yet implemented */
#define   eSUBCLASSEXIST_LOM  -7052  /* Subclasses for the given class exist */
#define   eCHECKFILTER_LOM  -7053  /* Registered filter is not valid */
#define   eCHECKKEYWORDEXTRACTOR_LOM  -7054  /* Registered keyword extractor is not valid */
#define   eNOSUCHFILTEREXIST_LOM  -7055  /* No such filter exists */
#define   eNOSUCHKEYWORDEXTRACTOREXIST_LOM  -7056  /* No such keyword extractor exists */
#define   eCANNOTLINKFILTERFUNCTION_LOM  -7057  /* Fail to dynamically linking */
#define   eCANNOTCLOSETEXTHANDLE_LOM  -7058  /* Fail to close dynamic objects */
#define   eFAILTOSORT_LOM  -7059  /* Fail to sort file */
#define   eFAILTOCALLKEYWORDEXTRACTOR_LOM  -7060  /* Fail in calling keyword extractor */
#define   eNOSUCHSTEMIZEREXIST_LOM  -7061  /* No such stemizer exists */
#define   eOBJECTSALREADYEXIST_LOM  -7062  /* object already exist */
#define   eNULLCOLLECTION_LOM  -7063  /* The collection to apply operation is NULL */
#define   eNULLRELATIONSHIP_LOM  -7064  /* The relationship to scan is NULL */
#define   eOBJECTNOTFOUND_LOM  -7065  /* No such object found */
#define   eUNIXFILEREADERROR_LOM  -7066  /* Fail in reading unix file */
#define   eTEXTIR_NOTENABLED_LOM  -7067  /* Text infomation retrieval is not enabled */
