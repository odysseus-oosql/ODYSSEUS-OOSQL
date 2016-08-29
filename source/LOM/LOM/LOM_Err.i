

case eBADPARAMETER_LOM:
	return(" Bad Parameter (in LOM) ");

case eTOOMANYVOLS_LOM:
	return(" Too Many Volumes Mounted (in LOM) ");

case eTOOMANYOPENRELS_LOM:
	return(" Too Many Relations Opened (in LOM) ");

case eVOLUMENOTMOUNTED_LOM:
	return(" Volume Not Mounted (in LOM) ");

case eCATALOGNOTFOUND_LOM:
	return(" Can Not Found Catalog Table (in LOM) ");

case eRELATIONNOTFOUND_LOM:
	return(" Relation Not Found (in LOM) ");

case eINDEXNOTFOUND_LOM:
	return(" Index Not Found (in LOM) ");

case eNUMOFCOLUMNSMISMATCH_LOM:
	return(" Number of Columns Different (in LOM) ");

case eNUMOFINDEXESMISMATCH_LOM:
	return(" Number of Indexes Different (in LOM) ");

case eFETCHERROR_LOM:
	return(" Unexpected Fetch Length (in LOM) ");

case eMEMORYALLOCERR_LOM:
	return(" Memory Allocation Error (in LOM) ");

case eCOLUMNVALUEEXPECTED_LOM:
	return(" Column Value Expected (in LOM) ");

case eWRONGCOLUMNVALUE_LOM:
	return(" Wrong Column Value (in LOM) ");

case eTOOLARGELENGTH_LOM:
	return(" Too Large Length of Column Value (in LOM) ");

case eRELATIONDUPLICATED_LOM:
	return(" Relation Definitin Duplicated (in LOM) ");

case eINDEXDUPLICATED_LOM:
	return(" Index Definition Duplicated (in LOM) ");

case eINVALIDCURRENTTUPLE_LOM:
	return(" Current TupleID is Invalid (in LOM) ");

case eOPENEDRELATION_LOM:
	return(" Already Opened Relation (in LOM) ");

case eTOOLONGKEY_LOM:
	return(" Key Length Too Long (in LOM) ");

case eLOCKREQUESTFAIL_LOM:
	return(" Lock Request Failed (in LOM) ");

case eNOSUCHCLASS_LOM:
	return(" no such class in lom (in LOM)");

case eNOTALLOWDEDTYPE_LOM:
	return(" not allowded type in lom (in LOM)");
 
case eBADOIDRELATIONSHIPINSTANCE_LOM:
	return(" bad oid in relationship instance (in LOM)");

case eRELATIONSHIPSCANCARDINALITY_LOM:
	return(" cannot open relationship scan because of cardinality (in LOM)");

case eRELATIONSHIPSCANDIRECTION_LOM:
	return(" cannot open relationship scan because of direction (in LOM)");

case eBADATTRRELATIONSHIP_LOM:
	return(" attribute type and relationship cardinality are not match (in LOM)");

case eMEMORYFREEERR_LOM:
	return(" Memory Free Error (in LOM) ");

case eNOSUCHRELATIONSHIP_LOM:
	return(" no such relationship in lom (in LOM)");

case eMULTIFULINHERITANCE_LOM:
	return(" its not supported in GEOM (in GEOM)'");

case eINTERNAL_LOM:
	return(" Internal Error");

case eCANNOTUPDATECONTENTWHENINDEXISBUILT_LOM:
	return(" Cannot update content when index is built");

case eOUTOFMEMORY_LOM:
	return(" Out of memory");

case eBIGGERPOSTINGBUFFERNEEDED_LOM:
	return(" Insufficient memory buffer");

case eNOTIMPLEMENTED_LOM:
	return(" Not implemented yet");

case eNOTALLOWEDNULLTEXTCOLUMNCASEOFBATCHINDEXBUILD_LOM:
	return(" not allowed null-text column when batch index is built");

case eNOSUCHINDEXENTRY_LOM:
	return(" No such index built on the given class");

case eMOREMEMORYREQUIRED_LOM:
	return(" You shoud need more sufficient memory");

case eNOSUCHINDEX_LOM:
	return(" No such index defined on the given class");

case eCOUNTERDUPLICATED_LOM:
	return(" The given named counter is already created");

case eFAILTOOPENUNIXFILE_LOM:
	return(" fail to open unix file");

case eFAILTOCLOSEUNIXFILE_LOM:
	return(" fail to close unix file");

case eFAILTODESTROYUNIXFILE_LOM:
	return(" fail to delete unix file");

case eCONFIGURATION_LOM:
	return(" You must check your configuration. You should set several environment variables including O_SYSTEM_PATH");

case eNAMEDOBJECTDUPLICATED_LOM:
	return("the given name already exists");

case eNAMEDOBJECTNOTFOUND_LOM:
	return("No such named object found");

case eCONNECTIONALREADY_LOM:
	return("You have already connected the given host");

case eBADCONNECTION_LOM:
	return("bad connection between client and server");

case eFAILTOCONNECT_LOM:
	return("fail to connect to server");

case eNOTESTABLISHEDCONNECTION_LOM:
	return("You even didnot connect to server yet");

case eFAILTODISCONNECT_LOM:
	return("fail to disconnect from the server");

case eNOTYETIMPLEMENTED_LOM:
	return("not yet implemented");

case eSUBCLASSEXIST_LOM:
	return("Subclasses for the given class exist");

case eCHECKFILTER_LOM:
	return("Registered filter is not valid");

case eCHECKKEYWORDEXTRACTOR_LOM:
	return("Registered keyword extractor is not valid");

case eNOSUCHFILTEREXIST_LOM:
	return("No such filter exists");

case eNOSUCHKEYWORDEXTRACTOREXIST_LOM:
	return("No such keyword extractor exists");

case eCANNOTLINKFILTERFUNCTION_LOM:
	return("Fail to dynamically linking");

case eCANNOTCLOSETEXTHANDLE_LOM:
	return("Fail to close dynamic objects");

case eFAILTOSORT_LOM:
	return("Fail to sort file");

case eFAILTOCALLKEYWORDEXTRACTOR_LOM:
	return("Fail in calling keyword extractor");

case eNOSUCHSTEMIZEREXIST_LOM:
	return("No such stemizer exists");

case eOBJECTSALREADYEXIST_LOM:
	return("object already exist");

case eNULLCOLLECTION_LOM:
	return("The collection to apply operation is NULL");

case eNULLRELATIONSHIP_LOM:
	return("The relationship to scan is NULL");

case eOBJECTNOTFOUND_LOM:
	return("No such object found");

case eUNIXFILEREADERROR_LOM:
	return("Fail in reading unix file");

case eTEXTIR_NOTENABLED_LOM:
	return("Text infomation retrieval is not enabled");

