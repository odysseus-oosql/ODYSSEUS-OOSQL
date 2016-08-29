

/*
 * Error Base Informations
 */
TOOL_Err_ErrBaseInfo_T tool_err_errBaseInfo[] = {
    { "UNIX_ERROR_BASE", "unix errors", 0 },
    { "OOSQL_EXPORT_ERR_BASE", "OOSQL_Export error", NUM_ERRORS_OOSQL_EXPORT_ERR_BASE },
};


/*
 * Error Informations for OOSQL_EXPORT_ERR_BASE
 */
static TOOL_Err_ErrInfo_T tool_err_infos_of_oosql_export_err_base[] = {
    { eBADPARAMETER_IMPORT, "eBADPARAMETER_IMPORT", "Bad parameter" },
    { eCANNOT_OPEN_FILE_IMPORT, "eCANNOT_OPEN_FILE_IMPORT", "Cann't open file" },
    { eCANNOT_SEEK_FILE_IMPORT, "eCANNOT_SEEK_FILE_IMPORT", "Cann't seek file" },
    { eCANNOT_CLOSE_FILE_IMPORT, "eCANNOT_CLOSE_FILE_IMPORT", "Cann't close file" },
    { eLOGVOLUME_NOT_DEFINED_IMPORT, "eLOGVOLUME_NOT_DEFINED_IMPORT", "Log volume isn't defined" },
    { eFILE_READ_FAIL_IMPORT, "eFILE_READ_FAIL_IMPORT", "File read failure occur" },
    { eFILE_WRITE_FAIL_IMPORT, "eFILE_WRITE_FAIL_IMPORT", "File write failure occur" },
    { eFILE_EXEC_FAIL_IMPORT, "eFILE_EXEC_FAIL_IMPORT", "Execution failure occur" },
    { eUNDEFINED_DATABASE_NAME_IMPORT, "eUNDEFINED_DATABASE_NAME_IMPORT", "Undefined database name" },
    { eUNDEFINED_TABLES_IMPORT, "eUNDEFINED_TABLES_IMPORT", "Undefined table" },
    { eALREADY_EXIST_TABLE_IMPORT, "eALREADY_EXIST_TABLE_IMPORT", "Table is already exist in database" },
    { eUNHANDLED_CASE_IMPORT, "eUNHANDLED_CASE_IMPORT", "Unhandled case occur" },
    { eUNHANDLED_TYPE_IMPORT, "eUNHANDLED_TYPE_IMPORT", "Unhandled type occur" },
    { eNO_EXIST_DATA_IMPORT, "eNO_EXIST_DATA_IMPORT", "No Data file exist" },
    { eCORRUPT_DATA_FILE_IMPORT, "eCORRUPT_DATA_FILE_IMPORT", "Data file corrupted" },
    { eLRDS_ERROR_IMPORT, "eLRDS_ERROR_IMPORT", "LRDS Error" },
    { eLOM_ERROR_IMPORT, "eLOM_ERROR_IMPORT", "LOM Error" },
    { eCATALOG_ERROR_IMPORT, "eCATALOG_ERROR_IMPORT", "Catalog Error" },
    { eOOSQL_ERROR_IMPORT, "eOOSQL_ERROR_IMPORT", "OOSQL Error" },
};


/*
 * Error Informations for all errors
 */
TOOL_Err_ErrInfo_T *tool_err_allErrInfo[] = {
    NULL,
    tool_err_infos_of_oosql_export_err_base,
};
