

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
    { eBADPARAMETER_EXPORT, "eBADPARAMETER_EXPORT", "Bad parameter" },
    { eCANNOT_OPEN_FILE_EXPORT, "eCANNOT_OPEN_FILE_EXPORT", "Cann't open file" },
    { eCANNOT_CLOSE_FILE_EXPORT, "eCANNOT_CLOSE_FILE_EXPORT", "Cann't close file" },
    { eFILE_WRITE_FAIL_EXPORT, "eFILE_WRITE_FAIL_EXPORT", "Failure occurs in file writing phase" },
    { eFILE_EXEC_FAIL_EXPORT, "eFILE_EXEC_FAIL_EXPORT", "Execution failure occur" },
    { eINSUFFICIENT_DISK_SPACE_EXPORT, "eINSUFFICIENT_DISK_SPACE_EXPORT", "Not enough disk space" },
    { eUNDEFINED_DATABASE_NAME_EXPORT, "eUNDEFINED_DATABASE_NAME_EXPORT", "Undefined database name" },
    { eUNDEFINED_TABLES_EXPORT, "eUNDEFINED_TABLES_EXPORT", "Undefined table" },
    { eUNHANDLED_CASE_EXPORT, "eUNHANDLED_CASE_EXPORT", "Unhandled case occur" },
    { eUNHANDLED_TYPE_EXPORT, "eUNHANDLED_TYPE_EXPORT", "Unhandled type occur" },
    { eGIVEN_INDEXNAME_INVALID_EXPORT, "eGIVEN_INDEXNAME_INVALID_EXPORT", "Given indexname is invalid in BYINDEX option" },
    { eINVALID_INDEX_EXPORT, "eINVALID_INDEX_EXPORT", "Invalid index used" },
    { eLRDS_ERROR_EXPORT, "eLRDS_ERROR_EXPORT", "LRDS Error" },
    { eLOM_ERROR_EXPORT, "eLOM_ERROR_EXPORT", "LOM Error" },
    { eCATALOG_ERROR_EXPORT, "eCATALOG_ERROR_EXPORT", "Catalog Error" },
    { eOOSQL_ERROR_EXPORT, "eOOSQL_ERROR_EXPORT", "OOSQL Error" },
    { eTOO_BIG_DATAFILESIZE, "eTOO_BIG_DATAFILESIZE", "Given data file size is too big" },
    { eTOO_BIG_INDEXFILESIZE, "eTOO_BIG_INDEXFILESIZE", "Given text index file size is too big" },
};


/*
 * Error Informations for all errors
 */
TOOL_Err_ErrInfo_T *tool_err_allErrInfo[] = {
    NULL,
    tool_err_infos_of_oosql_export_err_base,
};
