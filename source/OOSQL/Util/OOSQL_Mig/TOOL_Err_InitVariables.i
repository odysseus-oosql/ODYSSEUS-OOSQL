

/*
 * Error Base Informations
 */
TOOL_Err_ErrBaseInfo_T tool_err_errBaseInfo[] = {
    { "UNIX_ERROR_BASE", "unix errors", 0 },
    { "OOSQL_MIG_ERR_BASE", "OOSQL_Mig error", NUM_ERRORS_OOSQL_MIG_ERR_BASE },
};


/*
 * Error Informations for OOSQL_MIG_ERR_BASE
 */
static TOOL_Err_ErrInfo_T tool_err_infos_of_oosql_mig_err_base[] = {
    { eOOSQL_ERROR_MIG, "eOOSQL_ERROR_MIG", "OOSQL Error" },
    { eBADPARAMETER_MIG, "eBADPARAMETER_MIG", "Bad parameter" },
    { eCANNOT_OPEN_FILE_MIG, "eCANNOT_OPEN_FILE_MIG", "Cann't open file" },
    { eCANNOT_CLOSE_FILE_MIG, "eCANNOT_CLOSE_FILE_MIG", "Cann't close file" },
    { eUNDEFINED_DATABASE_NAME_MIG, "eUNDEFINED_DATABASE_NAME_MIG", "Undefined database name" },
    { eUNHANDLED_CASE_MIG, "eUNHANDLED_CASE_MIG", "Unhandled case occur" },
    { eLRDS_ERROR_MIG, "eLRDS_ERROR_MIG", "LRDS Error" },
    { eLOM_ERROR_MIG, "eLOM_ERROR_MIG", "LOM Error" },
    { eCATALOG_ERROR_MIG, "eCATALOG_ERROR_MIG", "Catalog Error" },
};


/*
 * Error Informations for all errors
 */
TOOL_Err_ErrInfo_T *tool_err_allErrInfo[] = {
    NULL,
    tool_err_infos_of_oosql_mig_err_base,
};
