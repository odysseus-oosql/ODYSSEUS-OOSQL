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


/* Index Descriptror conversion */
Four lom_ConvertIndexDesc(
	LOM_Handle *handle, 
	LOM_IndexDesc *lom_idesc,
	Server_IndexDesc *rpc_idesc,
	Two directionFlag
)
{
	Four i;		/* index variable */

	/* check directionality */
	if(directionFlag == LOM_TO_RPC) {
		if(lom_idesc->indexType == SM_INDEXTYPE_BTREE) {
			rpc_idesc->indexType = CS_BTREE;
			rpc_idesc->Server_IndexDesc_u.btree.flag = lom_idesc->kinfo.btree.flag;
			rpc_idesc->Server_IndexDesc_u.btree.nColumns = lom_idesc->kinfo.btree.nColumns;
			if(rpc_idesc->Server_IndexDesc_u.btree.nColumns > CS_MAXNUMKEYPARTS)
				LOM_ERROR(handle, eBADPARAMETER_LOM);
			for(i = 0; i < rpc_idesc->Server_IndexDesc_u.btree.nColumns; i++)
				rpc_idesc->Server_IndexDesc_u.btree.columns[i] = lom_idesc->kinfo.btree.columns[i];
		}
		else if(lom_idesc->indexType == SM_INDEXTYPE_MLGF) {
			rpc_idesc->indexType = CS_BTREE;
			rpc_idesc->Server_IndexDesc_u.mlgf.flag = lom_idesc->kinfo.mlgf.flag;
			rpc_idesc->Server_IndexDesc_u.mlgf.nColumns = lom_idesc->kinfo.mlgf.nColumns;
			if(rpc_idesc->Server_IndexDesc_u.mlgf.nColumns > CS_MLGF_MAXNUM_KEYS)
				LOM_ERROR(handle, eBADPARAMETER_LOM);
			for(i = 0; i < rpc_idesc->Server_IndexDesc_u.mlgf.nColumns; i++)
				rpc_idesc->Server_IndexDesc_u.mlgf.columns[i] = lom_idesc->kinfo.mlgf.columns[i];
			rpc_idesc->Server_IndexDesc_u.mlgf.extraDataLen = lom_idesc->kinfo.mlgf.extraDataLen;
		}
		else LOM_ERROR(handle, eBADPARAMETER_LOM);
	}
	else if (directionFlag == RPC_TO_LOM) {
		if(rpc_idesc->indexType == CS_BTREE) {
			lom_idesc->indexType = SM_INDEXTYPE_BTREE;
			lom_idesc->kinfo.btree.flag = rpc_idesc->Server_IndexDesc_u.btree.flag;
			lom_idesc->kinfo.btree.nColumns = rpc_idesc->Server_IndexDesc_u.btree.nColumns;
			if(lom_idesc->kinfo.btree.nColumns > MAXNUMKEYPARTS)
				LOM_ERROR(handle, eBADPARAMETER_LOM);
			for(i = 0; i < lom_idesc->kinfo.btree.nColumns; i++)
				lom_idesc->kinfo.btree.columns[i] = rpc_idesc->Server_IndexDesc_u.btree.columns[i];
		}
		else if(rpc_idesc->indexType == CS_MLGF) {
			lom_idesc->indexType = SM_INDEXTYPE_MLGF;
			lom_idesc->kinfo.mlgf.flag = rpc_idesc->Server_IndexDesc_u.mlgf.flag;
			lom_idesc->kinfo.mlgf.nColumns = rpc_idesc->Server_IndexDesc_u.mlgf.nColumns;
			if(lom_idesc->kinfo.mlgf.nColumns > MAXNUMKEYPARTS)
				LOM_ERROR(handle, eBADPARAMETER_LOM);
			for(i = 0; i < lom_idesc->kinfo.mlgf.nColumns; i++)
				lom_idesc->kinfo.mlgf.columns[i] = rpc_idesc->Server_IndexDesc_u.mlgf.columns[i];
			lom_idesc->kinfo.mlgf.extraDataLen = rpc_idesc->Server_IndexDesc_u.mlgf.extraDataLen;
		}
	}
	else LOM_ERROR(handle, eBADPARAMETER_LOM);

	return eNOERROR;
}


Four lom_ConvertIndexID(
	LOM_Handle *handle, 
	LOM_IndexID *lom_iid,
	Server_IndexID *rpc_iid,
	Two directionFlag
)
{

	if(directionFlag == LOM_TO_RPC) {
		if(lom_iid->isLogical == TRUE) {
			rpc_iid->isLogical = lom_iid->isLogical;
			rpc_iid->iid.pageNo = lom_iid->index.physical_iid.pageNo;
			rpc_iid->iid.volNo = lom_iid->index.physical_iid.volNo;
			rpc_iid->iid.slotNo = lom_iid->index.logical_iid.slotNo;
			rpc_iid->iid.unique = lom_iid->index.logical_iid.unique;
		}
		else {
			rpc_iid->isLogical = lom_iid->isLogical;
			rpc_iid->iid.pageNo = lom_iid->index.logical_iid.pageNo;
			rpc_iid->iid.volNo = lom_iid->index.logical_iid.volNo;
		}
	}
	else if(directionFlag == RPC_TO_LOM) {
		if(rpc_iid->isLogical == TRUE) {
			lom_iid->isLogical = (Boolean)rpc_iid->isLogical;
			lom_iid->index.physical_iid.pageNo = rpc_iid->iid.pageNo;
			lom_iid->index.physical_iid.volNo = rpc_iid->iid.volNo;
			lom_iid->index.logical_iid.slotNo = rpc_iid->iid.slotNo;
			lom_iid->index.logical_iid.unique = rpc_iid->iid.unique;
		}
		else {
			lom_iid->isLogical = (Boolean)rpc_iid->isLogical;
			lom_iid->index.logical_iid.pageNo = rpc_iid->iid.pageNo;
			lom_iid->index.logical_iid.volNo = rpc_iid->iid.volNo;
		}
	}
	else LOM_ERROR(handle, eBADPARAMETER_LOM);

	return eNOERROR;
}

Four lom_ConvertBoundCond(
	LOM_Handle *handle, 
	BoundCond *boundCond,
	Server_BoundCond *rpc_BoundCond,
	Two directionFlag
)
{
	if(directionFlag == LOM_TO_RPC) {
		/* key value */
		rpc_BoundCond->key.len = boundCond->key.len;
		bcopy(boundCond->key.val, rpc_BoundCond->key.val, MAXKEYLEN);

		/* comparision operator */
		rpc_BoundCond->op = (Server_CompOp)boundCond->op;
	}
	else if(directionFlag == RPC_TO_LOM) {
		/* key value */
		boundCond->key.len = rpc_BoundCond->key.len;
		bcopy(rpc_BoundCond->key.val, boundCond->key.val, MAXKEYLEN);

		/* comparision operator */
		boundCond->op = (CompOp)rpc_BoundCond->op;
	}
	else LOM_ERROR(handle, eBADPARAMETER_LOM);

	return eNOERROR;
}

Four lom_ConvertBoolExp(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four nBools,
	BoolExp *bool,
	Server_BoolExp *rpc_bool,
	Two directionFlag
)
{
	Four i;
	Four v;
	Four e;
	catalog_SysClassesOverlay *ptrToSysClasses;     /* pointer to sysclasses */
	catalog_SysAttributesOverlay *ptrToSysAttributes; /* pointer to sysattributes */
	Four idxForClassInfo;

	/* search catalog */
	e = Catalog_GetClassInfo(handle, volId, classId, &idxForClassInfo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) LOM_ERROR(handle, e);

	/* set physical pointer to in-memory catalog */
	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(v)[idxForClassInfo];
	ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	if(directionFlag == LOM_TO_RPC) {
		for(i = 0; i < nBools; i++) {
			rpc_bool->op = bool->op;
			rpc_bool->colNo = bool->colNo;
			rpc_bool->length = bool->length;
			switch(ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(rpc_bool->colNo)].type) {
				case SM_SHORT :
					rpc_bool->data.type = CS_SHORT_TYPE;
					rpc_bool->data.boolData_t_u.s = bool->data.s;
					break;
				case SM_INT :
					rpc_bool->data.type = CS_INT_TYPE;
					rpc_bool->data.boolData_t_u.i = bool->data.i;
					break;
				case SM_LONG:
					rpc_bool->data.type = CS_LONG_TYPE;
					rpc_bool->data.boolData_t_u.l = bool->data.l;
					break;
				case SM_LONG_LONG:
					rpc_bool->data.type = CS_LONG_LONG_TYPE;
					rpc_bool->data.boolData_t_u.ll = bool->data.ll;
					break;
				case SM_FLOAT:
					rpc_bool->data.type = CS_FLOAT_TYPE;
					rpc_bool->data.boolData_t_u.f = bool->data.f;
					break;
				case SM_DOUBLE:
					rpc_bool->data.type = CS_DOUBLE_TYPE;
					rpc_bool->data.boolData_t_u.d = bool->data.d;
					break;
				case SM_OID:
					rpc_bool->data.type = CS_OID_TYPE;
					rpc_bool->data.boolData_t_u.oid = *((CS_OID *)(&bool->data.oid));
					break;
				case SM_MBR:
					rpc_bool->data.type = CS_SHORT_TYPE;
					rpc_bool->data.boolData_t_u.mbr = *((CS_MBR *)(&bool->data.mbr));
					break;
				case SM_STRING:
					rpc_bool->data.type = CS_STRING_TYPE;
					bcopy(bool->data.str, rpc_bool->data.boolData_t_u.str, MAXKEYLEN);
					break;
				case SM_VARSTRING:
					rpc_bool->data.type = CS_VARSTRING_TYPE;
					bcopy(bool->data.str, rpc_bool->data.boolData_t_u.str, MAXKEYLEN);
					break;
				default : 
					LOM_ERROR(handle, eBADPARAMETER_LOM);
			}
		}
	}
	else if(directionFlag == RPC_TO_LOM) {
		LOM_ERROR(handle, eNOTYETIMPLEMENTED_LOM);
	}
	else LOM_ERROR(handle, eBADPARAMETER_LOM);

	return eNOERROR;
}

Four lom_ConvertLockParameter(
	LOM_Handle *handle, 
	LockParameter *lockup,
	Server_LockParameter *rpc_lockup,
	Two directionFlag
)
{

	if(directionFlag == LOM_TO_RPC) {
		rpc_lockup->mode = (Server_LockMode)lockup->mode;
		rpc_lockup->duration = (Server_LockDuration)lockup->duration;
	}
	else if(directionFlag == RPC_TO_LOM) {
		lockup->mode = (LockMode)rpc_lockup->mode;
		lockup->duration = (LockDuration)rpc_lockup->duration;
	}
	else LOM_ERROR(handle, eBADPARAMETER_LOM);

	return eNOERROR;
}

