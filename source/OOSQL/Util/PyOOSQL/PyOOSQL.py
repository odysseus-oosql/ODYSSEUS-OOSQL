# -*- coding: koi8-r -*-
from _PyOOSQL import *
import string
import struct
import types
import array
import sys

OOSQL_Detail_Time_Debug = 0

c_server_elapsedTime_OOSQL_AllocHandle = 0.0
c_server_elapsedTime_OOSQL_Prepare = 0.0
c_server_elapsedTime_OOSQL_GetNumResultCols = 0.0
c_server_elapsedTime_OOSQL_GetResultColType = 0.0
c_server_elapsedTime_OOSQL_Execute = 0.0
c_server_elapsedTime_OOSQL_GetMultipleResults = 0.0
c_server_elapsedTime_OOSQL_FreeHandle = 0.0

c_st_server = 0.0
c_et_server = 0.0


if OOSQL_Detail_Time_Debug == 1:
	import time

OOSQL_ERROR = "OOSQL Error"
DATAFILE_FORMAT_ERROR = "Data file format error"
OOSQL_GETMULTIPLEDATA_BUFFER_SIZE = 16384
BINARY_MODE = 1
TEXT_MODE   = 0

# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
DEFAULT_BUFFER_SIZE = 4*1024*1024
# ... IJKIM29DEC2008 Use ODYSSEUS memory manager 
# JSK15APR2009 NEW_API_FOR_WEB ...
MAXNUMOFCOLS = 256
FOUR_SIZE = 4
# ... JSK15APR2009 NEW_API_FOR_WEB

def StripNullTermination(str):
	index = string.find(str, '\0')
	if index >= 0:
		return str[:index]
	else:
		return str
	
class OOSQL_ERROR:
	def __init__(self, errorCode, errorMessage):
		self.errorCode	= errorCode
		self.errorMessage = errorMessage
		self.errorName	= MakeOOSQL_ErrorName(OOSQL_SystemHandlePtr("NULL"), errorCode)
		
	def __repr__(self):
		return "OOSQL_ERROR(" + self.errorName + "): " + self.errorMessage

def MakeOOSQL_ErrorName(systemHandle, errorCode):
	messageBuffer = " " * 1024	
	OOSQL_GetErrorName(systemHandle, errorCode, messageBuffer, len(messageBuffer))
	messageBuffer = StripNullTermination(messageBuffer)

	return messageBuffer

def MakeOOSQL_ErrorMessage(systemHandle, errorCode):
	messageBuffer = " " * 1024	
	OOSQL_GetErrorMessage(systemHandle, errorCode, messageBuffer, len(messageBuffer))
	messageBuffer = StripNullTermination(messageBuffer)

	return messageBuffer

def MakeOOSQL_QueryErrorMessage(systemHandle, errorCode, handle):
	messageBuffer = " " * 1024	
	OOSQL_GetErrorMessage(systemHandle, errorCode, messageBuffer, len(messageBuffer))
	messageBuffer = StripNullTermination(messageBuffer)

	result = messageBuffer
	
	messageBuffer = " " * 1024	
	OOSQL_GetQueryErrorMessage(systemHandle, handle, messageBuffer, len(messageBuffer))
	messageBuffer = StripNullTermination(messageBuffer)
	
	result = result + "\n" + messageBuffer
	
	return result

def OOSQL_CHECK_ERR(systemHandle, errorCode):
	if errorCode < eNOERROR:
		errorObject = OOSQL_ERROR(errorCode, MakeOOSQL_ErrorMessage(systemHandle, errorCode))
		raise errorObject

def OOSQL_CHECK_QUERY_ERR(systemHandle, errorCode, handle):
	if errorCode < eNOERROR:
		errorObject = OOSQL_ERROR(errorCode, MakeOOSQL_QueryErrorMessage(systemHandle, errorCode, handle))
		raise errorObject 

# IJKIM16FEB2009 error log ...
def OOSQL_PRTERR(errorCode, frame = None):
	errorName	= MakeOOSQL_ErrorName(OOSQL_SystemHandlePtr("NULL"), errorCode)

	if frame is None:
		frame = sys._getframe(1)
	logString = 'Error : %d(%s) in %s:%d\n' %(errorCode, errorName, frame.f_code.co_filename, frame.f_lineno)
	util_errorlog_printf(logString)
# ... IJKIM16FEB2009 error log 
	
	
class OOSQL_Query:
	# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
	#def __init__(self, systemHandle, handle):
	def __init__(self, systemHandle, handle, mmHandle):
	# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		self.systemHandle = systemHandle
		self.handle       = handle
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		self.mmHandle     = mmHandle
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		self.colTypes     = []
		self.nResultCols  = 0
		self.closed       = 0
		
	def __del__(self):
		self.Close()
		
	def Close(self):
		if self.closed == 0:
			# IJKIM 23DEC2008 detail elapsedTime ... 
			if OOSQL_Detail_Time_Debug == 1:
				global c_server_elapsedTime_OOSQL_FreeHandle
				global c_st_server, c_et_server
			# ... IJKIM 23DEC2008 detail elapsedTime 
			self.closed = 1
			# IJKIM 23DEC2008 detail elapsedTime ... 
			if OOSQL_Detail_Time_Debug == 1:
				c_st_server = time.time()
			# ... IJKIM 23DEC2008 detail elapsedTime 

			e = OOSQL_FreeHandle(self.systemHandle, self.handle)
			OOSQL_CHECK_ERR(self.systemHandle, e)

			# IJKIM 23DEC2008 detail elapsedTime ... 
			if OOSQL_Detail_Time_Debug == 1:
				c_et_server = time.time()
				print "c_server_elapsedTime_OOSQL_FreeHandle, c_et_server, c_st_server"
				print c_server_elapsedTime_OOSQL_FreeHandle, c_et_server, c_st_server
				c_server_elapsedTime_OOSQL_FreeHandle += c_et_server - c_st_server
			# ... IJKIM 23DEC2008 detail elapsedTime 
		
	# JSK15APR2009 NEW_API_FOR_WEB ...
	def ExecuteQueryAndGetMultipleResults(self, volumeID, query, topK, sortBufferInfo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize):
	
		columnTypes_void = QuickFitMM_Alloc(self.mmHandle, MAXNUMOFCOLS * FOUR_SIZE)
		columnTypes = util_convert_to_Four_pointer(columnTypes_void)

		(e, nResultsRead, nEstimatedResults, nCols) = OOSQL_ExecuteQueryAndGetMultipleResults(self.systemHandle, self.handle, volumeID, query, topK, sortBufferInfo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize, columnTypes)

		try:
			OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
		except OOSQL_ERROR, errorObject:
			raise errorObject

		self.nResultCols = nCols;

		for i in range (nCols):
			self.colTypes.append(util_convert_to_int_forarray(columnTypes, i));
		
		e = QuickFitMM_Free_Void_Pointer(self.mmHandle, columnTypes)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		# ╨╧╪Жюг ╟А╟З╦╕ ╧ч╬ф ©б╢ы.
		header = ''
		data   = ''
		nTotalResultsRead = 0
		headerSize   = OOSQL_MULTIPLERESULT_HEADER_SIZE(self.nResultCols)

		if e == ENDOFEVAL:
			pass
		else:
			# юп╬Н ╣Июн ╣╔юле╦╦╕ python stringю╦╥н conversionгя╢ы.						
			if nResultsRead > 0:

				if headerBufferSize > 0:
					if headerSize * nResultsRead < headerBufferSize:
						readHeaderBufferSize = headerSize * nResultsRead
					else:
						readHeaderBufferSize = headerBufferSize

					header = util_convert_to_string_with_size(headerBuffer, readHeaderBufferSize)
				else:
					header = ''

				# dataStringаъ©║╪╜ ╫га╕╟М юпю╨ ╨н╨п╦╦ю╩ юъ╤СЁ╫╢ы.
				lastObjectSize   = OOSQL_MULTIPLERESULT_NTH_OBJECT_SIZE(util_convert_from_string(header), self.nResultCols, nResultsRead - 1)
				lastObjectOffset = OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET(util_convert_from_string(header), self.nResultCols, nResultsRead - 1)

				if lastObjectOffset + lastObjectSize < dataBufferSize:
					readDataBufferSize = lastObjectOffset + lastObjectSize
				else:
					readDataBufferSize = dataBufferSize
					
				data   = util_convert_to_string_with_size(dataBuffer, readDataBufferSize)

			else:
				header = ''
				data = ''

		return (e, header, data, nResultsRead, nEstimatedResults, nCols, self.colTypes)
	# ... JSK15APR2009 NEW_API_FOR_WEB


	def Prepare(self, query, sortBufferInfo = OOSQL_SortBufferInfoPtr("NULL")):
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			global c_server_elapsedTime_OOSQL_Prepare
			global c_st_server, c_et_server

		if OOSQL_Detail_Time_Debug == 1:
			c_st_server = time.time()
		# ... IJKIM 23DEC2008 detail elapsedTime 

		e = OOSQL_Prepare(self.systemHandle, self.handle, query, sortBufferInfo)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_et_server = time.time()
			c_server_elapsedTime_OOSQL_Prepare += c_et_server - c_st_server
		# ... IJKIM 23DEC2008 detail elapsedTime 

		self.nResultCols = self.GetNumResultCols()
		self.colTypes = []
		for i in range(self.nResultCols):
			self.colTypes.append(self.GetResultColType(i))
				
	def Execute(self):
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			global c_server_elapsedTime_OOSQL_Execute
			global c_st_server, c_et_server
		# ... IJKIM 23DEC2008 detail elapsedTime 
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_st_server = time.time()
		# ... IJKIM 23DEC2008 detail elapsedTime 

		e = OOSQL_Execute(self.systemHandle, self.handle)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_et_server = time.time()
			c_server_elapsedTime_OOSQL_Execute += c_et_server - c_st_server
		# ... IJKIM 23DEC2008 detail elapsedTime 

	def ExecDirect(self, query, sortBufferInfo = OOSQL_SortBufferInfoPtr("NULL")):
		e = OOSQL_ExecDirect(self.systemHandle, self.handle, query, sortBufferInfo)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		self.nResultCols = self.GetNumResultCols()
		self.colTypes = []
		for i in range(self.nResultCols):
			self.colTypes.append(self.GetResultColType(i))
		
	def Next(self):
		e = OOSQL_Next(self.systemHandle, self.handle)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		return e

	def GetData(self, colNo, mode = TEXT_MODE):
		type = self.colTypes[colNo]

		if type == OOSQL_TYPE_STRING:
			length = self.GetResultColLength(colNo)
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#data = malloc(length)
			data = QuickFitMM_Alloc(self.mmHandle,length)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
			(e, retLength) = OOSQL_GetData(self.systemHandle, self.handle, colNo, 0, data, length)
			try:
				OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
			except OOSQL_ERROR, errorObject:
				# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
				#util_free(data)
				e = QuickFitMM_Free_Void_Pointer(self.mmHandle,data)
				OOSQL_CHECK_ERR(self.systemHandle, e)
				# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
				raise errorObject
			
			result = util_convert_to_string_with_size(data, length)
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#util_free(data)
			e = QuickFitMM_Free_Void_Pointer(self.mmHandle,data)
			OOSQL_CHECK_ERR(self.systemHandle, e)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

			if retLength >= 0:
				str = result[:retLength]
				if mode == TEXT_MODE:
					str = StripNullTermination(str)
				return str
			else:
				return None
		elif type == OOSQL_TYPE_VARSTRING or type == OOSQL_TYPE_TEXT:
			length = 8192
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#data = util_malloc(length)
			data = QuickFitMM_Alloc(self.mmHandle,length)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
			result = ""
			start  = 0
			while 1:
				(e, retLength) = OOSQL_GetData(self.systemHandle, self.handle, colNo, start, data, length)
				OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
				
				result = result + util_convert_to_string_with_size(data, length)
				start = start + retLength
				if retLength < length:
					break
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#util_free(data)
			e = QuickFitMM_Free_Void_Pointer(self.mmHandle,data)
			OOSQL_CHECK_ERR(self.systemHandle, e)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

			if start >= 0:
				str = result[:start]
				if mode == TEXT_MODE:
					str = StripNullTermination(str)
				return str
			else:
				return None
		elif type == OOSQL_TYPE_OID:
			length = 16
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#data = util_malloc(length)
			data = QuickFitMM_Alloc(self.mmHandle,length)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
			(e, retLength) = OOSQL_GetData(self.systemHandle, self.handle, colNo, 0, data, length)
			try:
				OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
			except OOSQL_ERROR, errorObject:
				# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
				#util_free(data)
				e = QuickFitMM_Free_Void_Pointer(self.mmHandle,data)
				OOSQL_CHECK_ERR(self.systemHandle, e)
				# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
				raise errorObject
			result = util_convert_to_oid(data)
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#util_free(data)
			e = QuickFitMM_Free_Void_Pointer(self.mmHandle,data)
			OOSQL_CHECK_ERR(self.systemHandle, e)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
			
			if retLength >= 0:			
				return result
			else:
				return None

		elif type in (OOSQL_TYPE_SMALLINT, OOSQL_TYPE_SHORT, OOSQL_TYPE_INTEGER, OOSQL_TYPE_INT, OOSQL_TYPE_LONG,
					  OOSQL_TYPE_REAL, OOSQL_TYPE_FLOAT, OOSQL_TYPE_DOUBLE, 
					  OOSQL_TYPE_OID, OOSQL_TYPE_DATE,
					  OOSQL_TYPE_TIME, OOSQL_TYPE_TIMESTAMP):
			length = self.GetResultColLength(colNo)
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#data = util_malloc(length)
			data = QuickFitMM_Alloc(self.mmHandle,length)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
			(e, retLength) = OOSQL_GetData(self.systemHandle, self.handle, colNo, 0, data, length)
			try:
				OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
			except OOSQL_ERROR, errorObject:
				# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
				#util_free(data)
				e = QuickFitMM_Free_Void_Pointer(self.mmHandle,data)
				OOSQL_CHECK_ERR(self.systemHandle, e)
				# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
				raise errorObject

			if type == OOSQL_TYPE_SHORT:
				result = util_convert_to_short(data)
			elif type == OOSQL_TYPE_INT or type == OOSQL_TYPE_LONG:
				result = util_convert_to_int(data)
			elif type == OOSQL_TYPE_FLOAT:
				result = util_convert_to_float(data)
			elif type == OOSQL_TYPE_DOUBLE:
				result = util_convert_to_double(data)
			elif type == OOSQL_TYPE_OID:
				result = util_convert_to_oid(data)
			elif type == OOSQL_TYPE_DATE:
				result = util_convert_to_date(data)
			elif type == OOSQL_TYPE_TIME:
				result = util_convert_to_time(data)
			elif type == OOSQL_TYPE_TIMESTAMP:
				result = util_convert_to_timestamp(data)
			else:
				result = util_convert_to_string_with_size(data, length)

			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#util_free(data)
			e = QuickFitMM_Free_Void_Pointer(self.mmHandle,data)
			OOSQL_CHECK_ERR(self.systemHandle, e)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

			if retLength >= 0:			
				return result
			else:
				return None
		else:
			# ╠Бе╦ type©║ ╢Кгь╪╜╢б аж╬НаЬ ╣╔юле╦╦╕ stringю╦╥н╫А юп╬Н Ё╫╢ы.
			length = self.GetResultColLength(colNo)
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#data = util_malloc(length)
			data = QuickFitMM_Alloc(self.mmHandle,length)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
			(e, retLength) = OOSQL_GetData(self.systemHandle, self.handle, colNo, 0, data, length)
			try:
				OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
			except OOSQL_ERROR, errorObject:
				# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
				#util_free(data)
				e = QuickFitMM_Free_Void_Pointer(self.mmHandle,data)
				OOSQL_CHECK_ERR(self.systemHandle, e)
				# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
				raise errorObject
			
			result = util_convert_to_string_with_size(data, length)
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#util_free(data)
			e = QuickFitMM_Free_Void_Pointer(self.mmHandle,data)
			OOSQL_CHECK_ERR(self.systemHandle, e)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

			if retLength >= 0:
				str = result[:retLength]
				return str
			else:
				return None

	def GetDataFromBuffer(self, header, data, ith, colNo, mode = TEXT_MODE):
		type   = self.colTypes[colNo]

		# ith ╟╢ц╪юг offset╟З isNull а╓╨╦╦╕ ╟║а╝©б╢ы.
		header = util_convert_from_string(header)
		offset = OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET(header, self.nResultCols, ith)
		isNull = OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_ISNULL(header, self.nResultCols, ith, colNo)
		
		# ith ╟╢ц╪юг ╟╒ column╨╟ size а╓╨╦╦╕ ╟║а╝©б╢ы.
		sizes = []		
		for j in range(self.nResultCols):
			sizes.append(OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_SIZE(header, self.nResultCols, ith, j))

		# ith ╟╢ц╪юг colNoюг offset╟З length╦╕ ╠╦гя╢ы.
		for size in sizes[:colNo]:
			offset = offset + size
		length = sizes[colNo]
	  
		if type == OOSQL_TYPE_STRING:
			if isNull:
				return None
			else:
				if length >= 0:
					str = data[offset:offset + length]
					if mode == TEXT_MODE:
						str = StripNullTermination(str)
					return str
				else:
					return None
		elif type == OOSQL_TYPE_VARSTRING or type == OOSQL_TYPE_TEXT:
			# ╨╩ routineю╦╥н╢б varstring, text╦╕ юЭ╨н юпаЖ╢б ╦Ьгя╢ы. ажюггр╟м
			if isNull:
				return None
			else:
				if length >= 0:
					str = data[offset:offset + length]
					if mode == TEXT_MODE:
						str = StripNullTermination(str)
					return str
				else:
					return None
		elif type == OOSQL_TYPE_OID:
			if isNull:
				return None
			else:
				return util_convert_to_oid(data[offset:offset + length])
		elif type in (OOSQL_TYPE_SMALLINT, OOSQL_TYPE_SHORT, OOSQL_TYPE_INTEGER, OOSQL_TYPE_INT, OOSQL_TYPE_LONG,
					  OOSQL_TYPE_REAL, OOSQL_TYPE_FLOAT, OOSQL_TYPE_DOUBLE, 
					  OOSQL_TYPE_OID, OOSQL_TYPE_DATE,
					  OOSQL_TYPE_TIME, OOSQL_TYPE_TIMESTAMP):
			if isNull:
				return None
			else:
				result = util_convert_from_string(data[offset:offset + length])

				if type == OOSQL_TYPE_SHORT:
					result = util_convert_to_short(result)
				elif type == OOSQL_TYPE_INT or type == OOSQL_TYPE_LONG:
					result = util_convert_to_int(result)
				elif type == OOSQL_TYPE_FLOAT:
					result = util_convert_to_float(result)
				elif type == OOSQL_TYPE_DOUBLE:
					result = util_convert_to_double(result)
				elif type == OOSQL_TYPE_OID:
					result = util_convert_to_oid(result)
				elif type == OOSQL_TYPE_DATE:
					result = util_convert_to_date(result)
				elif type == OOSQL_TYPE_TIME:
					result = util_convert_to_time(result)
				elif type == OOSQL_TYPE_TIMESTAMP:
					result = util_convert_to_timestamp(result)
				else:
					result = util_convert_to_string_with_size(result, length)
				return result
		else:
			return "..."

	def GetDataAsTuple(self, mode = TEXT_MODE):
		result = ()
		for i in range(self.nResultCols):
			result = result + (self.GetData(i, mode),)
		return result

	def GetDataAsTupleFromBuffer(self, header, data, ith, mode = TEXT_MODE):
		result = ()
		for i in range(self.nResultCols):
			result = result + (self.GetDataFromBuffer(header, data, ith, i, mode),)
		return result
	
	def FetchAll(self, mode = TEXT_MODE):
		results = []
		while self.Next() != ENDOFEVAL:
			results.append(self.GetDataAsTuple(mode))
		return results

	# IJKIM06JAN2009 progressive memory allocation for OOSQL_GetMultipleResults ... 
	#def GetMultipleResults2(self, headerBufferSize, dataBufferSize, nResultsToRead):
	def GetMultipleResults3(self, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize, nResultsToRead):
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			global c_server_elapsedTime_OOSQL_GetMultipleResults
			global c_st_server, c_et_server
		# IJKIM 23DEC2008 detail elapsedTime ... 

		# ╨╧╪Жюг ╟А╟З╦╕ ╧ч╬ф ©б╢ы.
		header = ''
		data   = ''
		nTotalResultsRead = 0
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#headerBuffer = util_malloc(headerBufferSize)
		#dataBuffer   = util_malloc(dataBufferSize)
		# IJKIM06JAN2009 progressive memory allocation for OOSQL_GetMultipleResults ...
		#headerBuffer = QuickFitMM_Alloc(self.mmHandle,headerBufferSize)
		#dataBuffer   = QuickFitMM_Alloc(self.mmHandle,dataBufferSize)
		# ... IJKIM06JAN2009 progressive memory allocation for OOSQL_GetMultipleResults 
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		headerSize   = OOSQL_MULTIPLERESULT_HEADER_SIZE(self.nResultCols)

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_st_server = time.time()
		# ... IJKIM 23DEC2008 detail elapsedTime 

		(e, nResultsRead) = OOSQL_GetMultipleResults(self.systemHandle, self.handle, nResultsToRead,
													 headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_et_server = time.time()
			c_server_elapsedTime_OOSQL_GetMultipleResults += c_et_server - c_st_server
		# ... IJKIM 23DEC2008 detail elapsedTime 
		try:
			OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
		except OOSQL_ERROR, errorObject:
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#util_free(headerBuffer)
			#util_free(dataBuffer)
			# IJKIM06JAN2009 progressive memory allocation for OOSQL_GetMultipleResults ...
			#e = QuickFitMM_Free_Void_Pointer(self.mmHandle,headerBuffer)
			#OOSQL_CHECK_ERR(self.systemHandle, e)
			#e = QuickFitMM_Free_Void_Pointer(self.mmHandle,dataBuffer)
			#OOSQL_CHECK_ERR(self.systemHandle, e)
			# ... IJKIM06JAN2009 progressive memory allocation for OOSQL_GetMultipleResults 
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
			raise errorObject

		if e == ENDOFEVAL:
			pass
		else:
			# IJKIM19JAN2009 util_convert debug ...
			'''
			# юп╬Н ╣Июн ╣╔юле╦╦╕ python stringю╦╥н conversionгя╢ы.						
			header = util_convert_to_string_with_size(headerBuffer, headerBufferSize)
			data   = util_convert_to_string_with_size(dataBuffer, dataBufferSize)

			# headerStringаъ©║╪╜ ╫га╕╥н юпю╨ ╨н╨п╦╦ю╩ юъ╤С Ё╫╢ы.
			header = header[:headerSize * nResultsRead]

			# dataStringаъ©║╪╜ ╫га╕╟М юпю╨ ╨н╨п╦╦ю╩ юъ╤СЁ╫╢ы.
			lastObjectSize   = OOSQL_MULTIPLERESULT_NTH_OBJECT_SIZE(util_convert_from_string(header), self.nResultCols, nResultsRead - 1)
			lastObjectOffset = OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET(util_convert_from_string(header), self.nResultCols, nResultsRead - 1)
			data = data[:lastObjectOffset + lastObjectSize]
			'''

			# юп╬Н ╣Июн ╣╔юле╦╦╕ python stringю╦╥н conversionгя╢ы.						
			if nResultsRead > 0:
				if headerBufferSize > 0:
					if headerSize * nResultsRead < headerBufferSize:
						readHeaderBufferSize = headerSize * nResultsRead
					else:
						readHeaderBufferSize = headerBufferSize

					header = util_convert_to_string_with_size(headerBuffer, readHeaderBufferSize)
				else:
					header = ''
					
				# dataStringаъ©║╪╜ ╫га╕╟М юпю╨ ╨н╨п╦╦ю╩ юъ╤СЁ╫╢ы.
				lastObjectSize   = OOSQL_MULTIPLERESULT_NTH_OBJECT_SIZE(util_convert_from_string(header), self.nResultCols, nResultsRead - 1)
				lastObjectOffset = OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET(util_convert_from_string(header), self.nResultCols, nResultsRead - 1)

				if lastObjectOffset + lastObjectSize < dataBufferSize:
					readDataBufferSize = lastObjectOffset + lastObjectSize
				else:
					readDataBufferSize = dataBufferSize
					
				data   = util_convert_to_string_with_size(dataBuffer, readDataBufferSize)
			else:
				header = ''
				data = ''
			#  ... IJKIM19JAN2009 util_convert debug

		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#util_free(headerBuffer)
		#util_free(dataBuffer)
		#  IJKIM06JAN2009 progressive memory allocation for OOSQL_GetMultipleResults ...
		#e = QuickFitMM_Free_Void_Pointer(self.mmHandle,headerBuffer)
		#OOSQL_CHECK_ERR(self.systemHandle, e)
		#e = QuickFitMM_Free_Void_Pointer(self.mmHandle,dataBuffer)
		#OOSQL_CHECK_ERR(self.systemHandle, e)
		# ... IJKIM06JAN2009 progressive memory allocation for OOSQL_GetMultipleResults 
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

		return e, header, data, nResultsRead
	# ... IJKIM06JAN2009 progressive memory allocation for OOSQL_GetMultipleResults 

	# ijkim 05/03/11 START ... It is used by OOSQL_Thread.py 
	# GetMultipleResults2 calls OOSQL_GetMultipleResult once
	# and returns error code unlike PyOOSQL.GetMultipleResults 
	def GetMultipleResults2(self, headerBufferSize, dataBufferSize, nResultsToRead):
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			global c_server_elapsedTime_OOSQL_GetMultipleResults
			global c_st_server, c_et_server
		# IJKIM 23DEC2008 detail elapsedTime ... 

		# ╨╧╪Жюг ╟А╟З╦╕ ╧ч╬ф ©б╢ы.
		header = ''
		data   = ''
		nTotalResultsRead = 0
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#headerBuffer = util_malloc(headerBufferSize)
		#dataBuffer   = util_malloc(dataBufferSize)
		headerBuffer = QuickFitMM_Alloc(self.mmHandle,headerBufferSize)
		dataBuffer   = QuickFitMM_Alloc(self.mmHandle,dataBufferSize)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		headerSize   = OOSQL_MULTIPLERESULT_HEADER_SIZE(self.nResultCols)

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_st_server = time.time()
		# ... IJKIM 23DEC2008 detail elapsedTime 

		(e, nResultsRead) = OOSQL_GetMultipleResults(self.systemHandle, self.handle, nResultsToRead,
													 headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_et_server = time.time()
			c_server_elapsedTime_OOSQL_GetMultipleResults += c_et_server - c_st_server
		# ... IJKIM 23DEC2008 detail elapsedTime 
		try:
			OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
		except OOSQL_ERROR, errorObject:
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#util_free(headerBuffer)
			#util_free(dataBuffer)
			e = QuickFitMM_Free_Void_Pointer(self.mmHandle,headerBuffer)
			OOSQL_CHECK_ERR(self.systemHandle, e)
			e = QuickFitMM_Free_Void_Pointer(self.mmHandle,dataBuffer)
			OOSQL_CHECK_ERR(self.systemHandle, e)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
			raise errorObject

		if e == ENDOFEVAL:
			pass
		else:
			# юп╬Н ╣Июн ╣╔юле╦╦╕ python stringю╦╥н conversionгя╢ы.						
			header = util_convert_to_string_with_size(headerBuffer, headerBufferSize)
			data   = util_convert_to_string_with_size(dataBuffer, dataBufferSize)

			# headerStringаъ©║╪╜ ╫га╕╥н юпю╨ ╨н╨п╦╦ю╩ юъ╤С Ё╫╢ы.
			header = header[:headerSize * nResultsRead]

			# dataStringаъ©║╪╜ ╫га╕╟М юпю╨ ╨н╨п╦╦ю╩ юъ╤СЁ╫╢ы.
			lastObjectSize   = OOSQL_MULTIPLERESULT_NTH_OBJECT_SIZE(util_convert_from_string(header), self.nResultCols, nResultsRead - 1)
			lastObjectOffset = OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET(util_convert_from_string(header), self.nResultCols, nResultsRead - 1)
			data = data[:lastObjectOffset + lastObjectSize]

		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#util_free(headerBuffer)
		#util_free(dataBuffer)
		e = QuickFitMM_Free_Void_Pointer(self.mmHandle,headerBuffer)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		e = QuickFitMM_Free_Void_Pointer(self.mmHandle,dataBuffer)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

		return e, header, data, nResultsRead
	# ... END #


	def GetMultipleResults(self, nResultsToRead):
		# ╨╧╪Жюг ╟А╟З╦╕ ╧ч╬ф ©б╢ы.
		header = ''
		data   = ''
		nTotalResultsRead = 0
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#headerBuffer = util_malloc(OOSQL_GETMULTIPLEDATA_BUFFER_SIZE)
		#dataBuffer   = util_malloc(OOSQL_GETMULTIPLEDATA_BUFFER_SIZE)
		headerBuffer = QuickFitMM_Alloc(self.mmHandle,OOSQL_GETMULTIPLEDATA_BUFFER_SIZE)
		dataBuffer   = QuickFitMM_Alloc(self.mmHandle,OOSQL_GETMULTIPLEDATA_BUFFER_SIZE)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		headerSize   = OOSQL_MULTIPLERESULT_HEADER_SIZE(self.nResultCols)

		while nResultsToRead > 0:
			(e, nResultsRead) = OOSQL_GetMultipleResults(self.systemHandle, self.handle, nResultsToRead,
														 headerBuffer, OOSQL_GETMULTIPLEDATA_BUFFER_SIZE,
														 dataBuffer, OOSQL_GETMULTIPLEDATA_BUFFER_SIZE)
			try:
				OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
			except OOSQL_ERROR, errorObject:
				# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
				#util_free(headerBuffer)
				#util_free(dataBuffer)
				e = QuickFitMM_Free_Void_Pointer(self.mmHandle,headerBuffer)
				OOSQL_CHECK_ERR(self.systemHandle, e)
				e = QuickFitMM_Free_Void_Pointer(self.mmHandle,dataBuffer)
				OOSQL_CHECK_ERR(self.systemHandle, e)
				# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
				raise errorObject

			if e == ENDOFEVAL:
				break

			# юп╬Н ╣Июн ╣╔юле╦╦╕ python stringю╦╥н conversionгя╢ы.						
			headerString = util_convert_to_string_with_size(headerBuffer, OOSQL_GETMULTIPLEDATA_BUFFER_SIZE)
			dataString   = util_convert_to_string_with_size(dataBuffer, OOSQL_GETMULTIPLEDATA_BUFFER_SIZE)

			# headerStringаъ©║╪╜ ╫га╕╥н юпю╨ ╨н╨п╦╦ю╩ юъ╤С Ё╫╢ы.
			headerString = headerString[:headerSize * nResultsRead]

			# dataStringаъ©║╪╜ ╫га╕╟М юпю╨ ╨н╨п╦╦ю╩ юъ╤СЁ╫╢ы.
			lastObjectSize   = OOSQL_MULTIPLERESULT_NTH_OBJECT_SIZE(util_convert_from_string(headerString), self.nResultCols, nResultsRead - 1)
			lastObjectOffset = OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET(util_convert_from_string(headerString), self.nResultCols, nResultsRead - 1)
			dataString = dataString[:lastObjectOffset + lastObjectSize]

			header = header + headerString
			data   = data + dataString
			nResultsToRead = nResultsToRead - nResultsRead
			nTotalResultsRead = nTotalResultsRead + nResultsRead
			
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#util_free(headerBuffer)
		#util_free(dataBuffer)
		e = QuickFitMM_Free_Void_Pointer(self.mmHandle,headerBuffer)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		e = QuickFitMM_Free_Void_Pointer(self.mmHandle,dataBuffer)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

		return header, data, nTotalResultsRead
	
	def GetComplexTypeInfo(self, colNo):
		complexTypeInfo = OOSQL_ComplexTypeInfo()
		e = OOSQL_GetComplexTypeInfo(self.systemHandle, self.handle, colNo, complexTypeInfo)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		return complexTypeInfo

	# ijkim 05/03/11 START ... It is used by OOSQL_Thread.py
	# PutData2 uses startPos, bufferLength and use C data 
	# packed in string unlike  PyOOSQL.Query.PutData
	def PutData2(self, colNo, startPos, data, bufferLength):
		# IJKIM08JAN2009 PIR Debug ... 
		#e, dataType = OOSQL_GetPutDataParamType(self.systemHandle, self.handle, colNo)
		#OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		if colNo == -1:
			dataType = -1 # OOSQL_TYPE_INT_ARRAY type. 
		else:
			e, dataType = OOSQL_GetPutDataParamType(self.systemHandle, self.handle, colNo)
			OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
		# IJKIM08JAN2009 PIR Debug ... 
		
		# eliminate null byte #
		if dataType == OOSQL_TYPE_STRING or dataType == OOSQL_TYPE_VARSTRING or dataType == OOSQL_TYPE_TEXT:
			index = string.find(data, '\0')
			if index != -1:
				data = data[:index]
				bufferLength = index

			e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, util_convert_from_string(data), bufferLength)

		else:
			data2 = util_convert_from_string(data)
			buffer = " " * 20
			
			if dataType == OOSQL_TYPE_SHORT:
				e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, data2, bufferLength)
			elif dataType == OOSQL_TYPE_INT or dataType == OOSQL_TYPE_LONG:
				e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, data2, bufferLength)
			# IJKIM08JAN2009 PIR Debug ... 
			elif dataType == -1:
				e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, data2, bufferLength)
			# IJKIM08JAN2009 PIR Debug ... 
			elif dataType == OOSQL_TYPE_FLOAT:
				e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, data2, bufferLength)
			elif dataType == OOSQL_TYPE_DOUBLE:
				e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, data2, bufferLength)
			elif dataType == OOSQL_TYPE_OID:
				e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, data2, bufferLength)
			elif dataType == OOSQL_TYPE_DATE:
				e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, data2, bufferLength)
			elif dataType == OOSQL_TYPE_TIME:
				e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, data2, bufferLength)
			elif dataType == OOSQL_TYPE_TIMESTAMP:
				e = OOSQL_PutData(self.systemHandle, self.handle, colNo, startPos, data2, bufferLength)
			else:
				raise OOSQL_ERROR, OOSQL_ERROR(-1, "Unhandled Result Type")
			OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
	## ... END ##


	def PutData(self, colNo, data):
		e, dataType = OOSQL_GetPutDataParamType(self.systemHandle, self.handle, colNo)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
		
		if dataType == OOSQL_TYPE_STRING or dataType == OOSQL_TYPE_VARSTRING or dataType == OOSQL_TYPE_TEXT:
			e = OOSQL_PutData(self.systemHandle, self.handle, colNo, 0, util_convert_from_string(data), len(data))
			OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
		else:
			buffer = " " * 10

			if dataType == OOSQL_TYPE_SHORT:
				length = util_convert_from_short(data, buffer)
			elif dataType == OOSQL_TYPE_INT or dataType == OOSQL_TYPE_LONG:
				length = util_convert_from_int(data, buffer)
			elif dataType == OOSQL_TYPE_FLOAT:
				length = util_convert_from_float(data, buffer)
			elif dataType == OOSQL_TYPE_DOUBLE:
				length = util_convert_from_double(data, buffer)
			elif dataType == OOSQL_TYPE_OID:
				length = util_convert_from_oid(data, buffer)
			elif dataType == OOSQL_TYPE_DATE:
				length = util_convert_from_date(data, buffer)
			elif dataType == OOSQL_TYPE_TIME:
				length = util_convert_from_time(data, buffer)
			elif dataType == OOSQL_TYPE_TIMESTAMP:
				length = util_convert_from_timestamp(data, buffer)
			else:
				raise OOSQL_ERROR, OOSQL_ERROR(-1, "Unhandled Result Type")

			e = OOSQL_PutData(self.systemHandle, self.handle, colNo, 0, buffer, length)
			OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
		
	def GetOID(self, targetNo):
		oid = OID()
		e = OOSQL_GetOID(self.systemHandle, self.handle, targetNo, oid)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		return oid
	
	def GetNumResultCols(self):
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			global c_server_elapsedTime_OOSQL_GetNumResultCols
			global c_st_server, c_et_server
		# ... IJKIM 23DEC2008 detail elapsedTime 
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_st_server = time.time()
		# ... IJKIM 23DEC2008 detail elapsedTime 

		(e, nCols) = OOSQL_GetNumResultCols(self.systemHandle, self.handle)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_et_server = time.time()
			c_server_elapsedTime_OOSQL_GetNumResultCols += c_et_server - c_st_server
		# ... IJKIM 23DEC2008 detail elapsedTime 

		return nCols
	
	def GetResultColName(self, colNo):
		name = ' ' * 1024
		e = OOSQL_GetResultColName(self.systemHandle, self.handle, colNo, name, len(name))
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		name = StripNullTermination(name)
		return name

	def GetResultColNamesAsTuple(self):
		result = ()
		for i in range(self.nResultCols):
			result = result + (self.GetResultColName(i),)
		return result
	
	def GetResultColType(self, colNo):
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			global c_server_elapsedTime_OOSQL_GetResultColType
			global c_st_server, c_et_server
		# ... IJKIM 23DEC2008 detail elapsedTime 
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_st_server = time.time()
		# ... IJKIM 23DEC2008 detail elapsedTime 

		(e, type) = OOSQL_GetResultColType(self.systemHandle, self.handle, colNo)

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_et_server = time.time()
			c_server_elapsedTime_OOSQL_GetResultColType += c_et_server - c_st_server
		# ... IJKIM 23DEC2008 detail elapsedTime 
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

		return type

	def GetResultColTypesAsTuple(self):
		result = ()
		for i in range(self.nResultCols):
			result = result + (self.GetResultColType(i),)
		return result
	
	def GetResultColLength(self, colNo):
		(e, length) = OOSQL_GetResultColLength(self.systemHandle, self.handle, colNo)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
		
		return length		

	def GetResultColLengthsAsTuple(self):
		result = ()
		for i in range(self.nResultCols):
			result = result + (self.GetResultColLength(i),)
		return result
	
	def DumpPlan(self):
		bufferSize = 128 * 1024
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#buffer	 = util_malloc(bufferSize)
		buffer	 = QuickFitMM_Alloc(self.mmHandle,bufferSize)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		e = OOSQL_DumpPlan(self.systemHandle, self.handle, buffer, bufferSize)
		if e == eNOERROR:
			result = util_convert_to_string(buffer)
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#util_free(buffer)	
			ef = QuickFitMM_Free_Void_Pointer(self.mmHandle,buffer)	
			OOSQL_CHECK_ERR(self.systemHandle, ef)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager 
			return result
		else:
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			#util_free(buffer)	
			ef = QuickFitMM_Free_Void_Pointer(self.mmHandle,buffer)	
			OOSQL_CHECK_ERR(self.systemHandle, ef)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager 
			OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)

	# IJKIM15DEC2008 add ...
	def EstimateNumResults(self):
		(e, nResults) = OOSQL_EstimateNumResults(self.systemHandle, self.handle)
		OOSQL_CHECK_QUERY_ERR(self.systemHandle, e, self.handle)
		
		return nResults
	# ... IJKIM15DEC2008 add 
		
class OOSQL_System:
	def __init__(self):
		self.systemHandle = OOSQL_SystemHandle()

		(e, self.procIndex) = OOSQL_CreateSystemHandle(self.systemHandle)

		## ijkim 2004/9/22 START ...  ##
		if e < eNOERROR:
			self.SystemHandleCreated = 0
		else:
			self.SystemHandleCreated = 1
		## ... END ##

		OOSQL_CHECK_ERR(self.systemHandle, e)

		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		self.mmHandle = QuickFitMM_Handle()
		e = QuickFitMM_Init(self.mmHandle, DEFAULT_BUFFER_SIZE)

		if e < eNOERROR:
			self.mmHandleCreated = 0
		else:
			self.mmHandleCreated = 1
		OOSQL_CHECK_ERR(self.systemHandle, e)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

	def __del__(self):
		## ijkim 2004/9/22 START ... ##
		e = 0
		if self.SystemHandleCreated == 1:
			# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
			if self.mmHandleCreated == 1:
					e = QuickFitMM_Final(self.mmHandle)
					self.mmHandleCreated = 0
					OOSQL_CHECK_ERR(self.systemHandle, e)
			# ... IJKIM29DEC2008 Use ODYSSEUS memory manager

			e = OOSQL_DestroySystemHandle(self.systemHandle, self.procIndex)
		else:
			pass
		## ... END ##


		##  ... Original start ##
#		e = OOSQL_DestroySystemHandle(self.systemHandle, self.procIndex)
		## ... END ##

		OOSQL_CHECK_ERR(self.systemHandle, e)

	def SetUserDefaultVolumeID(self, databaseID, volumeID):
		e = OOSQL_SetUserDefaultVolumeID(self.systemHandle, databaseID, volumeID)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def GetUserDefaultVolumeID(self, databaseID):
		(e, volumeID) = OOSQL_GetUserDefaultVolumeID(self.systemHandle, databaseID)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
		return volumeID

	def GetVolumeID(self, databaseID, volumeName):
		(e, volumeID) = OOSQL_GetVolumeID(self.systemHandle, databaseID, volumeName)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return volumeID

	def MountDB(self, databaseName):
		(e, databaseID) = OOSQL_MountDB(self.systemHandle, databaseName)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return databaseID

	def DismountDB(self, databaseID):
		e = OOSQL_DismountDB(self.systemHandle, databaseID)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def MountVolumeByVolumeName(self, databaseName, volumeName):
		(e, volumeId) = OOSQL_MountVolumeByVolumeName(self.systemHandle, databaseName, volumeName)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return volumeId

	def Dismount(self, volumeID):
		e = OOSQL_Dismount(self.systemHandle, volumeID)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def CreateQuery(self, volumeID):
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			global c_server_elapsedTime_OOSQL_AllocHandle,  c_server_elapsedTime_OOSQL_Prepare , c_server_elapsedTime_OOSQL_GetNumResultCols, c_server_elapsedTime_OOSQL_GetResultColType, c_server_elapsedTime_OOSQL_Execute, c_server_elapsedTime_OOSQL_GetMultipleResults, c_server_elapsedTime_OOSQL_FreeHandle
			global c_st_server, c_et_server
		# ... IJKIM 23DEC2008 detail elapsedTime 
		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_server_elapsedTime_OOSQL_AllocHandle = 0.0
			c_server_elapsedTime_OOSQL_Prepare = 0.0
			c_server_elapsedTime_OOSQL_GetNumResultCols = 0.0
			c_server_elapsedTime_OOSQL_GetResultColType = 0.0
			c_server_elapsedTime_OOSQL_Execute = 0.0
			c_server_elapsedTime_OOSQL_GetMultipleResults = 0.0
			c_server_elapsedTime_OOSQL_FreeHandle = 0.0
		# ... IJKIM 23DEC2008 detail elapsedTime 

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_st_server = time.time()
		# ... IJKIM 23DEC2008 detail elapsedTime 
		(e, handle) = OOSQL_AllocHandle(self.systemHandle, volumeID)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		# IJKIM 23DEC2008 detail elapsedTime ... 
		if OOSQL_Detail_Time_Debug == 1:
			c_et_server = time.time()
			c_server_elapsedTime_OOSQL_AllocHandle += c_et_server - c_st_server
		# ... IJKIM 23DEC2008 detail elapsedTime 

		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#query = OOSQL_Query(self.systemHandle, handle)
		query = OOSQL_Query(self.systemHandle, handle, self.mmHandle)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		return query

	# JSK15APR2009 NEW_API_FOR_WEB ...
	def ExecuteTopkQueryAndGetMultipleResults(self, volumeID, query, topK, sortBufferInfo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize):
	
		columnTypes_void = QuickFitMM_Alloc(self.mmHandle, MAXNUMOFCOLS * FOUR_SIZE)
		columnTypes = util_convert_to_Four_pointer(columnTypes_void)
		cTypes = []

		(e, nResultsRead, nEstimatedResults, nCols) = OOSQL_ExecuteTopkQueryAndGetMultipleResults(self.systemHandle, volumeID, query, topK, sortBufferInfo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize, columnTypes)

		try:
			OOSQL_CHECK_ERR(self.systemHandle, e)
		except OOSQL_ERROR, errorObject:
			raise errorObject

		for i in range (nCols):
			cTypes.append(util_convert_to_int_forarray(columnTypes, i))

		e = QuickFitMM_Free_Void_Pointer(self.mmHandle, columnTypes)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
		# ╨╧╪Жюг ╟А╟З╦╕ ╧ч╬ф ©б╢ы.
		header = ''
		data   = ''
		nTotalResultsRead = 0
		headerSize   = OOSQL_MULTIPLERESULT_HEADER_SIZE(nCols)

		if e == ENDOFEVAL:
			pass
		else:
			# юп╬Н ╣Июн ╣╔юле╦╦╕ python stringю╦╥н conversionгя╢ы.						
			if nResultsRead > 0:

				if headerBufferSize > 0:
					if headerSize * nResultsRead < headerBufferSize:
						readHeaderBufferSize = headerSize * nResultsRead
					else:
						readHeaderBufferSize = headerBufferSize

					header = util_convert_to_string_with_size(headerBuffer, readHeaderBufferSize)
				else:
					header = ''

				# dataStringаъ©║╪╜ ╫га╕╟М юпю╨ ╨н╨п╦╦ю╩ юъ╤СЁ╫╢ы.
				lastObjectSize   = OOSQL_MULTIPLERESULT_NTH_OBJECT_SIZE(util_convert_from_string(header), nCols, nResultsRead - 1)
				lastObjectOffset = OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET(util_convert_from_string(header), nCols, nResultsRead - 1)

				if lastObjectOffset + lastObjectSize < dataBufferSize:
					readDataBufferSize = lastObjectOffset + lastObjectSize
				else:
					readDataBufferSize = dataBufferSize
					
				data   = util_convert_to_string_with_size(dataBuffer, readDataBufferSize)

			else:
				header = ''
				data = ''

		return (e, header, data, nResultsRead, nEstimatedResults, nCols, cTypes)

	# ... JSK15APR2009 NEW_API_FOR_WEB
	def TransBegin(self, cclevel):
		self.xactID = XactID()

		e = OOSQL_TransBegin(self.systemHandle, self.xactID, cclevel)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def TransCommit(self):
		e = OOSQL_TransCommit(self.systemHandle, self.xactID)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def TransAbort(self):
		e = OOSQL_TransAbort(self.systemHandle, self.xactID)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def OIDToOIDString(self, oid):
		oidstring = " " * 32 + " "
		e = OOSQL_OIDToOIDString(self.systemHandle, oid, oidstring)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		oidstring = oidstring[:-1]
		
		return oidstring

	def Text_AddKeywordExtractor(self, volumeID, keywordExtractorName, version, path, initFuncName, getFuncName, finalFuncName):
		(e, extNo) = OOSQL_Text_AddKeywordExtractor(self.systemHandle, volumeID, keywordExtractorName, version, path, initFuncName, getFuncName, finalFuncName)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return extNo

	def Text_AddDefaultKeywordExtractor(self, volumeID, keywordExtractorName, version, path, initFuncName, getFuncName, finalFuncName):
		e = OOSQL_Text_AddKeywordExtractor(self.systemHandle, volumeID, keywordExtractorName, version, path, initFuncName, getFuncName, finalFuncName)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Text_DropKeywordExtractor(self, volumeID, keywordExtractorName, version):
		e = OOSQL_Text_DropKeywordExtractor(self.systemHandle, volumeID, keywordExtractorName, version)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Text_GetKeywordExtractorNo(self, volumeID, keywordExtractorName, version):
		(e, extNo) = OOSQL_Text_GetKeywordExtractorNo(self.systemHandle, volumeID, keywordExtractorName, version)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return extNo

	def Text_SetKeywordExtractor(self, volumeID, className, attrName, extNo):
		e = OOSQL_SetKeywordExtractor(self.systemHandle, volumeID, className, attrName, extNo)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Text_AddFilter(self, volumeID, filterName, version, path, funcName):
		(e, filterNo) = OOSQL_Text_AddFilter(self.systemHandle, volumeID, filterName, version, path, funcName)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return filterNo
	
	def Text_DropFilter(self, volumeID, filterName, version):
		e = OOSQL_Text_DropFilter(self.systemHandle, volumeID, filterName, version)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Text_GetFilterNo(self, volumeID, filterName, version):
		(e, filterNo) = OOSQL_Text_GetFilterNo(self.systemHandle, volumeID, filterName, version)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return filterNo
	
	def Text_SetFilter(self, volumeID, className, attrName, filterNo):
		e = OOSQL_Text_SetFilter(self.systemHandle, volumeID, className, attrName, filterNo)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Text_KeywordInfoScan_Open(self, volumeID, className, attrName, keyword):
		e = OOSQL_Text_KeywordInfoScan_Open(self.systemHandle, volumeID, className, attrName, keyword)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return e

	def Text_KeywordInfoScan_Close(self, scanId):
		e = OOSQL_Text_KeywordInfoScan_Close(self.systemHandle, scanId)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Text_KeywordInfoScan_Next(self, scanId):
		keyword = " " * 1024
		(e, nDocuments, nPositions) = OOSQL_Text_KeywordInfoScan_Next(self.systemHandle, scanId, keyword)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		keyword = StripNullTermination(keyword)

		return (keyword, nDocuments, nPositions)

	def Text_KeywordInfoScanForDocument_Open(self, volumeID, className, oid, attrName, keyword):
		e = OOSQL_Text_KeywordInfoScanForDocument_Open(self.systemHandle, volumeID, className, oid, attrName, keyword)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return e

	def Text_KeywordInfoScanForDocument_Close(self, scanId):
		e = OOSQL_Text_KeywordInfoScanForDocument_Close(self.systemHandle, scanId)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Text_KeywordInfoScanForDocument_Next(self, scanId):
		keyword = " " * 1024
		(e, nDocuments, nPositions) = OOSQL_Text_KeywordInfoScanForDocument_Next(self.systemHandle, scanId, keyword)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		keyword = StripNullTermination(keyword)

		return (keyword, nDocuments, nPositions)

	
	def Text_MakeIndex(self, volumeId, temporaryVolumeId, className):
		e = OOSQL_Text_MakeIndex(self.systemHandle, volumeId, temporaryVolumeId, className)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Text_GetNumKeywordsInDocument(self, volumeId, oid, columnName):
		(e, numKeywords) = OOSQL_Text_GetNumKeywordsInDocument(self.systemHandle, volumeId, oid, columnName)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return numKeywords

	def Text_GetIthKeywordInDocument(self, volumeId, oid, columnName, ith):
		keyword = " " * 1024
		e = OOSQL_Text_GetIthKeywordInDocument(self.systemHandle, volumeId, oid, columnName, ith, keyword);
		OOSQL_CHECK_ERR(self.systemHandle, e)

		keyword = StripNullTermination(keyword)
		
		return keyword
	
	def GetHour(self, time):
		return OOSQL_GetHour(self.systemHandle, time)

	def GetMinute(self, time):
		return OOSQL_GetMinute(self.systemHandle, time)

	def GetSecond(self, time):
		return OOSQL_GetSecond(self.systemHandle, time)

	def GetYear(self, date):
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#buffer = util_malloc(4)
		buffer = QuickFitMM_Alloc(self.mmHandle,4)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		date_ptr = util_get_unsigned_long_ptr(buffer, date)
		year = OOSQL_GetYear(self.systemHandle, date_ptr)
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#util_free(buffer)
		e = QuickFitMM_Free_Void_Pointer(self.mmHandle,buffer)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager 
		return year

	def GetMonth(self, date):
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#buffer = util_malloc(4)
		buffer = QuickFitMM_Alloc(self.mmHandle,4)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		date_ptr = util_get_unsigned_long_ptr(buffer, date)
		month = OOSQL_GetMonth(self.systemHandle, date_ptr)
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#util_free(buffer)
		e = QuickFitMM_Free_Void_Pointer(self.mmHandle,buffer)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager 
		return month
	
	def GetDay(self, date):
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#buffer = util_malloc(4)
		buffer = QuickFitMM_Alloc(self.mmHandle,4)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager
		date_ptr = util_get_unsigned_long_ptr(buffer, date)
		day = OOSQL_GetDay(self.systemHandle, date_ptr)
		# IJKIM29DEC2008 Use ODYSSEUS memory manager ...
		#util_free(buffer)
		e = QuickFitMM_Free_Void_Pointer(self.mmHandle,buffer)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		# ... IJKIM29DEC2008 Use ODYSSEUS memory manager 
		return day
	
	def GetVersionString(self):
		return OOSQL_GetVersionString()

	def GetCompilationParamString(self):
		return OOSQL_GetCompilationParamString()

	def GetTimeElapsed(self):
		(e, timeElapsed) = OOSQL_GetTimeElapsed(self.systemHandle)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return timeElapsed
	
	def ResetTimeElapsed(self):
		e = OOSQL_ResetTimeElapsed(self.systemHandle)
		OOSQL_CHECK_ERR(self.systemHandle, e)
				
	def ComplexType_GetElementType(self, complexTypeInfo):
		(e, type) = OOSQL_ComplexType_GetElementType(complexType)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return type

	def ComplexType_GetComplexType(self, complexTypeInfo):
		(e, type) = OOSQL_ComplexType_GetComplexType(complexType)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return type

	def ComplexType_GetNumElements(self, complexTypeInfo):
		(e, nElements) = OOSQL_ComplexType_GetNumElements(complexTypeInfo)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
		return nElements

	def ComplexType_GetElementsString(self, complexTypeInfo, start, nElements):
		str = ' ' * 8192
		e = OOSQL_ComplexType_GetElementsString(complexTypeInfo, start, nElements, str, len(str))
		OOSQL_CHECK_ERR(self.systemHandle, e)

		str = StripNullTermination(str)

		return str

	
	# ijkim 05/05/07 START ... It is used by OOSQL_Thread.py
	# PyOOSQL.System.ComplexType_GetElementsString2 uses stringLength
	def ComplexType_GetElementsString2(self, complexTypeInfo, start, nElements, stringLength):
		str = ' ' * stringLength
		e = OOSQL_ComplexType_GetElementsString(complexTypeInfo, start, nElements, str, stringLength)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		str = StripNullTermination(str)

		return str
	# END ... #


	def ComplexType_InsertElements(self, complexTypeInfo, start, nElements, elementSizes, elements):
		if type(elementSizes) == array.ArrayType:
			elementSizes = elementSizes.tostring()
		if type(elements) == array.ArrayType:
			elements = elements.tostring()			
		e = OOSQL_ComplexType_InsertElements(complexTypeInfo, start, nElements, elementSizes, elements)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def ComplexType_IsNULL(self, complexTypeInfo):
		e = OOSQL_ComplexType_IsNULL(complexTypeInfo)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		return e
								
	def ComplexType_GetElements(self, complexTypeInfo, start, nElements):
		#buffer = ' ' * 8192
		#e = OOSQL_ComplexType_GetElements(complexTypeInfo,
		print "Not Implemented"


	# IJKIM05JAN2009 add PIR API ... 
	def OOSQL_SetKeywordRangeForPIR(self, startKeyword, endKeyword, addedKeywords, flag):
		e = OOSQL_SetKeywordRangeForPIR(self.systemHandle, startKeyword, endKeyword, addedKeywords, flag);
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return e

	def OOSQL_ResetKeywordRangeForPIR(self):
		e = OOSQL_ResetKeywordRangeForPIR(self.systemHandle)
		OOSQL_CHECK_ERR(self.systemHandle, e)

		return e
	# ... IJKIM05JAN2009 add PIR API 

	def Tool_BuildTextIndex(self, volId, temporaryVolId, className, attrName, config):
		e = oosql_Tool_BuildTextIndex(self.systemHandle, volId, temporaryVolId, className, attrName, config)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def Tool_DeleteTextIndex(self, volId, className, attrName):
		e = oosql_Tool_DeleteTextIndex(self.systemHandle, volId, className, attrName)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def Tool_GetDatabaseStatistics(self, databaseName, databaseId):
		e = oosql_Tool_GetDatabaseStatistics(self.systemHandle, databaseName, databaseId)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def Tool_ExtractKeyword(self, volId, className, attrName, dataFileName, outputFileName, startObjectNo, endObjectNo, alwaysUsePreviousPostingFile):
		e = oosql_Tool_ExtractKeyword(self.systemHandle, volId, className, attrName, dataFileName, outputFileName, startObjectNo, endObjectNo, alwaysUsePreviousPostingFile)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def Tool_MapPosting(self, volId, className, attrName, postingFileNames, newPostingFileName, oidFileName, sortMergeMode, pageRankFile, pageRankMode):
		e = oosql_Tool_MapPosting(self.systemHandle, volId, className, attrName, len(postingFileNames), postingFileNames, newPostingFileName, oidFileName, sortMergeMode, pageRankFile, pageRankMode)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Tool_MergePosting(self, postingFileNames, newPostingFileName):
		e = oosql_Tool_MergePosting(self.systemHandle, len(postingFileNames), postingFileNames, newPostingFileName)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def Tool_SortPosting(self, postingFileName, sortedPostingFileName):
		e = oosql_Tool_SortPosting(postingFileName, sortedPostingFileName)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def Tool_SortStoredPosting(self, volId, temporaryVolId, className, attrName):
		e = oosql_Tool_SortStoredPosting(self.systemHandle, volId, temporaryVolId, className, attrName)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def Tool_StorePosting(self, volId, className, attrName, clearFlag):
		e = oosql_Tool_StorePosting(self.systemHandle, volId, className, attrName, clearFlag)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def Tool_UpdateTextDescriptor(self, volId, className):
		e = oosql_Tool_UpdateTextDescriptor(self.systemHandle, volId, className)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	# HYKWON29JAN2008 BUG FIX ... #
	#def Tool_LoadDB(self, volId, temporaryVolId, isDeferredTextIndexMode, smallUpdateFlag, useBulkloading, useDescriptorUpdating, datafileName):
	#	e = oosql_Tool_LoadDB(self.systemHandle, volId, temporaryVolId, isDeferredTextIndexMode, smallUpdateFlag, useBulkloading, useDescriptorUpdating, datafileName)
	def Tool_LoadDB(self, volId, temporaryVolId, isDeferredTextIndexMode, smallUpdateFlag, useBulkloading, useDescriptorUpdating, datafileName, pageRankFileName, pageRankMode):
		e = oosql_Tool_LoadDB(self.systemHandle, volId, temporaryVolId, isDeferredTextIndexMode, smallUpdateFlag, useBulkloading, useDescriptorUpdating, datafileName, pageRankFileName, pageRankMode)
	# ... HYKWON29JAN2008 BUG FIX #

	def Tool_BatchDeleteFromFile(self, volId, temporaryVolId, className, oidFileName):
		e = oosql_Tool_BatchDeleteFromFile(self.systemHandle, volId, temporaryVolId, className, oidFileName)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Tool_BatchDeleteByDeferredDeletionList(self, volId, temporaryVolId, className):
		e = oosql_Tool_BatchDeleteByDeferredDeletionList(self.systemHandle, volId, temporaryVolId, className)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def Tool_ShowBatchDeleteStatus(self, volId, className):
		e = oosql_Tool_ShowBatchDeleteStatus(self.systemHandle, volId, className)
		OOSQL_CHECK_ERR(self.systemHandle, e)

	def GetVersionString(self):
		return OOSQL_GetVersionString();

	def GetNumTextObjectsInVolume(self, volId):
		e, numObjects = OOSQL_GetNumTextObjectsInVolume(self.systemHandle, volId)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		return numObjects
	
	def GetNumObjectsInVolume(self, volId):
		e, numObjects = OOSQL_GetNumObjectsInVolume(self.systemHandle, volId)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		return numObjects

	def GetNumObjectsInClass(self, volId, className):
		e, numObjects = OOSQL_GetNumObjectsInClass(self.systemHandle, volId, className)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		return numObjects
	
	def GetTableDescription(self, volId, className):
		# аж╬НаЬ ╨╪╥Щ©║ а╦юГго╢б евюл╨М©м ╟Э╥ц╣х а╓╨╦╦╕ ╦П╣н ╬Р╬Н юл╦╕ ╧щх╞гя╢ы.
		# ╟Э╥ц╣х а╓╨╦╥н╢б ╠╦╪╨╣х ╪с╪╨╣И©║ ╢Кгя а╓╨╦╟З ╠╦╪╨╣х юн╣╕╫╨©║ ╢Кгя а╓╨╦юл╢ы.
		# ╠╦╪╨╣х ╪с╪╨©║ ╢Кгя а╓╨╦╥н╢б ╪с╪╨юг (╪с╪╨ юл╦╖, дц╥Ё ╧Ьхё, ╪с╪╨ е╦ют, ╪с╪╨е╦ют ╨н╟║а╓╨╦) юл╢ы.
		# ╠╦╪╨╣х юн╣╕╫╨©║ ╢Кгя а╓╨╦╥н╢б юнеь╫╨юг (юн╣╕╫╨ юл╦╖) юл╢ы.
		query = self.CreateQuery(volId)
		query.ExecDirect("select classId from lomSysClasses where className = '%s'" % className)
		results = query.FetchAll()
		classId = results[0][0]
		query.ExecDirect("select colName, columnNo, colType, complexType from lomSysColumns where classId = %d" % (classId))
		results = query.FetchAll()

		# results╦╕ columnNo╪Ью╦╥н ╧Х©╜гя╢ы.
		# resultsюг ╟╒ element ╠╦а╤
		# results[0][0] : colName of first object
		# results[0][1] : columnNo of first object
		# results[0][2] : colType of first object
		# results[0][3] : complexType of first object
		def resultColumnSortFunc(item1, item2):
			# results╦╕ columnNo╪Ью╦╥н sortго╠Б ю╖гя ╨Я╠Ёгт╪Ж
			return item1[1] - item2[1]

		# results╦╕ columnNo╪Ью╦╥н sortгя╢ы.		
		results.sort(resultColumnSortFunc)

		# _logicalId╢б ╫ц╫╨еш©║╪╜ ╟Э╦╝©Кю╦╥н ╨ыюн attributeюл╧г╥н ╩Ха╕гя╢ы.
		if results[0][0] == "_logicalId":
			results = results[1:]
			
			# дц╥Ёюл гоЁ╙ ╩Ха╕╣х╟мюл╧г╥н, дц╥Ё ╧Ьхё╦╕ гоЁ╙╬© ╟╗╪р╫це╡╢ы
			newresults = []
			for result in results:
				newresults.append((result[0], result[1] - 1, result[2], result[3]))
			results = newresults

		attributeInfos = results
		
		# index а╓╨╦╦╕ юп╬Н╣Июн╢ы.
		query.ExecDirect("select indexName, indexId from lomSysIndexes where className = '%s'" % (className))
		results = query.FetchAll(BINARY_MODE) # ╟А╟З╦╕ юп╬Н ╣Июо╤╖, binary mode╥н юп╬Н ╣Июн╢ы.

		# index informationюг Ё╩©Кю╩ гь╪╝гя╢ы.
		indexInfos = [] #results
		for result in results:
			indexInfo = []
			indexName = result[0]
			indexInfo.append(indexName) # indexNameю╩ цъ╟║гя╢ы.
			#   typedef struct {
			#		Boolean isLogical;
			#		union {
			#			IndexID physical_iid;
			#			LOM_LogicalIndexID logical_iid;
			#		}index;
			#	}LOM_IndexID;
			# indexInfo╢б ю╖╟З ╟╟ю╨ гЭеб╦╕ го╟М юж╢ы ю╖ ╠╦а╤╦╕ гь╪╝гя╢ы.
			isLogical = struct.unpack("i", result[1][:4])[0]
			if isLogical == 1:
				logical_iid = result[1][4:]
				# аж╬НаЬ logical_iid╢б OID╥н index а╓╨╦╟║ юЗюЕ╣х ╟╢ц╪юг pysical locationю╩ Ё╙е╦Ё╫╢ы.
				# logical_iid╥н ╨нем oid╦╕ ╠╦цЮгя╢ы.
				query.ExecDirect("select columnNo from lomSysTextIndexes where invertedIndexName = '%s'" % (indexName))
				results = query.FetchAll()
				indexInfo.append("TEXT_INDEX")
				indexInfo.append("NORMAL_INDEX")
				indexInfo.append(1)
				columns = [(results[0][0] - 1, "ASCENDING_ORDER")]
				indexInfo.append(columns)
				indexInfos.append(indexInfo)
			else:
				pysical_iid = result[1][4:4 + 8]
				# аж╬НаЬ  pysical_iid╦╕ ╩Г©Кго©╘ index desc╦╕ юп╬Н Ё╫╢ы.
				# lrdsSysIndexes©║ юж╢б ╦П╣Г index а╓╨╦╦╕ юп╬Н Ё╩╬Н гй©Дгя ╣╔юле╦╦╕ ╟Я╤СЁ╫╢ы.
				query.ExecDirect("select indexId, indexDesc from lrdsSysIndexes")
				results = query.FetchAll(BINARY_MODE) # ╟А╟З╦╕ юп╬Н ╣Июо╤╖, binary mode╥н юп╬Н ╣Июн╢ы.
				for result in results:
					if pysical_iid == result[0]:
						# index desc╦╕ гь╪╝гя╢ы.
						indexDesc = result[1]
						indexType = ord(struct.unpack("c", indexDesc[:1])[0])
						if indexType == 1: # BTREE index
							indexInfo.append("BTREE_INDEX")
							keyInfo = indexDesc[4:]
							flag, nColumns = struct.unpack("hh", keyInfo[:4])
							if flag == 0:
								indexInfo.append("NORMAL_INDEX")
							elif flag == 1:
								indexInfo.append("UNIQUE_INDEX")
							else:
								indexInfo.append("CLUSTERING_INDEX")
							indexInfo.append(nColumns)
							columns = []
							for i in range(nColumns):
								columnInfo = keyInfo[4 + i * 8:4 + i * 8 + 8]
								columnNo, flag = struct.unpack("ii", columnInfo)
								if flag == 2:
									flag = "ASCENDING_ORDER"
								else:
									flag = "DESCENDING_ORDER"
								columns.append((columnNo - 1, flag))
								
							# index╟║ indexго╟Мюз го╢б columnюл _logicalId╤С╦И юл а╓╨╦╢б
							# ╩Г©Кюз©║╟т ╧щх╞гоаЖ ╬й╣╣╥о гя╢ы.
							if columns[0][0] >= 0:
								indexInfo.append(columns)
								indexInfos.append(indexInfo)
						else:			  # MLGF index
							indexInfo.append("MLGF_INDEX")
							keyInfo = indexDesc[4:]
							flag, nColumns = struct.unpack("hh", keyInfo[:4])
							if flag == 0:
								indexInfo.append("NORMAL_INDEX")
							elif flag == 1:
								indexInfo.append("UNIQUE_INDEX")
							else:
								indexInfo.append("CLUSTERING_INDEX")
							indexInfo.append(nColumns)
							columns = []
							for i in range(nColumns):
								columnInfo = keyInfo[4 + i * 2:4 + i * 2 + 2]
								columnNo = struct.unpack("h", columnInfo)
								columns.append(columnNo - 1, "ASCENDING_ORDER")
								
							# index╟║ indexго╟Мюз го╢б columnюл _logicalId╤С╦И юл а╓╨╦╢б
							# ╩Г©Кюз©║╟т ╧щх╞гоаЖ ╬й╣╣╥о гя╢ы.
							if columns[0][0] >= 0:
								indexInfo.append(columns)
								indexInfos.append(indexInfo)
						break

		# аЗюг ╟╢ц╪╦╕ ╩Ха╕гя╢ы.		
		query = None

		# ╟А╟З╦╕ ╠╦╪╨гя╢ы. ╟А╟З╢б className, classId, attributeInfos, indexInfos╥н ╠╦╪╨╣х╢ы.
		# attributeInfos╢б columnName, columnNo, columnType, columnComplexTypeю╩ ╟╒ attribute©║ ╢Кгь ╟║аЖ╦Г,
		# indexInfos╢б indexName╦╕ ╟╒ index©║ ╢Кгя а╓╨╦╥н╪╜ ╟╓╢б╢ы.
		return (className, classId, attributeInfos, indexInfos)
	
	def GetAllClassNames(self, volId):
		query = self.CreateQuery(volId)
		query.ExecDirect("select className, classId from lomSysClasses")
		results = query.FetchAll()

		classNames = []
		for result in results:
			# system defined class╣Ию╨ screeningгя╢ы.
			if result[1] < 1000:
				continue
			if len(result[0]) >= 7 and result[0][:7] == "LOM_SYS":
				continue
			if len(result[0]) >= 6 and result[0][-6:] == "_docId":
				continue
			if len(result[0]) >= 9 and result[0][-9:] == "_Inverted":
				continue
			classNames.append(result[0])

		return classNames

	def SetCfgParam(self, name, value):
		e = OOSQL_SetCfgParam(self.systemHandle, name, value)
		OOSQL_CHECK_ERR(self.systemHandle, e)
		
	def GetCfgParam(self, name):
		return OOSQL_GetCfgParam(self.systemHandle, name)

	
# Utility functions
def CountObjectsInLoadDbFile(dataFileName):
	# data fileюг ц╧ аыю╩ parsingго©╘ ╦Н╟Ёюг ╪с╪╨ю╦╥н юл╥Г╬На╝ юж╢баЖ╦╕
	# ╬к╬ф Ё╫╢ы. юл ╟╙ю╩ numAttrsForOneObject ©║ юЗюЕгя╢ы.
	file = open(dataFileName, "r")
	classLine = file.readline()
	file.close()
	
	# replace '(' and ')' into <space>
	classLine = string.join(string.split(classLine, '('), ' ')
	classLine = string.join(string.split(classLine, ')'), ' ')
	
	# retrives className and attributesInDatafile	
	classLineSplitted = string.split(classLine)
	className = classLineSplitted[1]
	attributesInDatafile = classLineSplitted[2:]
	numAttrsForOneObject = len(attributesInDatafile)
	
	# data fileю╩ ©╜╬Н гя line╬© юп╬Н ця ╦Н╟Ёюг lineю╦╥н юл╥Г╬На╝ юж╢баЖ╦╕
	# х╝юнгя╢ы. юл ╟╙ю╩ numLinesInDataFile ©║ юЗюЕгя╢ы.
	# гоЁ╙юг lineю╩ юпю╩╤╖, ╦╦╬Ю гоЁ╙юг lineюл \\nюлЁ╙ \\ю╦╥н Ё║Ё╜ ╟Ф©Л,
	# ╢ыю╫ lineю╦╥н ©╛╟А╣х╟мюл╧г╥н, ©╛╟Аго©╘ ╠в ╪Ж╦╕ ╩В╢ы.
	# util_count_lines_in_loaddb_fileю╨ speed╦╕ ю╖гь C╥н ╠╦гЖ╣х гт╪Ж юл╢ы.
	numLinesInDataFile = util_count_lines_in_loaddb_file(dataFileName)
	
	# numLinesInDataFile / numAttrsForOneObject юг ╟╙ю╩ ╧щх╞гя╢ы.
	return numLinesInDataFile / numAttrsForOneObject

def MergeDataInLoadDbFiles(dataFileNameList, mergedFileName):
	return util_merge_loaddb_files(len(dataFileNameList), dataFileNameList, mergedFileName)
