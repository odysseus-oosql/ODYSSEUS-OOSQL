%typemap(python,in) char ** STRING_LIST {
	/* Check if is a list */
	if (PyList_Check($source)) {
		int size = PyList_Size($source);
		int i = 0;
		$target = (char **) malloc((size+1)*sizeof(char *));
		for (i = 0; i < size; i++) {
			PyObject *o = PyList_GetItem($source,i);
			if (PyString_Check(o))
				$target[i] = PyString_AsString(PyList_GetItem($source,i));
			else {
				PyErr_SetString(PyExc_TypeError,"list must contain strings");
				free($target);
				return NULL;
			}
		}
		$target[i] = 0;
	} else {
		PyErr_SetString(PyExc_TypeError,"not a list");
		return NULL;
	}
}

// This cleans up the char ** array we malloc¡¯d before the function call
%typemap(python,freearg) char ** STRING_LIST {
	free((char *) $source);
}

// This allows a C function to return a char ** as a Python list
%typemap(python,out) char ** STRING_LIST {
	int len,i;
	len = 0;
	while ($source[len]) len++;
	$target = PyList_New(len);
	for (i = 0; i < len; i++) {
		PyList_SetItem($target,i,PyString_FromString($source[i]));
	}
}
