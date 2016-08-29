const MAXNAMELEN = 256;

typedef string 			nametype<MAXNAMELEN>;
typedef long			CS_FOUR;
typedef unsigned long   CS_UFOUR;
typedef short			CS_TWO;
typedef unsigned short	CS_UTWO;
typedef char			CS_ONE;
typedef unsigned char	CS_UONE;


struct CS_LOM_Handle {
	CS_FOUR		serverInstanceId;
	CS_FOUR		instanceId;
};

struct CS_OID {        		/* OID is used accross the volumes */
	CS_FOUR 	pageNo;     /* specify the page holding the object */
	CS_TWO  	volNo;      /* specify the volume in which object is in */
	CS_TWO 		slotNo;     /* specify the slot within the page */
	CS_UFOUR 	unique;     /* Unique No for checking dangling object */
	CS_FOUR 	classID;    /* specify the class including the object */
};



struct rpc_dlopen_arg {
	nametype	moduleName;
	CS_FOUR		mode;
};

struct rpc_dlopen_reply {
    CS_FOUR     errCode;
	CS_FOUR		handle;
};

struct rpc_dlsym_arg {
	CS_FOUR		handle;
	nametype	funcName;
};

struct rpc_dlsym_reply {
	CS_FOUR		errCode;
	CS_FOUR		funcPtr;
};

struct rpc_dlclose_arg {
	CS_FOUR		handle;
};

struct rpc_dlclose_reply {
	CS_FOUR		errCode;
};

struct rpc_dlerror_arg {
	CS_FOUR		dummy;
};

struct rpc_dlerror_reply {
	CS_FOUR		errCode;
	string		errMsg<>;
};

struct rpc_KEinit_arg {
	CS_FOUR			funcPtr;
	CS_FOUR			locationOfContent;
	CS_LOM_Handle 	handle;
	CS_FOUR 		volId; 
	nametype		className; 
	CS_OID			oid;
	CS_TWO 			colNo;
	string			inFileName<>;
};

struct rpc_KEinit_reply {
    CS_FOUR     	errCode;
	CS_FOUR			resultHandle;
};

struct rpc_KEnext_arg {
	CS_FOUR			funcPtr;
	CS_FOUR			resultHandle;
};

struct rpc_KEnext_reply {
    CS_FOUR     	errCode;
	nametype		keyword;
	CS_FOUR			nPositions;
	opaque			positionList<>;
};

struct rpc_KEnext_bulk_reply {
	opaque			data<>;
};

struct rpc_KEfinal_arg {
	CS_FOUR			funcPtr;
	CS_FOUR			resultHandle;
};

struct rpc_KEfinal_reply {
    CS_FOUR     	errCode;
};



/*
 * The directory program definition
 */
program DLLSERVERPROG {
        version DLLSERVERVERS {
			rpc_dlopen_reply RPC_DLOPEN(rpc_dlopen_arg) = __LINE__;
			rpc_dlsym_reply RPC_DLSYM(rpc_dlsym_arg) = __LINE__;
			rpc_dlerror_reply RPC_DLERROR(rpc_dlerror_arg) = __LINE__;
			rpc_dlclose_reply RPC_DLCLOSE(rpc_dlclose_arg) = __LINE__;
			rpc_KEinit_reply RPC_KEINIT(rpc_KEinit_arg) = __LINE__;
			rpc_KEnext_reply RPC_KENEXT(rpc_KEnext_arg) = __LINE__;
			rpc_KEnext_bulk_reply RPC_KENEXT_BULK(rpc_KEnext_arg) = __LINE__;
			rpc_KEfinal_reply RPC_KEFINAL(rpc_KEfinal_arg) = __LINE__;
        } = 1;
} = 0x30000001;

