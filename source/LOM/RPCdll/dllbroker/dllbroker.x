const ODYS_TCP  = 0x00000001;
const ODYS_UDP  = 0x00000010;


struct dllbroker_connect_reply {
	long errCode;
	long serverNo;
};

struct dllbroker_connect_arg {
	long protocol;
};

struct dllbroker_disconnect_reply {
	long errCode;
};

struct dllbroker_disconnect_arg {
	long serverNo;
};

program DLLBROKERPROG {
	version DLLBROKERVERS {
		dllbroker_connect_reply DLLBROKER_CONNECT(dllbroker_connect_arg) = __LINE__;
		dllbroker_disconnect_reply DLLBROKER_DISCONNECT(dllbroker_disconnect_arg) = __LINE__;
	} = 1;
} = 0x30000001;

