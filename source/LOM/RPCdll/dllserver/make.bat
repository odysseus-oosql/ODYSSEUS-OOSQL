..\..\..\..\..\rpc\bin\rpcgen dllserver.x

rem dllserver_svc.c���� main�� rpc_main���� �̸��� �����Ѵ�.
sed s/main/rpc_main/g dllserver_svc.c > dllserver_svc.c.new
move dllserver_svc.c.new dllserver_svc.c