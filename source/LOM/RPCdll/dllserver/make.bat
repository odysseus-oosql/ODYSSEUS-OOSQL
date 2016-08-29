..\..\..\..\..\rpc\bin\rpcgen dllserver.x

rem dllserver_svc.c에서 main을 rpc_main으로 이름을 변경한다.
sed s/main/rpc_main/g dllserver_svc.c > dllserver_svc.c.new
move dllserver_svc.c.new dllserver_svc.c