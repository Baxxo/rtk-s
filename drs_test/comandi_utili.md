***Connessione ssh:***

    ssh root@192.168.178.27

***

***Copiare file:***

    scp test_single_core.py root@192.168.178.27:/root/

***

***Eseguire su n task:***

    taskset -c 0,1,2,3 python3 test_single_core.py