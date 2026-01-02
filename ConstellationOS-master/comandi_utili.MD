./builder.sh build-docker-image example-rpi3.conf

./builder.sh build example-rpi3.conf

***Comando per impostare ip ethernet***

    cd /etc/config
    nano network
    impostare:
        --> option device 'eth0'
        --> option ipaddr 'IP DA ASSEGNARE'

    reload_config

***Comando per monitor risorse:***

    htop

***Per collegarsi in ssh specificare utente***

    esempio: ssh root@192.168.178.27
