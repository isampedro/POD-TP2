TP2 POD
## Instrucciones de uso:
En el directorio root, ejecutar:
```
./install.sh
```

### 1. CORRER SERVER:
En el directorio server/target/tpe2-g14-server-1.0-SNAPSHOT:
```
./run-server.sh
```

### 2. CORRER CLIENTES:
En el directorio client/target/tpe2-g14-client-1.0-SNAPSHOT:

```
Para correr query 1 (con ejemplo):
./run-query1.sh -Dcity=BUE -Daddresses='127.0.0.1:5701' -DinPath=E:/Git/POD-TP2 -DoutPath='E:/Git/POD-TP2

Para correr query 2 (con ejemplo):
./run-query2.sh -Dcity=BUE -Daddresses='127.0.0.1:5701' -DinPath=E:/Git/POD-TP2 -DoutPath='E:/Git/POD-TP2

Para correr query 3 (con ejemplo):
./run-query3.sh -Dcity=BUE -Daddresses='127.0.0.1:5701' -DinPath=E:/Git/POD-TP2 -DoutPath='E:/Git/POD-TP2 -Dn=3

Para correr query 4 (con ejemplo):
./run-query4.sh -Dcity=BUE -Daddresses='127.0.0.1:5701' -DinPath=E:/Git/POD-TP2 -DoutPath='E:/Git/POD-TP2

Para correr query 5 (con ejemplo):
./run-query5.sh -Dcity=BUE -Daddresses='127.0.0.1:5701' -DinPath=E:/Git/POD-TP2 -DoutPath='E:/Git/POD-TP2 -DcommonName=Alfalfa -Dneighbourhood=1
```

*ACLARACION: LOS EJEMPLOS NO NECESARIAMENTE CORREN. HAY QUE CORRERLO CON NOMBRES CORRECTOS*