Para iniciar la apliacion se ha de iniciar zookeeper con el fichero de configuracion incluido zoo_smaple.cfg

El script DHT.sh proporciona los comandos para iniciar y parar los distintos nodos de la DHT.
Si se ejecuta sin parametros iniciara una DHT con un clusterManager un ManagerWatcher y cuatro nodos, con cuatro tablas y un factor de replicacion de 2.

Las opciones del script se pueden ver con la opcion -h. Si se quiere configar el numero de tablas o el factor de replicacion las opciones -q y -r se han de especificar antes del resto de opciones por limitaciones del script.

El .jar se proporciona compilado con Java 13.0.7 si no se descarga de GitHub, si no se ha de generar uno nuevo con Maven. Si cambia la ruta debera modificarla tambien en el script de instalacion.

Para iniciar el sistema sin script primero se ha de desplegar un ClusterManager, seguido de un ManagerWatcher. Una vez desplegados se han de arrancar tantas DHT como quorum se haya especificado.

En la interfaz grafica, sustituir key por el string con la key sobre la que operar y en value introducir el valor numerico a almacenar. Las respuesas apercen en la ventana responses. Una respuesta vacia significa que la key no existe.