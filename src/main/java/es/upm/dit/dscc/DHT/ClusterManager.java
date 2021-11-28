package es.upm.dit.dscc.DHT;

import org.apache.commons.lang.SerializationUtils;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manager del Cluster, asigna tablas y gestiona caidas.
 */

public class ClusterManager implements Watcher {

    //Zookeeper configuration y rutas
    String[] hosts;
    private ZooKeeper zk;
    private static String rootMembers;
    private static String rootManagement;
    private static String rootOperations;
    private static String manager;
    private static String aTable;
    private static String tableAssignments;
    private static String initBarrier;
    private static String responses;
    private static String temporalAssignments;
    private static String quorumRoute;
    private static String integration;
    private static String aIntegration;
    Integer mutexBarrier = -1;
    private String myId;
    private static final int SESSION_TIMEOUT = 5000;


    private String[] DHTs; //Lideres de las tablas
    private String[] temporalLeaders; //Lideress temporales de las dht
    private int numberOfDHTs = 0; // Numero de nodos conectados
    private int replicationFactor; //Numero de copias de cada tabla

    //Number of servers needed
    private int Quorum;
    private boolean initedSystem;

    //Logger configuration
    private static final Logger LOGGER = Logger.getLogger(ClusterManager.class.getName());

    /**
     * Constructor del CLuster manager. DHTs y temporalLeaders permite iniciar el manager en un estado determinado en
     * caso de caida.
     *
     * @param Quorum Numero de nodos DHT, uno por cada tabla que tengamos
     * @param replicationFactor Numero de copias de cada tabla
     * @param DHTs Array con los liders de cada tabla
     * @param temporalLeaders Array con los lideres temporales de cada tabla
     */

    ClusterManager(int Quorum,int replicationFactor,String[] DHTs, String[] temporalLeaders){
        rootMembers = Common.rootMembers;
        rootOperations = Common.rootOperations;
        manager = Common.manager;
        rootManagement = Common.rootManagement;
        hosts = Common.hosts;
        aTable = Common.aTable;
        initBarrier = Common.initBarrier;
        responses = Common.responses;
        temporalAssignments = Common.temporalAssignments;
        integration = Common.integration;
        aIntegration = Common.aIntegration;
        this.Quorum = Quorum;
        quorumRoute = Common.quorumRoute;
        if(DHTs == null){
            this.DHTs = new String[Quorum];
        }else{
            this.DHTs = DHTs;
        }
        if(temporalLeaders == null){
            this.temporalLeaders = new String[Quorum];
        }else{
            this.temporalLeaders = temporalLeaders;
        }
        this.replicationFactor = replicationFactor;
        tableAssignments = Common.tableAssignments;

    }

    /* ---------------------Initialization ---------------------------------  */

    /**
     * Inicializacion del manager, conecta a zookeeper y crea los nodos necesarios para el funcionamiento de la aplicacion
     *
     */

    public void init(){
        // Select a random zookeeper server
        Random rand = new Random();
        int i = rand.nextInt(hosts.length);

        try {
            if (zk == null) {
                zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
                try {
                    // Wait for creating the session. Use the object lock
                    synchronized (mutexBarrier) {
                        mutexBarrier.wait();
                    }
                    //zk.exists("/",false);
                } catch (Exception e) {
                    LOGGER.severe("Exception in the wait while creating the session");
                }
            }
        } catch (Exception e) {
            LOGGER.severe("Exception while creating the session");
        }

        // Create zookeeper session if it doesn't exist

        if (zk != null) {
            // Create a folder for members and include this process/server
            try {
                // Create a node with the name "member", if it is not created
                // Set a watcher
                String response = new String();
                Stat s = zk.exists(rootMembers, membersWatcher);
                if (s == null) {
                    response = zk.create(rootMembers, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.fine(response);
                }

                // Create a node with the name "operations", if it is not created
                s = zk.exists(rootOperations, operationsWatcher);
                if (s == null) {
                    response = zk.create(rootOperations, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.fine(response);
                }

                // Create a node with the name "management", if it is not created
                s = zk.exists(rootManagement, managementWatcher);
                if (s == null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootManagement, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.fine(response);
                }

                // Create a node with the name tableAssingmets, if it is not created
                s = zk.exists(rootManagement + tableAssignments, null);
                if (s == null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootManagement + tableAssignments, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.fine(response);
                }

                // Create a node with the name barrier, if it is not created
                s = zk.exists(rootManagement + initBarrier, null);
                if (s == null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootManagement + initBarrier, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.fine(response);
                }

                // Create a node with the name tableAssingmets, if it is not created
                s = zk.exists(rootOperations + responses, null);
                if (s == null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootOperations + responses, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.fine(response);
                }

                // Create a node with the name tableAssingmets, if it is not created
                s = zk.exists(rootManagement + temporalAssignments, null);
                if (s == null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootManagement + temporalAssignments, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.fine(response);
                }

                s = zk.exists(rootManagement + quorumRoute, null);
                if (s == null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootManagement + quorumRoute,  ByteBuffer.allocate(4).putInt(Quorum).array(),
                            Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    LOGGER.fine(response);
                }

                s = zk.exists(rootManagement + integration, null);
                if (s == null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootManagement + integration,   new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOGGER.fine(response);
                }else{
                   List<String> list = zk.getChildren(rootManagement + integration,null);
                    for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
                        String path = (String) iterator.next();
                        Stat s2 = new Stat();
                        zk.getData(rootManagement + integration+"/"+ path, null, s2);
                        zk.delete(rootManagement + integration+"/"+ path, s2.getVersion());
                    }
                }

                // Create a znode for registering as member and get my id
                myId = zk.create(rootMembers + manager, new byte[0],
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                myId = myId.replace(rootMembers + "/", "");

                // Get the children of a node and set a watcher
                List<String> list = zk.getChildren(rootMembers, membersWatcher, s);
                LOGGER.info("Created znode nember id:"+ myId );
                printListMembers(list);

            } catch (KeeperException e) {
                LOGGER.severe("The session with Zookeeper failes. Closing");
                LOGGER.warning(e.toString());
                return;
            } catch (InterruptedException e) {
                LOGGER.warning("InterruptedException raised");
            }

        }
    }

     /**
     * This variable creates a new watcher. It is fired when the session
     * is created.
     */
    private Watcher cWatcher = new Watcher() {
        public void process (WatchedEvent e) {
            LOGGER.info("Created session");
            LOGGER.fine(e.toString());
            synchronized (mutexBarrier) {
                mutexBarrier.notify();
            }
        }
    };

    /* ---------------------Functions for members---------------------------------  */

    /**
     * This variable creates a new watcher. It is fired when a child of "members"
     * is created or deleted.
     */

    private Watcher membersWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------ClusterManager:Watcher for  members------------------\n");
            try {
                //Miramos en el watcher si tenemos algun nodo nuevo
                Boolean watcherFound = false; //Miramos si no se nos ha caido el que mira si nos caemos nosotros
                List<String> list = zk.getChildren(rootMembers,  membersWatcher);
                printListMembers(list);
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
                    String member = iterator.next();
                    //Esto para ver si es una DHT que ya tenemos
                    Boolean exists = false;
                    for(int i = 0; i< DHTs.length; i++){
                        if( member.equals( DHTs[i])){
                            exists = true;
                        }
                    }
                    //Si tenemos una DHT nueva y no hemos llegado al quorum le asignamos su tablas y replicas
                    //Ya que estamos miramos si el watcher sigue vivo
                    if(!exists  && !member.equals("manager") && !member.equals("listener") && numberOfDHTs<Quorum){
                        TableAssigment assigment = assignTable(member);
                        int position = writeTableAssigment(assigment);
                        DHTs[position] = member; //Guardamos el nodo nuevo
                        if(temporalLeaders[position] != null){
                            temporalLeaders[position] = null;
                            LOGGER.info("Member needs integration");
                            TableAssignmentIntegration integrator = new TableAssignmentIntegration(assigment.getTableLeader(),assigment.getTableReplicas(),assigment.getDHTId());
                            byte[] data = SerializationUtils.serialize(integrator); //Assignment -> byte[]
                            zk.create(rootManagement + integration +aIntegration, data,
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                        }
                        LOGGER.fine(String.valueOf(position));
                        numberOfDHTs++; //Ahora tenemos un miembro nuevo
                        LOGGER.info("Added new member "+member);
                    }else if(member.equals("listener") ){
                        watcherFound = true;
                    }
                }
                if(watcherFound){
                    LOGGER.info("ManagerWatcher still up");
                }else{
                    LOGGER.info("ManagerWatcher id down, we should create a new one");
                }

                if(!initedSystem && numberOfDHTs == Quorum){
                    initedSystem = true;
                    zk.delete(rootManagement + initBarrier,0);
                    LOGGER.info("Quorum reached");
                }
                else if(numberOfDHTs>=Quorum){
                    LOGGER.info("No more servers needed");
                }
                //Miramos si se ha caido algun nodo de los de la DHT
                for(int i = 0; i<DHTs.length; i++){
                    if(DHTs[i] != null ){
                        Boolean exists = false;
                        for(Iterator<String> iterator = list.iterator(); iterator.hasNext();){
                            if(iterator.next().equals(DHTs[i])){
                                exists =  true;
                                break;
                            }
                        } //Asignamos lider temporal
                        if(!exists){
                            LOGGER.info("Node "+ DHTs[i] + " has failed. Looking for leader for table " + i);
                            numberOfDHTs--;
                            String copyDHT = DHTs[i]; //Guardamos la copia para buscar si era lider temporal
                            DHTs[i] = null; //Hay que borrarlo para que asigne bien los temporales
                            assignTemporalLeader(i);
                            for(int j =0; j<temporalLeaders.length; j++){ //Si era lider temporal de alguna tabla
                                if(copyDHT.equals(temporalLeaders[j])){
                                    LOGGER.info("Node " + copyDHT + " was also temporal leader of table " + j);
                                    LOGGER.info("Looking for new leader");
                                    assignTemporalLeader(j);
                                }
                            }
                        }
                    }
                }

            } catch (Exception e) {
                LOGGER.warning(e.toString());
                LOGGER.warning("Exception: watcherMember");
            }
        }
    };

    /* ---------------------Functions for operations---------------------------------  */

    /**
     * This variable creates a new watcher. It is fired when a child of "operations"
     * is created or deleted.
     */
    private Watcher operationsWatcher = new Watcher() {
        public void process(WatchedEvent event) {
          LOGGER.info("------------------ ClusterManager: Watcher for operations ------------------\n");
            try {
                List<String> list = zk.getChildren(rootOperations,  operationsWatcher);
                printListMembers(list);
            } catch (Exception e) {
                LOGGER.warning("Exception: operationsWatcher");
            }
        }
    };


    /* ---------------------Functions for management---------------------------------  */

    /**
     * This variable creates a new watcher. It is fired when a child of "management"
     * is created or deleted.
     */
    private Watcher managementWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------ClusterManager:Watcher for management------------------\n");
            try {
                List<String> list = zk.getChildren(rootManagement,  managementWatcher);
                printListMembers(list);
            } catch (Exception e) {
                LOGGER.info("Exception: operationsWatcher");
            }
        }
    };

    /**
     * Asigna las tablas de la siguiente manera: si tenemos 3 tablas y replicacion 2 el primer servidor es el lider de la 0 y replica de 1 y 2.
     * el segundo lider de la 1 y replica de 2 y 0 y el tercero lider de 2 y replica de 1 y 2.
     * @param member Id de la DHT para la que se genera el assignment
     * @return El assigment generado
     */


    private TableAssigment assignTable(String member){
        int[] replicas = new int[replicationFactor];
        //Calculamos lo indicado antes.

        int table=0; //Buscamos la primera tabla sin nadie asignado
        for(int i=0; i<DHTs.length; i++){
            if(DHTs[i] == null){
                table = i;
                LOGGER.info("First empty table is "+i);
                break;
            }
        }

        int replica = table;
        for(int i= 0; i < replicationFactor; i++){
            if((replica+1)<Quorum){
                replica++;
                replicas[i]= replica;
            }else{
                replica=0;
                replicas[i]= replica;
            }
        }

        TableAssigment assigment = new TableAssigment(table,replicas,member);
        LOGGER.info("Created table assigment with:");
        LOGGER.info("Leader: " + assigment.getTableLeader());
        for (int replicaServer: assigment.getTableReplicas()) {
            LOGGER.info("Replica : " + replicaServer);
        }
        return assigment;
    }

    /**
     * Escribe el Assigment en zookeeper
     * @param assigment Assigment a escribir
     * @return Numero de la tabla a la que se le ha asignador el lider
     */
    private int writeTableAssigment(TableAssigment assigment){
        byte[] data = SerializationUtils.serialize(assigment); //Assignment -> byte[]
        if (zk != null) {
            try {
                // Si el nodo management existe añadimos un hijo con nuestra operacion
                String response = new String();
                Stat s = zk.exists(rootManagement + tableAssignments , managementWatcher);
                if (s != null) {
                    // Created the znode, if it is not created.
                    LOGGER.info("Creating assignment node");
                    List<String> list = zk.getChildren(rootManagement + tableAssignments, null, s);
                    response = zk.create(rootManagement + tableAssignments +aTable, data,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); //Nodo secuencial efimero
                    LOGGER.info(response);
                    return assigment.getTableLeader();
                } else {
                    LOGGER.warning("Node table doesnt exists");
                }
            } catch (KeeperException e) {
                LOGGER.severe("The session with Zookeeper failes. Closing");
                LOGGER.warning(e.toString());
            } catch (InterruptedException e) {
                LOGGER.warning("InterruptedException raised");
            }
        }
        return 0;
    }

    /**
     *
     * @param assigment Assigment temporal a escribir
     * @return Numero de la tabla a la que se le ha asignador el lider temporal
     */
    private int writeTemporalTableAssigment(TableTemporalAssignment assigment){
        byte[] data = SerializationUtils.serialize(assigment); //Assignment -> byte[]
        if (zk != null) {
            try {
                // Si el nodo management existe añadimos un hijo con nuestra operacion
                String response = new String();
                Stat s = zk.exists(rootManagement + temporalAssignments , managementWatcher);
                if (s != null) {
                    // Created the znode, if it is not created.
                    LOGGER.info("Creating assignment node");
                    zk.getChildren(rootManagement + temporalAssignments, null, s);
                    response = zk.create(rootManagement + temporalAssignments +aTable, data,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); //Nodo secuencial efimero
                    LOGGER.fine(response);
                    return assigment.getTableLeader();
                } else {
                    LOGGER.warning("Node temporal table doesnt exists");
                }
            } catch (KeeperException e) {
                LOGGER.severe("The session with Zookeeper failes. Closing");
                LOGGER.warning(e.toString());
            } catch (InterruptedException e) {
                LOGGER.warning("InterruptedException raised");
            }
        }
        return 0;
    }

    /**
     * Crea un asignacion temporal de lider
     * @param i Numero de la tabla para la que hay que generar el assigment
     */
    private void assignTemporalLeader(int i) {
        int position = i;
        Boolean replicaFound = false;
        for(int j= 0; j < replicationFactor; j++){
            position--;
            if((position)<0){
                position = Quorum-1;
            }
            if(DHTs[position] != null){ //Buscamos un DHT conectgado
                temporalLeaders[i] = DHTs[position];
                LOGGER.info("Temporal leader is: " + DHTs[position]);
                replicaFound = true;
                break;
            } else{
                LOGGER.info("Replica "+ (j+1) + " is down, looking for next one");
            }

        }
        if(replicaFound){
            TableTemporalAssignment assigment = new TableTemporalAssignment(i,DHTs[position]);
            writeTemporalTableAssigment(assigment);
        }else{
            LOGGER.info("Replica for could not be found. Table " + i + " lost");
        }

    }




    /* ---------------------Main and common functions functions ---------------------------------  */

    /**
     * Print a list of the nodes
     * @param list The list to be printed
     */
    private void printListMembers (List<String> list) {
        LOGGER.info("Remaining # members:" + list.size());
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
            String string = (String) iterator.next();
            LOGGER.info(string + ", ");
        }
    }

    //Esto es para implementar la clase watcher(lo pide por algun motivo) y que no se nos mueran los procesos.
    @Override
    public void process(WatchedEvent event) {
        try {
            LOGGER.warning("Unexpected invocated this method. Process of the object");
        } catch (Exception e) {
            LOGGER.warning("Unexpected exception. Process of the object");
        }
    }


    public static void main(String[] args) {

        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s %n");
        LOGGER.setLevel(Level.INFO);

        int Q ;
        int R;
        LOGGER.fine(String.valueOf(args.length));
        if(args.length >= 1){
            Q = Integer.parseInt(args[0]);
        }else {
            Q = 4;
        }

        if(args.length >=2){
            R = Integer.parseInt(args[1]);
        }else {
            R = 2;
        }
        LOGGER.info("Quorum is " + Q );
        LOGGER.info("Replication factor is " + R);
        if(R>Q){
            LOGGER.severe("Replication factor can not be greater than quorum");
            System.exit(2);
        }


        ClusterManager clusterManager = new ClusterManager(Q,R,null,null);
        clusterManager.init();
        LOGGER.info("Inited manager");
        try {
            Thread.sleep(300000000);
        } catch (Exception e) {
            LOGGER.info("Exception in the sleep in main");
        }
    }
}


