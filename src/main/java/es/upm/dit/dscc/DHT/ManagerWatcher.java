package es.upm.dit.dscc.DHT;

import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clase que guarda la informacion necesaria para iniciar un manager nuevo y que escucha las asignaciones de lideres
 */

public class ManagerWatcher implements Watcher {


    //Campos que necesita un manager para iniciar
    private int Quorum;
    private int replicationFactor;
    private String[] DHTs;
    private String[] temporalLeaders;

    //Rutas y zookeeper
    private static final int SESSION_TIMEOUT = 5000;
    private static String rootMembers;
    private static String tableAssignments;
    private static String rootManagement;
    private static String temporalAssignments;
    private ZooKeeper zk;
    private String myId;
    String[] hosts;
    Integer mutexBarrier = -1;

    private static final Logger LOGGER = Logger.getLogger(DHT.class.getName());

    /**
     * Constructor. DHTs y temporalLeaders son opcionales y sirven para iniciar un watcher en un estado determinado
     * @param Quorum Numero de tablas y nodos requeridos
     * @param replicationFactor Numero de copias de cada servidor
     * @param DHTs Lideres de las tablas
     */
    ManagerWatcher(int Quorum,int replicationFactor,String[] DHTs, String[] temporalLeaders){

        this.Quorum = Quorum;
        this.replicationFactor = replicationFactor;
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
        rootMembers = Common.rootMembers;
        hosts = Common.hosts;
        tableAssignments = Common.tableAssignments;
        rootManagement = Common.rootManagement;
        temporalAssignments = Common.temporalAssignments;
    }

    /**
     * Iniciacion del watcher
     */
    public void init(){
        //Log4j config

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
                    LOGGER.warning("Exception in the wait while creating the session");
                }
            }
        } catch (Exception e) {
            LOGGER.warning("Exception while creating the session");
        }

        // Create zookeeper session if it doesn't exist

        if (zk != null) {
            // Create a folder for members and include this process/server
            try {
                // Create a znode for registering as member and get my id
                myId = zk.create(rootMembers + "/listener", new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                myId = myId.replace(rootMembers + "/", "");
                Stat s = new Stat();
                // Get the children of a node and set a watcher
                List<String> list = zk.getChildren(rootMembers, membersWatcher, s);
                LOGGER.info("Created znode nember id:"+ myId );
                zk.getChildren(rootManagement+ tableAssignments ,  tableWatcher);
                zk.getChildren(rootManagement+ temporalAssignments ,  temporalTablesWatcher);
            } catch (KeeperException e) {
               LOGGER.warning("The session with Zookeeper failes. Closing");
                LOGGER.warning(String.valueOf(e));
                return;
            } catch (InterruptedException e) {
                LOGGER.warning("InterruptedException raised");
            }

        }
    }

    /**
     * Watcher de miembros
     */
    private Watcher membersWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.warning("------------------ Manager Watccher: Watcher for members ------------------\n");
            try { //Buscamos si el manager se ha caido
                List<String> list = zk.getChildren(rootMembers,  membersWatcher);
                Boolean managerFound = false;
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
                    if(iterator.next().equals("manager")){
                        managerFound = true;
                    }
                }
                if(managerFound){
                    LOGGER.info("Manager still up");
                }else{
                    LOGGER.warning("Manager down, should init new one");
                    ClusterManager c1 = new ClusterManager(Quorum, replicationFactor, DHTs, temporalLeaders);
                    c1.init();
                    try {
                        Thread.sleep(300000000);
                    } catch (Exception e) {
                        LOGGER.info("Exception in the sleep in main");
                    }
                }

            } catch (Exception e) {
                LOGGER.warning("Exception: operationsWatcher");
            }
        }
    };

    /**
     * Watcher for table assignment events
     */

    private Watcher tableWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------Watcher for table assignments------------------\n");
            try { //Nos vamos apuntando los lideres de la tablas para en caso de caida del manager arrancar uno con las tablas
                List<String> list = zk.getChildren(rootManagement+ tableAssignments ,  tableWatcher);
                Stat s = new Stat();
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
                    String string = (String) iterator.next();
                    byte[] data = zk.getData(rootManagement + tableAssignments + "/" + string, null, s); //Leemos el nodo
                    TableAssigment assigment = (TableAssigment) SerializationUtils.deserialize(data); //byte -> Order
                    DHTs[assigment.getTableLeader()] = assigment.getDHTId();
                    LOGGER.info(assigment.getDHTId() + "is leader for table" + assigment.getTableLeader());
                    LOGGER.info("Updating in case of manager fail");
                    if(temporalLeaders[assigment.getTableLeader()] != null){
                        temporalLeaders[assigment.getTableLeader()] = null;
                        LOGGER.info("Table " + assigment.getTableLeader() + " no longer has temporal leader");
                    }
                }

            } catch (Exception e) {
                LOGGER.warning("Exception: managementWatcher");
                LOGGER.warning(String.valueOf(e));
            }
        }
    };


    private Watcher temporalTablesWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------Watcher for table temporal table assignments------------------\n");
            try { //Nos vamos apuntando los lideres de la tablas para en caso de caida del manager arrancar uno con las tablas
                List<String> list = zk.getChildren(rootManagement+ temporalAssignments ,  temporalTablesWatcher);
                Stat s = new Stat();
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
                    String string = (String) iterator.next();
                    byte[] data = zk.getData(rootManagement + temporalAssignments + "/" + string, null, s); //Leemos el nodo
                    TableTemporalAssignment assigment = (TableTemporalAssignment) SerializationUtils.deserialize(data); //byte -> Order
                    temporalLeaders[assigment.getTableLeader()] = assigment.getDHTId();
                    DHTs[assigment.getTableLeader()] = null;
                    LOGGER.info(assigment.getDHTId() + "is tempopral leader for table" + assigment.getTableLeader());
                    LOGGER.info("Updating in case of manager fail");
                }
            } catch (Exception e) {
                LOGGER.warning("Exception: managementWatcher");
                LOGGER.warning(String.valueOf(e));
            }
        }
    };



    //Esto es para implementar la clase watcher(lo pide por algun motivo) y que no se nos mueran los procesos.
    @Override
    public void process(WatchedEvent event) {
        try {
            LOGGER.info("Unexpected invocated this method. Process of the object");
        } catch (Exception e) {
            LOGGER.warning("Unexpected exception. Process of the object");
        }
    }

    /**
     * This variable creates a new watcher. It is fired when the session
     * is created
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

    /**
     * Para iniciar el programa con quorum y replication factor personalizado.
     * @param args Args [0] = Quorum (int), args[1] = replication factor(int)
     */
    public static void main(String[] args) {
        int Q ;
        if(args.length >=1){
            Q = Integer.parseInt(args[0]);
        }else {
            Q = 4;
        }
        int R;
        if(args.length >=2){
            R = Integer.parseInt(args[1]);
        }else {
            R = 2;
        }
        if(R>Q){
            LOGGER.severe("Replication factor can not be greater than quorum");
            System.exit(2);
        }

        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s %n");

        LOGGER.setLevel(Level.FINE);
        ManagerWatcher w = new ManagerWatcher(Q,R,null, null);
        w.init();
        LOGGER.info("Inited listener");
        try {
            Thread.sleep(300000000);
        } catch (Exception e) {
            LOGGER.warning("Exception in the sleep in main");
        }
    }

}
