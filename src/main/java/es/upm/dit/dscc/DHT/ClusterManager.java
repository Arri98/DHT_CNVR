package es.upm.dit.dscc.DHT;

import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;

import java.util.Iterator;
import java.util.List;
import java.util.Random;


public class ClusterManager implements Watcher {

    //Zookeeper configuration
    String[] hosts;
    private ZooKeeper zk;
    private static String rootMembers;
    private static String rootManagement;
    private static String rootOperations;
    private static String manager;
    private static String aTable;
    private static String tableAssignments;
    Integer mutexBarrier = -1;
    private String myId;
    private static final int SESSION_TIMEOUT = 5000;
    private String[] DHTs;
    private int numberOfDHTs = 0;
    private int replicationFactor;

    //Number of servers needed
    private int Quorum; //TODO Definir esto

    //Logger configuration TODO Configurar esto bien
    static Logger logger = Logger.getLogger(ClusterManager.class);
    String userDirectory = System.getProperty("user.dir");
    String log4jConfPath = userDirectory + "/src/main/java/es/upm/dit/dscc/DHT/log4j.properties";
    boolean config_log4j = false;


    ClusterManager(int Quorum,int replicationFactor){
        Common c = new Common();
        rootMembers = c.rootMembers;
        rootOperations = c.rootOperations;
        manager = c.manager;
        rootManagement = c.rootManagement;
        hosts = c.hosts;
        aTable = c.aTable;
        this.Quorum = Quorum;
        DHTs = new String[Quorum];
        this.replicationFactor = replicationFactor;
        tableAssignments = c.tableAssignments;
    }

    /* ---------------------Initialization ---------------------------------  */

    public void init(){
        //Log4j config
        if (config_log4j) {
            try {
                System.out.println(log4jConfPath);
                // Configure Logger
                //BasicConfigurator.configure();
                PropertyConfigurator.configure(log4jConfPath);
                logger.setLevel(Level.TRACE);//(Level.INFO);
            } catch (Exception E){
                System.out.println("The path of log4j.properties is not correct");
            }
        }
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
                    System.out.println("Exception in the wait while creating the session");
                }
            }
        } catch (Exception e) {
            System.out.println("Exception while creating the session");
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
                    System.out.println(response);
                }

                // Create a node with the name "operations", if it is not created
                s = zk.exists(rootOperations, operationsWatcher);
                if (s == null) {
                    response = zk.create(rootOperations, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println(response);
                }

                // Create a node with the name "management", if it is not created
                s = zk.exists(rootManagement, managementWatcher);
                if (s == null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootManagement, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println(response);
                }

                // Create a node with the name tableAssingmets, if it is not created
                s = zk.exists(rootManagement + tableAssignments, null);
                if (s == null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootManagement + tableAssignments, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println(response);
                }


                // Create a znode for registering as member and get my id
                myId = zk.create(rootMembers + manager, new byte[0],
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                myId = myId.replace(rootMembers + "/", "");

                // Get the children of a node and set a watcher
                List<String> list = zk.getChildren(rootMembers, membersWatcher, s);
                System.out.println("Created znode nember id:"+ myId );
                printListMembers(list);

            } catch (KeeperException e) {
                System.out.println("The session with Zookeeper failes. Closing");
                System.out.println(e);
                return;
            } catch (InterruptedException e) {
                System.out.println("InterruptedException raised");
            }

        }
    }

     /**
     * This variable creates a new watcher. It is fired when the session
     * is created
     */
    private Watcher cWatcher = new Watcher() {
        public void process (WatchedEvent e) {
            System.out.println("Created session");
            System.out.println(e.toString());
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


    private Watcher  membersWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------ClusterManager:Watcher for  members------------------\n");
            try {
                //Miramos en el watcher si tenemos algun nodo nuevo
                List<String> list = zk.getChildren(rootMembers,  membersWatcher);
                printListMembers(list);
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
                    String member = iterator.next();
                    Boolean exists = false;
                    for(int i = 0; i< DHTs.length; i++){
                        if( member.equals( DHTs[i])){
                            exists = true;
                        }
                    }
                    //Si tenemos un nodo nuevo y no hemos llegado al quorum le asignamos su tablas y replicas
                    if(!exists  && !member.equals("manager") && numberOfDHTs<Quorum){
                        writeTableAssigment(assignTable(member,numberOfDHTs));
                        DHTs[numberOfDHTs] = member; //Guardamos el nodo nuevo
                        numberOfDHTs++; //Ahora tenemos un miembro nuevo
                        System.out.println("Added new member "+member);
                    }else if(numberOfDHTs>=Quorum){
                        System.out.println("No more servers needed");
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
                System.out.println("Exception: watcherMember");
            }
        }
    };

    /* ---------------------Functions for operations---------------------------------  */

    //Watcher for operations events, no hacemos nada con ellas por ahora
    private Watcher operationsWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------ ClusterManager: Watcher for operations ------------------\n");
            try {
                List<String> list = zk.getChildren(rootOperations,  operationsWatcher);
                printListMembers(list);
            } catch (Exception e) {
                System.out.println("Exception: operationsWatcher");
            }
        }
    };


    /* ---------------------Functions for management---------------------------------  */

    //Watcher for management events, los escribimos nosotros asi que no escuchamos
    private Watcher managementWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------ClusterManager:Watcher for management------------------\n");
            try {
                List<String> list = zk.getChildren(rootManagement,  managementWatcher);
                printListMembers(list);
            } catch (Exception e) {
                System.out.println("Exception: operationsWatcher");
            }
        }
    };

    //Asigna las tablas de la siguiente manera: si tenemos 3 tablas y replicacion 2 el primer servidor es el lider de la 0 y replica de 1 y 2.
    // el segundo lider de la 1 y replica de 2 y 0 y el tercero lider de 2 y replica de 1 y 2.
    private TableAssigment assignTable(String member,int table){ //Number = number of the leader, table = id of table.
        int[] replicas = new int[replicationFactor];
        //Calculamos lo indicado antes.
        if(table<Quorum){
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
        }
        TableAssigment assigment = new TableAssigment(table,replicas,member);
        System.out.println("Created table assigment with:");
        System.out.println("Leader: " + assigment.getTableLeader());
        for (int replica: assigment.getTableReplicas()) {
            System.out.println("Replica : " + replica);
        }
        return assigment;
    }
    //Escribimos la asignacion
    private void writeTableAssigment(TableAssigment assigment){
        byte[] data = SerializationUtils.serialize(assigment); //Assignment -> byte[]
        if (zk != null) {
            try {
                // Si el nodo management existe añadimos un hijo con nuestra operacion
                String response = new String();
                Stat s = zk.exists(rootManagement + tableAssignments , managementWatcher);
                if (s != null) {
                    // Created the znode, if it is not created.
                    System.out.println("Creating assignment node");
                    List<String> list = zk.getChildren(rootManagement + tableAssignments, null, s);
                    response = zk.create(rootManagement + tableAssignments +aTable, data,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); //Nodo secuencial efimero
                    System.out.println(response);
                } else {
                    System.out.println("Node table doesnt exists");
                }
            } catch (KeeperException e) {
                System.out.println("The session with Zookeeper failes. Closing");
                System.out.println(e);
                return;
            } catch (InterruptedException e) {
                System.out.println("InterruptedException raised");
            }
        }
    }


    /* ---------------------Main and common functions functions ---------------------------------  */

    /**
     * Print a list of the nodes
     * @param list The list to be printed
     */
    private void printListMembers (List<String> list) {
        System.out.println("Remaining # members:" + list.size());
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
            String string = (String) iterator.next();
            System.out.print(string + ", ");
        }
        System.out.println();

    }


    //Esto es para implementar la clase watcher(lo pide por algun motivo) y que no se nos mueran los procesos.
    @Override
    public void process(WatchedEvent event) {
        try {
            System.out.println("Unexpected invocated this method. Process of the object");
        } catch (Exception e) {
            System.out.println("Unexpected exception. Process of the object");
        }
    }


    public static void main(String[] args) {
        ClusterManager clusterManager = new ClusterManager(3,2);
        clusterManager.init();
        System.out.println("Inited manager");
        try {
            Thread.sleep(300000000);
        } catch (Exception e) {
            System.out.println("Exception in the sleep in main");
        }
    }
}


