package es.upm.dit.dscc.DHT;

import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;

import java.util.Iterator;
import java.util.List;
import java.util.Random;


public class ClusterManager extends Thread {

    //Zookeeper configuration
    String[] hosts;
    private ZooKeeper zk;
    private static String rootMembers;
    private static String rootManagement;
    private static String rootOperations;
    private static String manager;
    Integer mutexBarrier = -1;
    private String myId;
    private static final int SESSION_TIMEOUT = 5000;

    ClusterManager(){
        Common c = new Common();
        rootMembers = c.rootMembers;
        rootOperations = c.rootOperations;
        manager = c.manager;
        rootManagement = c.rootManagement;
        hosts = c.hosts;
    }

    //Number of servers needed
    private int Quorum; //TODO Definir esto

    //Logger configuration TODO Configurar esto bien
    static Logger logger = Logger.getLogger(ClusterManager.class);
    String userDirectory = System.getProperty("user.dir");
    String log4jConfPath = userDirectory + "/src/main/java/es/upm/dit/dscc/DHT/log4j.properties";
    boolean config_log4j = false;


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


                // Create a znode for registering as member and get my id
                myId = zk.create(rootMembers + manager, new byte[0],
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                myId = myId.replace(rootMembers + "/", "");

                // Get the children of a node and set a watcher
                List<String> list = zk.getChildren(rootMembers, membersWatcher, s); //this, s);
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


    /**
     * This variable creates a new watcher. It is fired when a child of "members"
     * is created or deleted.
     */
    private Watcher  membersWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------ClusterManager:Watcher for  members------------------\n");
            try {
                List<String> list = zk.getChildren(rootMembers,  membersWatcher);
                printListMembers(list);
            } catch (Exception e) {
                System.out.println("Exception: watcherMember");
            }
        }
    };

    //Watcher for operations events
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

    //Watcher for management events
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


    /**
     * Print a list of the nodes of memberes
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

    public static void main(String[] args) throws InterruptedException {
        ClusterManager clusterManager = new ClusterManager();
        clusterManager.init();
        System.out.println("Inited manager");
        DHT dht1 = new DHT();
        System.out.println("New DHT");
        dht1.init();
        System.out.println("Inited");
        dht1.sendOperation(OperationEnum.PUT_MAP,"AAAA",new DHT_Map("a",2));
        System.out.println("Operation sended");
        while (true){
            sleep(1000);
        }
    }
}


