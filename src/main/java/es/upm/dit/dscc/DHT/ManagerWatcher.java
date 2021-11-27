package es.upm.dit.dscc.DHT;

import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Level;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Clase que guarda la informacion necesaria para iniciar un manager nuevo y que escucha las asignaciones de lideres
 */

public class ManagerWatcher implements Watcher {

    //Campos que necesita un manager para iniciar
    private int Quorum;
    private int replicationFactor;
    private String[] DHTs;

    //Rutas y zookeeper
    private static final int SESSION_TIMEOUT = 5000;
    private static String rootMembers;
    private static String tableAssignments;
    private static String rootManagement;
    private ZooKeeper zk;
    private String myId;
    String[] hosts;
    Integer mutexBarrier = -1;

    /**
     * Constructor. DHTs y temporalLeaders son opcionales y sirven para iniciar un watcher en un estado determinado
     * @param Quorum Numero de tablas y nodos requeridos
     * @param replicationFactor Numero de copias de cada servidor
     * @param DHTs Lideres de las tablas
     */
    ManagerWatcher(int Quorum,int replicationFactor,String[] DHTs){
        Common c = new Common();
        this.Quorum = Quorum;
        this.replicationFactor = replicationFactor;
        if(DHTs == null){
            this.DHTs = new String[Quorum];
        }else{
            this.DHTs = DHTs;
        }
        this.rootMembers = c.rootMembers;
        hosts = c.hosts;
        this.tableAssignments = c.tableAssignments;
        this.rootManagement = c.rootManagement;
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
                // Create a znode for registering as member and get my id
                myId = zk.create(rootMembers + "/listener", new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                myId = myId.replace(rootMembers + "/", "");
                Stat s = new Stat();
                // Get the children of a node and set a watcher
                List<String> list = zk.getChildren(rootMembers, membersWatcher, s);
                System.out.println("Created znode nember id:"+ myId );
                zk.getChildren(rootManagement+ tableAssignments ,  tableWatcher);
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
     * Watcher de miembros
     */
    private Watcher membersWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------ Manager Watccher: Watcher for members ------------------\n");
            try { //Buscamos si el manager se ha caido
                List<String> list = zk.getChildren(rootMembers,  membersWatcher);
                Boolean managerFound = false;
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
                    if(iterator.next().equals("manager")){
                        managerFound = true;
                    }
                }
                if(managerFound){
                    System.out.println("Manager still up");
                }else{
                    System.out.println("Manager down, should init new one");
                }

            } catch (Exception e) {
                System.out.println("Exception: operationsWatcher");
            }
        }
    };

    /**
     * Watcher for table assignment events
     */

    private Watcher tableWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------Watcher for table assignments------------------\n");
            try { //Nos vamos apuntando los lideres de la tablas para en caso de caida del manager arrancar uno con las tablas
                List<String> list = zk.getChildren(rootManagement+ tableAssignments ,  tableWatcher);
                Stat s = new Stat();
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
                    String string = (String) iterator.next();
                    byte[] data = zk.getData(rootManagement + tableAssignments + "/" + string, null, s); //Leemos el nodo
                    TableAssigment assigment = (TableAssigment) SerializationUtils.deserialize(data); //byte -> Order
                    DHTs[assigment.getTableLeader()] = assigment.getDHTId();
                    System.out.println(assigment.getDHTId() + "is leader for table" + assigment.getTableLeader());
                    System.out.println("Updating in case of manager fail");
                }

            } catch (Exception e) {
                System.out.println("Exception: managementWatcher");
                System.out.println(e);
            }
        }
    };


    //Esto es para implementar la clase watcher(lo pide por algun motivo) y que no se nos mueran los procesos.
    @Override
    public void process(WatchedEvent event) {
        try {
            System.out.println("Unexpected invocated this method. Process of the object");
        } catch (Exception e) {
            System.out.println("Unexpected exception. Process of the object");
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

    public static void main(String[] args) {
        ManagerWatcher w = new ManagerWatcher(3,2,null);
        w.init();
        System.out.println("Inited listener");
        try {
            Thread.sleep(300000000);
        } catch (Exception e) {
            System.out.println("Exception in the sleep in main");
        }
    }

}
