package es.upm.dit.dscc.DHT;

import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Iterator;
import java.util.List;
import java.util.Random;


//La clase de DHT
public class DHT extends Thread{

    String[] hosts;
    private ZooKeeper zk;
    private static String rootMembers;
    private static String rootManagement;
    private static String rootOperations ;
    private static String aMember;
    private static String aOperation;
    Integer mutexBarrier = -1;
    private static final int SESSION_TIMEOUT = 5000;
    private String myId;

    DHT(){
        Common c = new Common();
        rootMembers = c.rootMembers;
        rootOperations = c.rootOperations;
        aMember = c.aMember;
        rootManagement = c.rootManagement;
        hosts = c.hosts;
        aOperation = c.aOperation;
    }

    public void init(){
        Random rand = new Random();
        int i = rand.nextInt(hosts.length);

        try {
            if (zk == null) {
                zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT,cWatcher);
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

        if (zk != null) {
            // Create a folder for members and include this process/server
            try {
                // Create a znode for registering as member and get my id
                System.out.println("Registering DHT");
                myId = zk.create(rootMembers + aMember, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

                myId = myId.replace(rootMembers + "/", "");
            } catch (KeeperException e) {
                System.out.println("The session with Zookeeper failes. Closing");
                return;
            } catch (InterruptedException e) {
                System.out.println("InterruptedException raised");
            }

        }

    }

    //Escribe un nodo con una operacion
    public void sendOperation(OperationEnum _operation,  String _GUID, DHT_Map _map){
        Operation operation = new Operation(_operation,_GUID,_map);
        byte[] data = SerializationUtils.serialize(operation); //Operation -> byte[]

        if (zk != null) {
            try {
                // Si el nodo operacion existe añadimos un hijo con nuestra operacion
                String response = new String();
                Stat s = zk.exists(rootOperations, operationsWatcher);
                if (s != null) {
                    // Created the znode, if it is not created.
                    response = zk.create(rootOperations+aOperation, data,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); //Nodo secuencial efimero
                    System.out.println(response);
                    List<String> list = zk.getChildren(rootOperations, operationsWatcher, s); //Esto es para poner el watcher
                }else{
                    System.out.println("Node operations doesn't exists");
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

   //Cuando se añaden operaciones a operations
    private Watcher operationsWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------Watcher for operations------------------\n");
            System.out.println("------------------"+myId+"------------------\n");

            try {
                List<String> list = zk.getChildren(rootOperations,  operationsWatcher); //Coje la lista de nodos
                printOperations(list); //Imprime cada uno
            } catch (Exception e) {
                System.out.println("Exception: operationsWatcher");
            }
        }
    };


    //Recibe la lista de operaciones y va a cada nodo a recibir la info
    private void printOperations (List<String> list) throws InterruptedException, KeeperException {
        Stat stat = new Stat();
        System.out.println("Operations # members:" + list.size());
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
            String string = (String) iterator.next();
            System.out.println(string);
            byte[] data = zk.getData(rootOperations +"/"+ string, null, stat); //Leemos el nodo
            Operation oper = (Operation) SerializationUtils.deserialize(data); //byte -> Order
            System.out.print("\nOperation " + oper.getOperation().toString());
            System.out.print("\nOperation " + oper.getGUID());
        }
        System.out.println();
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

    public static void main(String[] args) throws InterruptedException {
        while (true){
            sleep(1000);
        }
    }

}
