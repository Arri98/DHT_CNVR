package es.upm.dit.dscc.DHT;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

//La clase de DHT
public class DHT implements Watcher{

    String[] hosts;
    private ZooKeeper zk;
    //Strings de rutas
    private static String rootMembers;
    private static String rootManagement;
    private static String rootOperations ;
    private static String aMember;
    private static String aOperation;
    private static String tableAssignments;
    private static String responses;
    private static String temporalAssignments;

    //Ultima operacion escuchada para no repetir
    private int lastOperation=-1; //Ultima operacion escuchada, por si me llegan repetidas antes de que alguien borre.
    private List<String> myPetitions =  new ArrayList(); //Peticiones que he hecho de las que espero respuesta

    private TableManager tableManager; //El manager encargado de devolver la tabla correspondiente a una clave
    private String localAdress; //La direccion local de la tabla
    Integer mutexBarrier = -1;
    private static final int SESSION_TIMEOUT = 5000;
    private String myId;
    private int leaderOfTable; //De que tabla contesto gets
    private boolean[] temporalLeaderOfTables; //Tablas de las que soy lider pero no deberia
    private int[] myReplicas;
    private static String initBarrier;
    private boolean listenPetitions = false; //Si se ha llegado al quorum empiezo a contestar.

    DHT(){
        rootMembers = Common.rootMembers;
        rootOperations = Common.rootOperations;
        aMember = Common.aMember;
        rootManagement = Common.rootManagement;
        hosts = Common.hosts;
        aOperation = Common.aOperation;
        tableAssignments = Common.tableAssignments;
        initBarrier = Common.initBarrier;
        responses = Common.responses;
        temporalAssignments = Common.temporalAssignments;
    }

    /* ---------------------Initialization ---------------------------------  */

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


                List<String> l= zk.getChildren(rootManagement, managementWatcher);
                zk.getChildren(rootOperations, operationsWatcher);
                zk.getChildren(rootOperations+responses, responseWatcher);
                zk.getChildren(rootManagement+temporalAssignments, temportalAssignmentWatcher);
                l = zk.getChildren(rootManagement + tableAssignments, tableWatcher);
                myId = myId.replace(rootMembers + "/", "");
                processAssignments(l);
                System.out.println("I am: "+myId);
            } catch (KeeperException e) {
                System.out.println("The session with Zookeeper failes. Closing");
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

    /* ---------------------Functions for management---------------------------------  */

    //Watcher for table assignment events
    private Watcher tableWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------DHT:Watcher for table assignments------------------\n");
            try {
                List<String> list = zk.getChildren(rootManagement+ tableAssignments ,  tableWatcher);
                processAssignments(list);
            } catch (Exception e) {
                System.out.println(e);
                System.out.println("Exception: tableWatcher");
            }
        }
    };




    //Watcher for management events
    private Watcher managementWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------DHT:Watcher for management------------------\n");
            try {
                List<String> list = zk.getChildren(rootManagement,  managementWatcher);
                boolean barrierUp = false;
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
                    String member = iterator.next();
                    if(member.equals(initBarrier)){
                        barrierUp = true;
                    }
                }
                if(!barrierUp){
                    System.out.println("Quorum reached, listening petitions now");
                    listenPetitions = true;
                }
            } catch (Exception e) {
                System.out.println("Exception: managementWatcher");
            }
        }
    };

    //Leemos la clase asigment y vemos si tenemos que actualizar algo
    private void processAssignments(List<String> list) throws InterruptedException, KeeperException {
        Stat s = new Stat();
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
            String string = (String) iterator.next();
            byte[] data = zk.getData(rootManagement+tableAssignments+"/"+ string, null, s); //Leemos el nodo
            TableAssigment assigment = (TableAssigment) SerializationUtils.deserialize(data); //byte -> Order
            if(assigment.getDHTId().equals(myId)){ //Si hay alguna peticion para mi
                System.out.println("Assignment for me: "+ myId);
                System.out.println("I am leader for table:"+ assigment.getTableLeader());
                leaderOfTable = assigment.getTableLeader(); //Soy el lider de la tabla
                System.out.print("And replica for tables: ");
                myReplicas = assigment.getTableReplicas(); //Mis replicas son
                temporalLeaderOfTables = new boolean[myReplicas.length];
                for(int i=0; i<assigment.getTableReplicas().length; i++) {
                    System.out.print(assigment.getTableReplicas()[i]+ " ");
                }
                System.out.println("\n");
                zk.delete(rootManagement+tableAssignments+"/"+ string,s.getVersion());
            }else if(temporalLeaderOfTables[assigment.getTableLeader()]){
                System.out.println("There is a new leader for table : "+ assigment.getTableLeader());
                System.out.println("My temporal work here is done");
                temporalLeaderOfTables[assigment.getTableLeader()] = false;
            }
            else{
                System.out.println("I am "+ myId + " and this is for " + assigment.getDHTId() );
            }
        }
    }

    //Watcher for table assignment events
    private Watcher temportalAssignmentWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------DHT:Watcher for temporal table assignments------------------\n");
            try {
                List<String> list = zk.getChildren(rootManagement+ temporalAssignments ,  temportalAssignmentWatcher);
                Stat s = new Stat();
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
                    String string = (String) iterator.next();
                    byte[] data = zk.getData(rootManagement+ temporalAssignments+"/"+ string, null, s); //Leemos el nodo
                    TableTemporalAssignment assigment = (TableTemporalAssignment) SerializationUtils.deserialize(data); //byte -> Order
                    if(assigment.getDHTId().equals(myId)){ //Si hay alguna peticion para mi
                        System.out.println("Temporal ssignment for me: "+ myId);
                        System.out.println("I am temporal leader for table:"+ assigment.getTableLeader());
                        temporalLeaderOfTables[ assigment.getTableLeader()] = true;
                        System.out.println("\n");
                        zk.delete(rootManagement+ temporalAssignments+"/"+ string,s.getVersion());
                    }else{
                        System.out.println("I am "+ myId + " and this temporal assignment is for " + assigment.getDHTId() );
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
                System.out.println("Exception: tableWatcher");
            }
        }
    };



    /* ---------------------Functions for operations---------------------------------  */

    //Recibe la lista de operaciones y va a cada nodo a recibir la info
    private void printOperations (List<String> list) throws InterruptedException, KeeperException {
        Stat stat = new Stat();
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
            String string = (String) iterator.next();
            if(!string.equals("responses")){
                System.out.println(string);
                String editedString = string.replace("operation-","");
                int operationNumber = Integer.parseInt(editedString);
                System.out.println(operationNumber);
                int maxNewOperation = -1;
                if(operationNumber>lastOperation){
                    if(operationNumber>maxNewOperation){
                        maxNewOperation = operationNumber;
                    }
                    lastOperation=operationNumber;
                    byte[] data = zk.getData(rootOperations +"/"+ string, null, stat); //Leemos el nodo
                    DHTPetition oper = (DHTPetition) SerializationUtils.deserialize(data); //byte -> Order
                    System.out.print("\nOperation " + oper.getOperation().toString());
                    System.out.print("\nOperation " + oper.getTableId());

                    if(oper.getTableId()== leaderOfTable ){ //Si la peticion va para la tabla de la que soy lider
                        System.out.println("I am the leader of the table and should listen to operation");
                        DHTResponse response = new DHTResponse(1,new DHT_Map("a",2));
                        sendResponse(response,string);
                    }else if(temporalLeaderOfTables[oper.getTableId()]){
                        System.out.println("I am the temporal leader of the table and should listen to operation");
                        DHTResponse response = new DHTResponse(1,new DHT_Map("a",2));
                        sendResponse(response,string);
                    }
                    else if(ArrayUtils.contains(myReplicas,oper.getTableId())){ //Si somos replica de la tabla
                        if(oper.getOperation() == OperationEnum.PUT_MAP){
                            System.out.println("I am replica and should execute put");
                        } else if(oper.getOperation() == OperationEnum.GET_MAP){
                            System.out.println("I am replica and should not execute get");
                        } else if(oper.getOperation() == OperationEnum.REMOVE_MAP){
                            System.out.println("I am replica and should execute remove");
                        }
                    }
                    lastOperation = maxNewOperation;
                }else{
                    System.out.println("Operation "+string+" already listened");
                }


            }
        }
        System.out.println();
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
                System.out.println(e);
                System.out.println("Exception: operationsWatcher");
            }
        }
    };

    //Cuando se añaden responses a operations
    private Watcher responseWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------Watcher for responses------------------\n");
            System.out.println("------------------"+myId+"------------------\n");

            try {
                List<String> list = zk.getChildren(rootOperations + responses,  responseWatcher); //Coje la lista de nodos
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext();){
                    String response = iterator.next();
                    if(myPetitions.contains(response)){
                        Stat s = new Stat();
                        byte[] data = zk.getData(rootOperations + responses+"/"+response,null,s);
                        DHTResponse dhtResponse = (DHTResponse) SerializationUtils.deserialize(data); //byte -> Response
                        System.out.println("Response for petition "+response+ "is "+ dhtResponse.getMap().getValue() );
                        myPetitions.remove(response);
                        zk.delete(rootOperations + responses +"/"+ response,s.getVersion());
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
                System.out.println("Exception: responseWatcher");
            }
        }
    };

    //Escribe un nodo con una operacion
    public void sendOperation(OperationEnum _operation,  int _GUID, DHT_Map _map){
        DHTPetition DHTPetition = new DHTPetition(_operation,_GUID,_map);
        byte[] data = SerializationUtils.serialize(DHTPetition); //Operation -> byte[]

        if (zk != null) {
            try {
                // Si el nodo operacion existe añadimos un hijo con nuestra operacion
                String response = new String();
                Stat s = zk.exists(rootOperations, null);
                if (s != null) {
                    // Created the znode, if it is not created.
                    List<String> list = zk.getChildren(rootOperations, null, s);
                    response = zk.create(rootOperations+aOperation, data,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); //Nodo secuencial efimero
                    String petitionId = response.replace(rootOperations + "/", "");
                    System.out.println(petitionId);
                    myPetitions.add(petitionId);
                    System.out.println(response);
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


    //Escribe un nodo con una operacion
    public void sendResponse(DHTResponse res,String operation){
        byte[] data = SerializationUtils.serialize(res); //Response -> byte[]

        if (zk != null) {
            try {
                // Si el nodo operacion existe añadimos un hijo con nuestra operacion
                String response = new String();
                Stat s = zk.exists(rootOperations+responses, null);
                if (s != null) {
                    // Created the znode, if it is not created.
                    List<String> list = zk.getChildren(rootOperations, null, s);
                    response = zk.create(rootOperations+responses+"/"+operation, data,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL); //Nodo  efimero
                    System.out.println(response);
                }else{
                    System.out.println("Node responses doesn't exists");
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



    /* ---------------------Main and common functions functions for management---------------------------------  */

    @Override
    public void process(WatchedEvent event) {
        try {
            System.out.println("Unexpected invocated this method. Process of the object");
        } catch (Exception e) {
            System.out.println("Unexpected exception. Process of the object");
        }
    }


    public static void main(String[] args) throws InterruptedException {
        DHT dht1 = new DHT();
        System.out.println("New DHT");
        dht1.init();
        System.out.println("Inited");
        Thread.sleep(2000);
        dht1.sendOperation(OperationEnum.PUT_MAP,1,new DHT_Map("a",2));
        System.out.println("Operation sended");

        try {
            Thread.sleep(30000000);
        } catch (Exception e) {
            System.out.println("Exception in the sleep in main");
        }
    }


}
