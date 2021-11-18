package es.upm.dit.dscc.DHT;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.*;

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
    private static String initBarrier;
    private static String quorumRoute;

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
    private boolean listenPetitions = false; //Si se ha llegado al quorum empiezo a contestar.
    private int Quorum;
    private HashMap<Integer, DHTUserInterface> DHTTables = new HashMap<Integer, DHTUserInterface>();

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
        quorumRoute = Common.quorumRoute;
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
                byte[] quorum = zk.getData(rootManagement+quorumRoute,null,null);
                Quorum = ByteBuffer.wrap(quorum).getInt();
                System.out.println("Quourm for this session is "+ Quorum);
                for(int j = 0; j< Quorum; j++){
                    DHTTables.put(j,new DHTHashMap());
                }
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
                temporalLeaderOfTables = new boolean[Quorum];
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
                        leaderAnswer(oper,string);

                    }else if(temporalLeaderOfTables[oper.getTableId()]){
                        System.out.println("I am the temporal leader of the table and should listen to operation");
                        leaderAnswer(oper,string);
                    }
                    else if(ArrayUtils.contains(myReplicas,oper.getTableId())){ //Si somos replica de la tabla
                        if(oper.getOperation() == OperationEnum.PUT_MAP){
                            System.out.println("I am replica and should execute put");
                            putOperation(oper.getMap().getKey(),oper.getMap().getValue(),false);
                        } else if(oper.getOperation() == OperationEnum.GET_MAP){
                            System.out.println("I am replica and should not execute get");
                        } else if(oper.getOperation() == OperationEnum.REMOVE_MAP){
                            System.out.println("I am replica and should execute remove");
                            removeOperation(oper.getMap().getKey(),false);
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
                        if(dhtResponse.getMap() != null){
                            System.out.println("Response for petition "+response+ "is "+ dhtResponse.getMap().getValue() );
                            System.out.println(dhtResponse.getMap().getValue());
                        }else{
                            System.out.println("Emptry response for petition "+response);
                        }

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

    private void leaderAnswer(DHTPetition petition, String operation){
        switch (petition.getOperation()){
            case GET_MAP:
                DHT_Map map = getOperation(petition.getMap().getKey()).getMap();
                sendResponse(new DHTResponse(getPos(petition.getMap().getKey()),map),operation);
                break;
            case PUT_MAP:
                putOperation(petition.getMap().getKey(), petition.getMap().getValue(),false);
                break;
            case REMOVE_MAP:
                removeOperation(petition.getMap().getKey(),false);
                break;
            default:
                System.out.println("Wrong operation");
        }
    }

    //Escribe un nodo con una operacion
    private void sendResponse(DHTResponse res,String operation){
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
    /* ---------------------Insert and get functions ---------------------------------  */

    private Integer getPos (String key) {

        int hash =	key.hashCode();
        if (hash < 0) {
            System.out.println("Hash value is negative!!!!!");
            hash = -hash;
        }

        int segment = Integer.MAX_VALUE / (Quorum); // No negatives

        for(int i = 0; i < Quorum; i++) {
            if (hash >= (i * segment) && (hash <  (i+1)*segment)){
                return i;
            }
        }

        System.out.println("getPos: This sentence shound not run");
        return 1;

    }

    private void addDHT(DHTUserInterface table, int pos) {DHTTables.put(pos, table);}

    private DHTUserInterface getDHT(String key) {
        return DHTTables.get(getPos(key));
    }

    private ScannerAnswer putOperation(String key, int value,boolean broadcast){
        int position = getPos(key);
        if(isLocalTable(position)){
            System.out.println("Local operation");
                getDHT(key).put(new DHT_Map(key, value));
                if(broadcast) {
                    sendOperation(OperationEnum.PUT_MAP, position, new DHT_Map(key, value));
                }
                return new ScannerAnswer(ScannerAnswerEnum.ANSWER,null);
        }else {
            System.out.println("Getting  answer");
            sendOperation(OperationEnum.PUT_MAP,position,new DHT_Map(key,value));
            return new ScannerAnswer(ScannerAnswerEnum.EXTERNAL_PETITION,null);
        }
    }

    private ScannerAnswer getOperation(String key){
        int position = getPos(key);
        if(isLocalTable(position)){
            System.out.println("Local operation");
            if(getDHT(key).containsKey(key)) {
                System.out.println("Getting key  "+key);
                getDHT(key).get(key);
                return new ScannerAnswer(ScannerAnswerEnum.ANSWER,new DHT_Map(key,getDHT(key).get(key)));
            }else{
                System.out.println("Key does not exist");
                return new ScannerAnswer(ScannerAnswerEnum.NO_KEY,null);
            }
        }else {
            System.out.println("Getting  answer");
            sendOperation(OperationEnum.GET_MAP,position,new DHT_Map(key,0));
            return new ScannerAnswer(ScannerAnswerEnum.EXTERNAL_PETITION,null);
        }
    }

    private ScannerAnswer removeOperation(String key,boolean broadcast){
        int position = getPos(key);
        if(isLocalTable(position)){
            System.out.println("Local operation");
            if(getDHT(key).containsKey(key)) {
                System.out.println("Removed key "+key);
                getDHT(key).remove(key);
                if(broadcast){
                    sendOperation(OperationEnum.REMOVE_MAP,position,new DHT_Map(key,0));
                }
                return new ScannerAnswer(ScannerAnswerEnum.ANSWER,null);
            }else{
                System.out.println("Got asked to remove non existing local key: "+key);
                return new ScannerAnswer(ScannerAnswerEnum.NO_KEY,null);
            }
        }else {
            System.out.println("Getting  answer");
            sendOperation(OperationEnum.REMOVE_MAP,position,new DHT_Map(key,0));
            return new ScannerAnswer(ScannerAnswerEnum.EXTERNAL_PETITION,null);
        }
    }

    private boolean isLocalTable(int position){
        if(position == leaderOfTable){
            return true;
        } else {
            for (int i= 0; i< myReplicas.length ; i++){
                if(myReplicas[i]==position){
                    return true;
                }
            }
            return false;
        }
    }

    /* ---------------------Main and common functions ---------------------------------  */

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

        boolean correct = false;
        int     menuKey = 0;
        boolean exit    = false;
        Scanner sc      = new Scanner(System.in);
        String  key    = null;
        Integer value   = 0;
        ScannerAnswer answer;


        while (!exit) {
            try {
                correct = false;
                menuKey = 0;
                while (!correct) {
                    System. out .println(">>> Enter option: 1) Put. 2) Get. 3) Remove.0) Exit");
                    if (sc.hasNextInt()) {
                        menuKey = sc.nextInt();
                        correct = true;
                    } else {
                        sc.next();
                        System.out.println("The provised text provided is not an integer");
                    }

                }

                switch (menuKey) {
                    case 1: // Put
                        System. out .print(">>> Enter name (String) = ");
                        key = sc.next();

                        System. out .print(">>> Enter account number (int) = ");
                        if (sc.hasNextInt()) {
                            value = sc.nextInt();
                            dht1.putOperation(key,value,true);
                        } else {
                            System.out.println("The provised text provided is not an integer");
                            sc.next();
                        }
                    break;

                    case 2: // Get
                        System. out .print(">>> Enter key (String) = ");
                        key    = sc.next();
                        answer  = dht1.getOperation(key);
                        if (answer.typeAnswer == ScannerAnswerEnum.ANSWER) {
                            value = answer.getMap().getValue();
                            System.out.println(value);
                        } else if (answer.typeAnswer == ScannerAnswerEnum.EXTERNAL_PETITION){
                            System.out.println("The key: " + key + " does not exist");
                        } else if (answer.typeAnswer == ScannerAnswerEnum.NO_KEY){
                            System.out.println("The key: " + key + " does not exist");
                        }

                        break;
                    case 3: // Remove
                        System. out .print(">>> Enter key (String) = ");
                        key    = sc.next();
                        //if (dht.containsKey(key)) {
                        answer  = dht1.removeOperation(key,true);
                        if (answer.typeAnswer == ScannerAnswerEnum.ANSWER) {
                            System.out.println("The key: " + key + " has been removed");
                        } else if (answer.typeAnswer == ScannerAnswerEnum.EXTERNAL_PETITION){
                            System.out.println("The key: " + key + " does not exist");
                        } else if (answer.typeAnswer == ScannerAnswerEnum.NO_KEY){
                            System.out.println("The key: " + key + " does not exist");
                        }
                        break;
                    case 0:
                        exit = true;
                        //dht.close();
                    default:
                        break;
                }
            } catch (Exception e) {
                System.out.println("Exception at Main. Error read data");
                System.err.println(e);
                e.printStackTrace();
            }

        }
        sc.close();


    }


}
