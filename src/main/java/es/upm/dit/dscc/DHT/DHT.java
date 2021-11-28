package es.upm.dit.dscc.DHT;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Nodo de la DHT. Envia y recibe peticiones, integra tablas.
 */
public class DHT extends JFrame implements Watcher, ActionListener {

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
    private static String integration;

    //Ultima operacion escuchada para no repetir
    private int lastOperation=-1; //Ultima operacion escuchada, por si me llegan repetidas antes de que alguien borre.
    private List<String> myPetitions =  new ArrayList(); //Peticiones que he hecho de las que espero respuesta
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
    //Logger configuration
    private static final Logger LOGGER = Logger.getLogger(DHT.class.getName());

    private ServerSocket ss;

    //UI
    private JButton b1;
    private JButton b2;
    private JButton b3;
    private TextField textField1;
    private TextField textField2;
    private TextField textField3;




    /**
     * Constructor
     */
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
        integration = Common.integration;
    }

    /* ---------------------Initialization ---------------------------------  */

    /**
     * Inicialicacion de la conexion a zookeeper.
     */
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
                List<String> l2 = zk.getChildren(rootManagement+integration, integrationWatcher);

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
                Stat s = new Stat();
                for (Iterator<String> iterator = l2.iterator(); iterator.hasNext(); ) {
                    String string = (String) iterator.next();
                    LOGGER.info("Found integration " + string);
                    byte[] data = zk.getData(rootManagement + integration + "/" + string, null, s); //Leemos el nodo
                    TableAssignmentIntegration assigment = (TableAssignmentIntegration) SerializationUtils.deserialize(data); //byte -> Order
                    LOGGER.info("Integration for"+ assigment.getDHTId());
                    if (assigment.getDHTId().equals(myId) && !assigment.isDone()) {
                        LOGGER.info("This integration is for me");
                        LOGGER.info("Socket inited at" + 3000 + leaderOfTable);
                        zk.create(rootManagement + integration + "/" + string + "/" + myId ,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        initSocket(3000 + leaderOfTable);
                        //TODO: REFRACTOR THIS AS FUNCTION        
                        assigment.setDone(true);
                        byte[] dataModified = SerializationUtils.serialize(assigment); //Assignment -> byte[]
                        zk.setData(rootManagement + integration + "/" + string, dataModified ,s.getVersion());
                        LOGGER.fine("Assigment finished");
                        List<String> listChildrens = zk.getChildren(rootManagement + integration + "/" + string, false);
                        
                        for (Iterator<String> iterator2 = listChildrens.iterator(); iterator2.hasNext(); ) {
                        	String node = (String) iterator2.next();
                        	zk.delete(rootManagement + integration + "/" + string + "/" + node,0);
                        }
                        zk.delete(rootManagement + integration + "/" + string,1);
                       
                        
                    }
                }
            } catch (KeeperException e) {
                LOGGER.severe(e.toString());
                LOGGER.severe("The session with Zookeeper failes. Closing");
                return;
            } catch (InterruptedException | IOException | ClassNotFoundException e) {
                LOGGER.severe("InterruptedException raised");
            }

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

    /* ---------------------Functions for management---------------------------------  */


    /**
     * Watcher for table assignment events
     */
    private Watcher tableWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------DHT:Watcher for table assignments------------------\n");
            try {
                List<String> list = zk.getChildren(rootManagement+ tableAssignments ,  tableWatcher);
                processAssignments(list);
            } catch (Exception e) {
                LOGGER.warning(e.toString());
                LOGGER.warning("Exception: tableWatcher");
            }
        }
    };




    /**
     * Watcher for management  events
     */
    private Watcher managementWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------DHT:Watcher for management------------------\n");
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
                    LOGGER.info("Quorum reached, listening petitions now");
                    listenPetitions = true;
                }else{
                    LOGGER.info("Barrier up");
                }
            } catch (Exception e) {
                LOGGER.warning("Exception: managementWatcher");
            }
        }
    };



    /**
     * Watcher for integrations events
     */
    private Watcher integrationWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------DHT:Watcher for integration------------------\n");
            try {
                List<String> list = zk.getChildren(rootManagement+integration,  integrationWatcher);
                Stat s = new Stat();
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
                    String string = (String) iterator.next();
                    LOGGER.info("Found integration " + string);
                    byte[] data = zk.getData(rootManagement + integration + "/" + string, null, s); //Leemos el nodo
                    TableAssignmentIntegration assigment = (TableAssignmentIntegration) SerializationUtils.deserialize(data); //byte -> Order
                    LOGGER.info("Integration for"+ assigment.getDHTId());
                    if (assigment.getDHTId().equals(myId) && !assigment.isDone()) {
                        LOGGER.info("This integration is for me");
                        LOGGER.info("Socket inited at" + 3000 + leaderOfTable);
                        zk.create(rootManagement + integration + "/" + string + "/" + myId ,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        initSocket(3000 + leaderOfTable);
                        assigment.setDone(true);
                        byte[] dataModified = SerializationUtils.serialize(assigment); //Assignment -> byte[]
                        zk.setData(rootManagement + integration + "/" + string, dataModified ,s.getVersion());
                        LOGGER.fine("Assigment finished");
                        List<String> listChildrens = zk.getChildren(rootManagement + integration + "/" + string, false);
                        
                        for (Iterator<String> iterator2 = listChildrens.iterator(); iterator2.hasNext(); ) {
                        	String node = (String) iterator2.next();
                        	zk.delete(rootManagement + integration + "/" + string + "/" + node,0);
                        }
                        zk.delete(rootManagement + integration + "/" + string,1);
                    }else{
                    	if( !assigment.isDone() ) {
                    		if(temporalLeaderOfTables[assigment.getTableLeader()]){
                                List<String> children  = zk.getChildren(rootManagement + integration + "/" + string,integrationWatcher);
                                if( children.size() == 1){
                                    temporalLeaderOfTables[assigment.getTableLeader()] = false;
                                    LOGGER.info("Im no longer temporal leader of table " + assigment.getTableLeader());
                                    LOGGER.info("Need to send table  " + assigment.getTableLeader());
                                    LOGGER.info("Receptor ready for integration");
                                    sendMessage(new TableIntegration(assigment.getTableLeader(),DHTTables.get(assigment.getTableLeader())),3000+assigment.getTableLeader());
                                    zk.create(rootManagement+integration+"/"+string+"/"+myId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                                }else{
                                    LOGGER.info("Receptor not ready for integration");
                                }
                            }
                            int[] replicas = assigment.getTableReplicas();
                            for (int i = 0; i < replicas.length; i++){
                                if(replicas[i] == leaderOfTable || temporalLeaderOfTables[replicas[i]]){
                                    List<String> children  = zk.getChildren(rootManagement + integration + "/" + string,integrationWatcher);
                                    if( children.size()>=1){
                                        LOGGER.info("Receptor ready for integration");
                                        int order;
                                        if(replicas[i] >= assigment.getTableLeader()){
                                            order = replicas[i] - assigment.getTableLeader();
                                        }else{
                                            order = replicas[i] - assigment.getTableLeader() + Quorum;
                                        }
                                        LOGGER.info("I send table " + replicas[i] + " in position " +order);
                                        LOGGER.info("Children size " + children.size());
                                        if(children.size() == (order+1)){
                                            sendMessage(new TableIntegration(leaderOfTable,DHTTables.get(replicas[i])),3000+assigment.getTableLeader());
                                            zk.create(rootManagement+integration+"/"+string+"/"+myId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                                        }else{
                                            LOGGER.info("Children position " + children.size());
                                        }
                                    }else{
                                        LOGGER.info("Receptor not ready for integration");
                                    }
                                }
                            }
                    	}
                        
                    }
                }
            } catch (Exception e) {
                LOGGER.warning(e.toString());
                LOGGER.warning("Exception: integration watcher");
            }
        }
    };

    /**
     * Leemos la clase asigment y vemos si tenemos que actualizar algo
     * @param list Lista recibida del nodo table assigments
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void processAssignments(List<String> list) throws InterruptedException, KeeperException, IOException, ClassNotFoundException {
        Stat s = new Stat();
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
            String string = (String) iterator.next();
            byte[] data = zk.getData(rootManagement+tableAssignments+"/"+ string, null, s); //Leemos el nodo
            TableAssigment assigment = (TableAssigment) SerializationUtils.deserialize(data); //byte -> Order
            if(assigment.getDHTId().equals(myId)){ //Si hay alguna peticion para mi
                LOGGER.info("Assignment for me: "+ myId);
                LOGGER.info("I am leader for table:"+ assigment.getTableLeader());
                leaderOfTable = assigment.getTableLeader(); //Soy el lider de la tabla
                LOGGER.info("And replica for tables: ");
                myReplicas = assigment.getTableReplicas(); //Mis replicas son
                temporalLeaderOfTables = new boolean[Quorum];
                for(int i=0; i<assigment.getTableReplicas().length; i++) {
                    LOGGER.info(assigment.getTableReplicas()[i]+ " ");
                }
                LOGGER.info("\n");
                zk.delete(rootManagement+tableAssignments+"/"+ string,s.getVersion());
            }else if(temporalLeaderOfTables[assigment.getTableLeader()]){
                LOGGER.info("There is a new leader for table : "+ assigment.getTableLeader());
                LOGGER.info("My temporal work here is almost done done");
                LOGGER.info("Waiting for integration");
            }
            else{
                LOGGER.info("I am "+ myId + " and this is for " + assigment.getDHTId() );
            }
        }
    }

    /**
     * Watcher for table assignment events
     */
    private Watcher temportalAssignmentWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------DHT:Watcher for temporal table assignments------------------\n");
            try {
                List<String> list = zk.getChildren(rootManagement+ temporalAssignments ,  temportalAssignmentWatcher);
                Stat s = new Stat();
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
                    String string = (String) iterator.next();
                    byte[] data = zk.getData(rootManagement+ temporalAssignments+"/"+ string, null, s); //Leemos el nodo
                    TableTemporalAssignment assigment = (TableTemporalAssignment) SerializationUtils.deserialize(data); //byte -> Order
                    if(assigment.getDHTId().equals(myId)){ //Si hay alguna asignacion para mi
                        LOGGER.info("Temporal ssignment for me: "+ myId);
                        LOGGER.info("I am temporal leader for table:"+ assigment.getTableLeader());
                        temporalLeaderOfTables[ assigment.getTableLeader()] = true;
                        LOGGER.info("\n");
                        Thread.sleep(100);
                        zk.delete(rootManagement+ temporalAssignments+"/"+ string,s.getVersion());
                    }else{
                        LOGGER.info("I am "+ myId + " and this temporal assignment is for " + assigment.getDHTId() );
                    }
                }
            } catch (Exception e) {
                LOGGER.warning(e.toString());
                LOGGER.warning("Exception: tableWatcher");
            }
        }
    };



    /* ---------------------Functions for operations---------------------------------  */



    /**
     * Recibe la lista de operaciones y va a cada nodo a recibir cada operacion. Si es para el la processa
     * @param list Lista del nodo operaciones
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void printOperations (List<String> list) throws InterruptedException, KeeperException {
        Stat stat = new Stat();
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
            String string = (String) iterator.next();
            if(!string.equals("responses")){
                LOGGER.fine(string);
                String editedString = string.replace("operation-","");
                int operationNumber = Integer.parseInt(editedString);
                LOGGER.fine(String.valueOf(operationNumber));
                int maxNewOperation = -1;
                if(operationNumber>lastOperation){
                    if(operationNumber>maxNewOperation){
                        maxNewOperation = operationNumber;
                    }
                    lastOperation=operationNumber;
                    byte[] data = zk.getData(rootOperations +"/"+ string, null, stat); //Leemos el nodo
                    DHTPetition oper = (DHTPetition) SerializationUtils.deserialize(data); //byte -> Order
                    LOGGER.fine("\nOperation " + oper.getOperation().toString());
                    LOGGER.fine("\nOperation " + oper.getTableId());

                    if(oper.getTableId()== leaderOfTable ){ //Si la peticion va para la tabla de la que soy lider
                        LOGGER.info("I am the leader of the table and should listen to operation");
                        leaderAnswer(oper,string);

                    }else if(temporalLeaderOfTables[oper.getTableId()]){
                        LOGGER.info("I am the temporal leader of the table and should listen to operation");
                        leaderAnswer(oper,string);
                    }
                    else if(ArrayUtils.contains(myReplicas,oper.getTableId())){ //Si somos replica de la tabla
                        if(oper.getOperation() == OperationEnum.PUT_MAP){
                            LOGGER.info("I am replica and should execute put");
                            getDHT(oper.getMap().getKey()).put(oper.getMap());
                        } else if(oper.getOperation() == OperationEnum.GET_MAP){
                            LOGGER.info("I am replica and should not execute get");
                        } else if(oper.getOperation() == OperationEnum.REMOVE_MAP){
                            LOGGER.info("I am replica and should execute remove");
                            getDHT(oper.getMap().getKey()).remove(oper.getMap().getKey());
                        }
                    }
                    lastOperation = maxNewOperation;
                }else{
                    LOGGER.info("Operation "+string+" already listened");
                }


            }
        }
    }

    /**
     * Cuando se añaden operaciones a operations
     */
    private Watcher operationsWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------Watcher for operations------------------\n");
            try {
                List<String> list = zk.getChildren(rootOperations,  operationsWatcher); //Coje la lista de nodos
                printOperations(list); //Imprime cada uno
            }catch (KeeperException e){
                LOGGER.fine("Someone deleted an operation that was not for me");
            }
            catch (Exception e) {
                LOGGER.warning(e.toString());
                LOGGER.warning("Exception: operationsWatcher");
            }
        }
    };

    /**
     * Watcher para las respuestas. Mira en el nodo si hay alguna respuesta para el nodo y la imprime por pantalla si ocurre.
     */
    private Watcher responseWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            LOGGER.info("------------------Watcher for responses------------------\n");
            try {
                List<String> list = zk.getChildren(rootOperations + responses,  responseWatcher); //Coje la lista de nodos
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext();){
                    String response = iterator.next();
                    if(myPetitions.contains(response)){
                        Stat s = new Stat();
                        byte[] data = zk.getData(rootOperations + responses+"/"+response,null,s);
                        DHTResponse dhtResponse = (DHTResponse) SerializationUtils.deserialize(data); //byte -> Response
                        if(dhtResponse.getMap() != null){
                            LOGGER.info("Response for petition "+response+ "is "+ dhtResponse.getMap().getValue() );
                            LOGGER.info(String.valueOf(dhtResponse.getMap().getValue()));
                            textField3.setText(String.valueOf(dhtResponse.getMap().getValue()));

                        }else{
                            LOGGER.info("Emptry response for petition "+response);
                            textField3.setText("Emptry response for petition "+response);
                        }

                        myPetitions.remove(response);
                        zk.delete(rootOperations + responses +"/"+ response,s.getVersion());
                    }
                }
            } catch (Exception e) {
                LOGGER.warning(e.toString());
                LOGGER.warning("Exception: responseWatcher");
            }
        }
    };


    /**
     * Escribe una operacion en el nodo, para pedir un valor
     * @param _operation Tipo de operacion
     * @param _GUID Tabla sobre la que se realiza la operacion
     * @param _map Map con la informacion de la operacion
     */
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
                    LOGGER.fine(petitionId);
                    myPetitions.add(petitionId);
                    LOGGER.fine(response);
                }else{
                    LOGGER.warning("Node operations doesn't exists");
                }
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
     * Funcion para emitir una respuesta a una DHT petition si somos el lider de la tabla. Leemos put y remove y contestamos
     * a los gets
     * @param petition
     * @param operation
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void leaderAnswer(DHTPetition petition, String operation) throws InterruptedException, KeeperException {
        switch (petition.getOperation()){
            case GET_MAP:
                DHT_Map map = getOperation(petition.getMap().getKey()).getMap();
                sendResponse(new DHTResponse(getPos(petition.getMap().getKey()),map),operation);
                break;
            case PUT_MAP:
                getDHT(petition.getMap().getKey()).put(petition.getMap());
                zk.delete(rootOperations +"/"+ operation,0);
                break;
            case REMOVE_MAP:
                removeOperation(petition.getMap().getKey(),false);
                zk.delete(rootOperations +"/"+ operation,0);
                break;
            default:
                LOGGER.warning("Wrong operation");
        }
    }

    /**
     * Escribe un nodo con la respuesta a la operacion.
     * @param res Respuesta  a escribir
     * @param operation Respuesta a la operacion
     */
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
                    LOGGER.fine(response);
                }else{
                    LOGGER.info("Node responses doesn't exists");
                }
            } catch (KeeperException e) {
                LOGGER.severe("The session with Zookeeper failes. Closing");
                LOGGER.warning(e.toString());
                return;
            } catch (InterruptedException e) {
                LOGGER.warning("InterruptedException raised");
            }

        }
    }
    /* ---------------------Insert and get functions ---------------------------------  */

    /**
     * Devuelve la tabla en la que se encuentra una key
     * @author aalonso
     * @param key Key de la que se quiere conocer la tabla
     * @return Numero de la tabla
     */
    private Integer getPos (String key) {
        int hash =	key.hashCode();
        if (hash < 0) {
            LOGGER.warning("Hash value is negative!!!!!");
            hash = -hash;
        }

        int segment = Integer.MAX_VALUE / (Quorum); // No negatives

        for(int i = 0; i < Quorum; i++) {
            if (hash >= (i * segment) && (hash <  (i+1)*segment)){
                return i;
            }
        }

        LOGGER.warning("getPos: This sentence shound not run");
        return 1;

    }

    private DHTUserInterface getDHT(String key) {
        return DHTTables.get(getPos(key));
    }

    /**
     * Procesado de una operacion put
     * @param key Key a insertar
     * @param value Valor a insertar
     * @return Respuesta
     */
    private ScannerAnswer putOperation(String key, int value){
        int position = getPos(key);
            LOGGER.info("Getting  answer");
            sendOperation(OperationEnum.PUT_MAP,position,new DHT_Map(key,value));
            return new ScannerAnswer(ScannerAnswerEnum.EXTERNAL_PETITION,null);
    }

    /**
     * Procesado de una opeacion get. Si tenemos la tabla en local constetamos directamete, si no pedimos el valor.
     * @param key Key de la que se quiere obtener el valor
     * @return Respueta
     */
    private ScannerAnswer getOperation(String key){
        int position = getPos(key);
        if(isLocalTable(position)){
            LOGGER.info("Local operation");
            if(getDHT(key).containsKey(key)) {
                LOGGER.info("Getting key  "+key);
                getDHT(key).get(key);
                return new ScannerAnswer(ScannerAnswerEnum.ANSWER,new DHT_Map(key,getDHT(key).get(key)));
            }else{
                LOGGER.info("Key does not exist");
                return new ScannerAnswer(ScannerAnswerEnum.NO_KEY,null);
            }
        }else {
            LOGGER.info("Getting  answer");
            sendOperation(OperationEnum.GET_MAP,position,new DHT_Map(key,0));
            return new ScannerAnswer(ScannerAnswerEnum.EXTERNAL_PETITION,null);
        }
    }


    /**
     * Procesa una respuesta de remove
     * @param key Key a eliminar
     * @param broadcast Si se ha de escribir la operacion en un nodo o solo borar el valor.
     * @return Respuesta
     */
    private ScannerAnswer removeOperation(String key,boolean broadcast){
        int position = getPos(key);
        if(isLocalTable(position)){
            LOGGER.info("Local operation");
            if(getDHT(key).containsKey(key)) {
                LOGGER.info("Removed key "+key);
                getDHT(key).remove(key);
                if(broadcast){
                    sendOperation(OperationEnum.REMOVE_MAP,position,new DHT_Map(key,0));
                }
                return new ScannerAnswer(ScannerAnswerEnum.ANSWER,null);
            }else{
                LOGGER.info("Got asked to remove non existing local key: "+key);
                return new ScannerAnswer(ScannerAnswerEnum.NO_KEY,null);
            }
        }else {
            LOGGER.info("Getting  answer");
            sendOperation(OperationEnum.REMOVE_MAP,position,new DHT_Map(key,0));
            return new ScannerAnswer(ScannerAnswerEnum.EXTERNAL_PETITION,null);
        }
    }

    /**
     * Devuelve si tenemos una tabla en local
     * @param position Numero de la tabla
     * @return Existe la tabla en local
     */
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

    /**
     * Abre el socket una vez por cada tabla a recibir y almacena las tablas recibidas
     * @param port Puerto en el que abrir el socket
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void initSocket(int port) throws IOException, ClassNotFoundException, InterruptedException, KeeperException {
        for(int i = 0; i <= (myReplicas.length); i++ ){
            ss = new ServerSocket(port);
            System.out.println("ServerSocket awaiting connections...");
            Socket socket = ss.accept(); // blocking call, this will wait until a connection is attempted on this port.
            System.out.println("Connection from " + socket + "!");

            // get the input stream from the connected socket
            InputStream inputStream = socket.getInputStream();
            // create a DataInputStream so we can read data from it.
            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

            // read the list of messages from the socket
            TableIntegration table = (TableIntegration) objectInputStream.readObject();
            LOGGER.info("Received table "+ table.getTable());
            DHTTables.put(table.getTable(),table.getHashMap());
            System.out.println("Closing sockets.");
            ss.close();
            socket.close();
        }

    }

    /**
     *
     * @param integration Mensaje con las tablas a enviar
     * @param port Puerto con el que conectarse
     * @throws InterruptedException
     * @throws IOException
     */
    private void sendMessage(TableIntegration integration, int port) throws InterruptedException, IOException {
        Socket socket = new Socket("localhost", port);
        System.out.println("Connected!");
        Thread.sleep(500);
        // get the output stream from the socket.
        OutputStream outputStream = socket.getOutputStream();
        // create an object output stream from the output stream so we can send an object through it
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        System.out.println("Sending messages to the ServerSocket");
        for(int i=0; i<20; i++){
            try {
                LOGGER.info("Try " +i);
                objectOutputStream.writeObject(integration);
                break;
            }catch (Exception e){
                Thread.sleep(500);
            }
        }

        System.out.println("Closing socket and terminating.");
        socket.close();
    }




    @Override
    public void process(WatchedEvent event) {
        try {
            LOGGER.warning("Unexpected invocated this method. Process of the object");
        } catch (Exception e) {
            LOGGER.warning("Unexpected exception. Process of the object");
        }
    }

    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == b1) {
            String key = textField1.getText();
            ScannerAnswer answer =  getOperation(key);
            if (answer.typeAnswer == ScannerAnswerEnum.ANSWER) {
                int value = answer.getMap().getValue();
                textField3.setText(String.valueOf(value));
            } else if (answer.typeAnswer == ScannerAnswerEnum.EXTERNAL_PETITION){
                textField3.setText("The key: " + key + " does not exist");
            } else if (answer.typeAnswer == ScannerAnswerEnum.NO_KEY){
                textField3.setText("The key: " + key + " does not exist");
            }
        }else if (e.getSource() == b2) {
            putOperation(textField1.getText(), Integer.parseInt(textField2.getText()));
            textField3.setText("Put done");
        }else if (e.getSource() == b3) {
            ScannerAnswer answer  = removeOperation(textField1.getText(),true);
            if (answer.typeAnswer == ScannerAnswerEnum.ANSWER) {
                textField3.setText("The key: " + textField1.getText() + " has been removed");
            } else if (answer.typeAnswer == ScannerAnswerEnum.EXTERNAL_PETITION){
                textField3.setText("The key: " + textField1.getText() + " does not exist");
            } else if (answer.typeAnswer == ScannerAnswerEnum.NO_KEY){
                textField3.setText("The key: " + textField1.getText() + " does not exist");
            }
        }
    }

    private void drawUI(){
        JFrame f = new JFrame();
        f.setBounds(10,10,400,400);
        f.getContentPane().setLayout(new BorderLayout());
        f.getContentPane().add(new TextField());

        textField1 = new TextField();
        textField1.setBounds(0,0,200,100);
        f.getContentPane().add(textField1);

        textField2 = new TextField();
        textField2.setBounds(200,0,200,100);
        f.getContentPane().add(textField2);

        textField3 = new TextField();
        textField3.setBounds(0,200,400,100);
        f.getContentPane().add(textField3);

        b1 = new JButton("GET");
        b1.setBounds(0,100,133,100);
        b1.addActionListener(this);
        f.getContentPane().add(b1);

        b2 = new JButton("PUT");
        b2.setBounds(133,100,133,100);
        b2.addActionListener(this);
        f.getContentPane().add(b2);


        b3 = new JButton("REMOVE");
        b3.setBounds(266,100,134,100);
        b3.addActionListener(this);
        f.getContentPane().add(b3);

        f.getContentPane().add(new TextField());
        f.setVisible(true);

        textField1.setText("Key");
        textField2.setText("Value");
        textField3.setText("Responses");

    }



    public static void main(String[] args) {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s %n");

        LOGGER.setLevel(Level.FINE);
        DHT dht1 = new DHT();
        dht1.drawUI();
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
                if(dht1.listenPetitions){
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
                                dht1.putOperation(key,value);
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
                                System.out.println("The key: " + key + " does not exist locally, getting value");
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
                }else{
                    Thread.sleep(1000);
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
