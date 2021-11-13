package es.upm.dit.dscc.DHT;

import java.util.HashMap;

import java.util.logging.Logger;


/**
 * @author aalonso
 * This class manages the tables in a DHT. It includes the information
 * required for the server/process that provides the tables in the DHT.
 */
public class TableManager {

	private java.util.logging.Logger LOGGER = 	Logger.getLogger(TableManager.class.getName());
	private String   localAddress;
	private int       nReplica;
	private int       nServersMax;

	// Each server is assigned to a position in these tables
	
	// DHTServers associates the position in the DHT servers to corresponding address (JGroups)
	// The address is used for exchanging messages between the servers
	private HashMap<Integer, String> DHTServers = new HashMap<Integer, String>();
	// Associates the position with the DHT tables to a table (HashMap) local
	// The number of the local DHT tables depends on the replica number
	// One DHTUserInterface may not be used when the server has not to save it
	private HashMap<Integer, DHTUserInterface> DHTTables = new HashMap<Integer, DHTUserInterface>();

	
	public TableManager(String localAddress,
			int     nServersMax, 
			int     nReplica) {

		this.localAddress = localAddress;
		this.nServersMax  = nServersMax;
		this.nReplica     = nReplica;
	}

	/**
	 * Get the position of associated to a key in the DHT tables
	 * @param key the key
	 * @return the position of the address
	 */
	// TODO TRY TO MAKE THIS PRIVATE
	public Integer getPos (String key) {

		int hash =	key.hashCode();
		if (hash < 0) {
			LOGGER.finest("Hash value is negative!!!!!");
			hash = -hash;
		}

		int segment = Integer.MAX_VALUE / (nServersMax); // No negatives

		for(int i = 0; i < nServersMax; i++) {
			if (hash >= (i * segment) && (hash <  (i+1)*segment)){
				return i;
			}
		}

		LOGGER.warning("getPos: This sentence shound not run");
		return 1;

	}

	/**
	 * Get the position of an address in the DHT tables
	 * @param address the address to get
	 * @return the position of the address
	 */
	public Integer getPosAddress (String address) {

		int posAddress = 0;
		for (int i = 0; i < DHTServers.size(); i++){
			if (localAddress.equals(DHTServers.get(i))) {
				posAddress = i;
				continue;
			}
		}

		return posAddress;

	}

	/**
	 * Obtain the position of servers associated to a key
	 * @param key the key to get the servrs
	 * @return the list with the positions of the servers
	 */
	public int[] getNodes(String key) {
		int pos = getPos(key);
		int[] nodes = new int[nReplica];
		for (int i = 0; i < nodes.length; i++) {
			nodes[i] = (pos + i) % nServersMax;
		}
		return nodes;
	}



	/**
	 * Adds a table in the provided position of the DHT table
	 * @param table the table to be stored
	 * @param pos the position for the table
	 */
	public void addDHT(DHTUserInterface table, int pos) {
		
		DHTTables.put(pos, table);

	}

	/**
	 * Returns the table associated to the key for identifying 
	 * the table where it is stored
	 * @param key the key 
	 * @return the table 
	 */
	public DHTUserInterface getDHT(String key) {
		return DHTTables.get(getPos(key));
	}

	/**
	 * Check whether the table associated to a key for identifying
	 * is stored in the provided address
	 * @param key the key
	 * @param address the address
	 * @return true if the table associated to the key is stored in
	 * the provided address
	 */
	public boolean isDHTLocalReplica (String key, String address) {
		//int pos = getPos(key);
		return address.equals(localAddress);
	}

	/**
	 * Check whether the table associated to a key is stored in the 
	 * position provided
	 * @param posReplica the position
	 * @param key the key
	 * @return true if the table associated to the key is stored in
	 * the position provided
	 */
	public boolean isDHTLocalReplica (int posReplica, String key) { 

		int pos = getPos(key);
		return posReplica == pos;
	}

	/**
	 * Check whether the information provided by the position is local
	 * @param pos the position
	 * @return true if the information in the position is local
	 */
	public boolean isDHTLocal (int pos) {

		boolean isLocal = localAddress.equals(DHTServers.get(pos));
		LOGGER.fine("Posición: " + pos + ", isDHTLocal: " + isLocal);
		return isLocal;
	}


	/**
	 * Check whether the information associated to the key is local
	 * @param key the key
	 * @return true if the information associated to the key is local
	 */
	public boolean isDHTLocal (String key) {

		int pos = getPos(key);
		boolean isLocal = localAddress.equals(DHTServers.get(pos));
		LOGGER.fine("Posición: " + pos + ", isDHTLocal: " + isLocal);
		return isLocal;
	}

	/**
	 * Return the address of the process in the position
	 * @param pos the position
	 * @return the address associated to the position
	 */
	public String DHTAddress (int pos) {
		//Address aux = DHTServers.get(pos);
		return DHTServers.get(pos);
	}


	/**
	 * Return the address of the process associated to the key
	 * @param key the key
	 * @return the address associated to the key
	 */
	public String DHTAddress (String key) {
		// NO REPLICATION!!!!
		int pos = getPos(key);
		return DHTServers.get(pos);
	}
	
	/**
	 * Get the DHT table
	 * @return the DHT table
	 */
	public HashMap<Integer, DHTUserInterface> getDHTTables() {
		return DHTTables;
	}

	/**
	 * Get the addresses of the replicas associted to a key
	 * @param key the key
	 * @return the list of the addresses
	 */
	java.util.List<String> DHTReplicas (String key) {
		java.util.List<String> DHTReplicas = new java.util.ArrayList<String>();

		int pos = getPos(key);

		if (nReplica > 1) {
			for (int i = 1; i < nReplica; i++) {
				//TODO Si hay fallos podría ser nServersMax
				int aux = (pos + i) % nServersMax; 
				DHTReplicas.add(DHTServers.get(aux));
				LOGGER.fine("Replica #" + aux);
			}
		}
		return DHTReplicas;
	}

	/**
	 * Get the DHT servers
	 * @return the DHT servers
	 */
	HashMap<Integer, String> getDHTServers() {
		return DHTServers;
	}
	

	/**
	 * Print the servers in the DHT
	 * @return string of the servers
	 */
	public String printDHTServers() {
		String aux = "DHTManager: Servers => [";

		for (int i = 0; i < nServersMax; i++) {
			if (DHTServers.get(i) != null) {
				aux = aux + DHTServers.get(i) + " ";
			} else {
				aux = aux + "null ";	
			}	
		}	

		aux = aux + "]";

		return aux;
	}

	@Override
	public String toString() {
		DHTUserInterface dht;
		String aux = "Size: " + DHTTables.size() + " Local server: " + getPos(localAddress) +"\n";
		aux = aux + printDHTServers() + "\n";

		for (int i = 0; i < nServersMax; i ++) {
			dht = DHTTables.get(i);
			if (dht == null) {
				aux = aux + "Table " + i + ": null" + "\n" ; 
			} else {
				aux = aux + "Table " + i + ": " + dht.toString() + "\n";
			}

		}

		return aux;
	}

}

