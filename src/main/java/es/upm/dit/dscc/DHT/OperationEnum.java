package es.upm.dit.dscc.DHT;


/**
 * @author aalonso
 * This class list the operations that can be requested 
 * within a message.
 */
/**
 * @author aalonso
 *
 */
public enum OperationEnum {
	/**
	 * Put a map 
	 */
	PUT_MAP,
	/**
	 * Get a map
	 */
	GET_MAP,
	/**
	 * Remove a map
	 */
	REMOVE_MAP,
	/**
	 * Check wheter key is contained in collection
	 */
	CONTAINS_KEY_MAP,
	/**
	 * Request for the set of the keys in a collection
	 */
	KEY_SET_HM,
	//Request for the set of the values in a collection
	//VALUES_HM,
	LIST_SERVERS,
	/**
	 * A requested value has been received 
	 */
	RETURN_VALUE,
	/**
	 * A requested status has been received 
	 */
	RETURN_STATUS,
	
	/*
	 * The DHT Servers is sent to a new replica to recover a server
	 */
	DHT_REPLICA,
//	DATA_REPLICA,
	
	/*
	 * Initial operation for ensure the start of servers
	 */
	INIT
}
