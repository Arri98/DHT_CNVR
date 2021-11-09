package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * @author aalonso
 * This interface describes the API for accessing
 * to the DHT
 */
public interface DHTUserInterface {

	/**
	 * Adds a map in the DHT 
	 * @param map The map to be added
	 * @return The value of the map stored
	 */
	Integer put(DHT_Map map);
	
	/**
	 * Adds a map in the DHT 
	 * It is used when the operation is remote
	 * @param map The map to be added
	 * @return The value of the map stored
	 */
	Integer putMsg(DHT_Map map);

	/**
	 * Return the value associated to the provided key
	 * @param key The key for getting the associated map
	 * @return The value of the map associated
	 */
	Integer get(String key);

	/**
	 * Remove the value associated to the provided key
	 * @param key The key for removing the associated map
	 * @return The value of the map removed
	 */
	Integer remove(String key);

	
	/**
	 * Remove the value associated to the provided key.
	 * It is used when the operation is remote
	 * @param key The key for removing the associated map
	 * @return The value of the map removed
	 */
	Integer removeMsg(String key);
	
	/**
	 * Checks whether the provided key is in the DHT
	 * @param key The key for checking the associated map
	 * @return True if the key is stored
	 */
	boolean containsKey(String key);

	/**
	 * Returns a Set view of the keys contained in this DHT.
	 * @return Returns the set of the values contained in this DHT.
	 */
	Set<String> keySet();
	
//	Set<Map.Entry<String,Integer>> entrySet();

	/**
	 * Returns a List of the values contained in this DHT.
	 * @return The list of the values in the DHT
	 */
	ArrayList<Integer> values();

}