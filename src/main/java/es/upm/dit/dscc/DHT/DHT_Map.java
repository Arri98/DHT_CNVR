package es.upm.dit.dscc.DHT;

import java.io.Serializable;
 
/**
 * @author aalonso
 * This class provides pairs: (key, value)
 */
public class DHT_Map implements Serializable{
	
	private static final long serialVersionUID = 2L;
	private String  key;
	private Integer value;

	public DHT_Map (String key, Integer value ) {
		this.key   = key;
		this.value = value;
	}
	
	/** 
	 * Get the key in a map
	 * @return The key
	 */
	public String getKey() {
		return this.key;
	}
	
	/** 
	 * Seg the key in a map
	 * @param key The new key
	 */
	public void setKey(String key) {
		this.key = key;
	}
	
	/**
	 * Get the value in a map
	 * @return The value
	 */
	public int getValue() {
		return this.value;
	}
	
	/**
	 * Set the value in a map
	 * @param value The new value
	 */
	public void setValue(Integer value) {
		this.value = value;
	}

	@Override
	public String toString() {
		//return "DHT_map [key=" + key + ", value=" + value + "]";
		return "(" + key + " ," + value + ")";
	}

	
	
}
