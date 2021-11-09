package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.io.Serializable;

public class DHTHashMap implements DHTUserInterface, Serializable{
	
	private static final long serialVersionUID = 1L;
	// LOGGER is not serilizable
	//private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private HashMap <String, Integer> hashMap = new HashMap<String, Integer>();
		
	@Override
	public Integer put(DHT_Map map) {
		if (map == null) {
			return null;
		}
		hashMap.put(map.getKey(), map.getValue());
		return map.getValue();
	}
	
	@Override
	public Integer get(String key) {
		return hashMap.get(key);
	}
	
	@Override
	public Integer remove(String key) {
		return hashMap.remove(key);
	}
	
	@Override
	public boolean containsKey(String key) {
		return hashMap.containsKey(key);
	}
	
	@Override
	public Set<String> keySet() {
		return hashMap.keySet();
	}
	
	@Override
	public ArrayList<Integer> values() {
		
		Collection<Integer> values;
		ArrayList<Integer> list = new ArrayList<Integer>();
		values = hashMap.values();
		for (Iterator iterator = values.iterator(); iterator.hasNext();) {
			Integer integer = (Integer) iterator.next();
			list.add(integer);
		}

		return list;
		
	}
	
	@Override
	public Integer putMsg(DHT_Map map) {
		return null;
	}

	//@Override
	public Integer removeMsg(String key) {
		return null;
	}

	public HashMap <String, Integer> getDHT() {
		return hashMap;
	}
	
	@Override
	public String toString() {
		String aux = "";
		ArrayList<Integer> list = new ArrayList<Integer>();
		list = 	this.values();
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			Integer values = (Integer) iterator.next();
			aux = aux + " " + values;
		}
		return aux;
	}


}
