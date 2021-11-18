package es.upm.dit.dscc.DHT;

import java.io.Serializable;


//Peticion que se escribe en un nodo para solicitar put, get remove
public class DHTResponse implements Serializable  {

    private int GUID;
    private DHT_Map map;// Datos

    DHTResponse(int _GUID, DHT_Map _map){
        GUID = _GUID;
        map = _map;
    }

    public int getGUID() {
        return GUID;
    }

    public DHT_Map getMap() {
        return map;
    }

}
