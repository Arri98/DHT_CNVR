package es.upm.dit.dscc.DHT;

import java.io.Serializable;


/**
 * Clase a escribir en el nodo response.
 */
public class DHTResponse implements Serializable  {

    private int GUID;
    private DHT_Map map;// Datos

    /**
     * Constructor
     * @param _GUID ID de la tabla que responde
     * @param _map Respuesta a la peticion
     */
    DHTResponse(int _GUID, DHT_Map _map){
        GUID = _GUID;
        map = _map;
    }

    /**
     * Getter
     * @return ID de la tabla que respond
     */
    public int getGUID() {
        return GUID;
    }

    /**
     * Getter
     * @return Respuesta a la peticion
     */
    public DHT_Map getMap() {
        return map;
    }

}
