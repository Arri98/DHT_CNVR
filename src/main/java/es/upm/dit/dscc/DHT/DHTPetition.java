package es.upm.dit.dscc.DHT;

import java.io.Serializable;


//Peticion que se escribe en un nodo para solicitar put, get remove
public class DHTPetition implements Serializable  {

    private OperationEnum operation; //Tipo de operacion
    private String GUID; //Esto habra que quitarlo o cambiarle el nombre, por ahora es la tabla a la que le pido el valor.
    private DHT_Map map;// Datos

    DHTPetition(OperationEnum _operation, String _GUID, DHT_Map _map){
        operation = _operation;
        GUID = _GUID;
        map = _map;
    }

    public OperationEnum getOperation() {
        return operation;
    }

    public String getGUID() {
        return GUID;
    }

    public DHT_Map getMap() {
        return map;
    }

}
