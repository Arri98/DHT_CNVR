package es.upm.dit.dscc.DHT;

import java.io.Serializable;

public class Operation implements Serializable  {

    private OperationEnum operation;
    private String GUID;
    private DHT_Map map;

    Operation(OperationEnum _operation,  String _GUID, DHT_Map _map){
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
