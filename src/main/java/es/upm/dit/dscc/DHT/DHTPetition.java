package es.upm.dit.dscc.DHT;

import java.io.Serializable;


/**
 * Clase a escribir en los nodos operaciones.
 */

public class DHTPetition implements Serializable  {

    private OperationEnum operation;
    private int tableId;
    private DHT_Map map;

    /**
     *
     * @param _operation Tipo de operacion
     * @param tableId ID de la tabla para de la que necesito el valor
     * @param _map Map con la informacion de la peticion
     */
    DHTPetition(OperationEnum _operation, int tableId, DHT_Map _map){
        operation = _operation;
        this.tableId = tableId;
        map = _map;
    }

    /**
     * Getter
     * @return Tipo de operacion
     */
    public OperationEnum getOperation() {
        return operation;
    }

    /**
     * Getter
     * @return ID de la tabla para de la que necesito el valor
     */
    public int getTableId() {
        return tableId;
    }


    /**
     * Getter
     * @return Map con la informacion de la peticion
     */
    public DHT_Map getMap() {
        return map;
    }

}
