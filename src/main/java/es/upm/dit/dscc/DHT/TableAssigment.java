package es.upm.dit.dscc.DHT;

import java.io.Serializable;


/**
 *  Clase que se escribe en un znode para indicarle a una DHT de que tabla es el lider y de que tablas tiene es replica
 */

public class TableAssigment implements Serializable {
    private int tableLeader;
    private int[] tableReplicas;
    private String DHTId;

    /**
     * Constructor
     * @param leader Tabla de la que asign el lider lider, cuando piden un get contesto el DHTID
     *     private
     * @param replicas Soy replica de la tabla, solo ejecuto puts y removes.
     * @param DHTId DHT que gestiona las tablas indicadas
     */

    TableAssigment(int leader, int[] replicas, String DHTId){
        tableLeader = leader;
        tableReplicas = replicas;
        this.DHTId = DHTId;
    }

    /**
     * Getter for DHTId
     */
    public String getDHTId() {
        return DHTId;
    }
    /**
     * Setter for DHTId
     */
    public void setDHTId(String DHTId) {
        this.DHTId = DHTId;
    }
    /**
     * Getter for tableLeader
     */
    public int getTableLeader() {
        return tableLeader;
    }
    /**
     * Setter for tableLeader
     */
    public void setTableLeader(int tableLeader) {
        this.tableLeader = tableLeader;
    }
    /**
     * Geter for tableReplicas
     */
    public int[] getTableReplicas() {
        return tableReplicas;
    }
    /**
     * Setter for tableReplicas
     */
    public void setTableReplicas(int[] tableReplicas) {
        this.tableReplicas = tableReplicas;
    }
}
