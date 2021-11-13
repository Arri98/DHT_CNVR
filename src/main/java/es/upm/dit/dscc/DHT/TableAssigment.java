package es.upm.dit.dscc.DHT;

import java.io.Serializable;

//Clase que se escribe en un znode para indicarle a una DHT de que tabla es el lider y de que tablas tiene es replica
public class TableAssigment implements Serializable {
    private int tableLeader; //Tabla de la que asign el lider lider, cuando piden un get contesto el DHTID
    private int[] tableReplicas;// Soy replica de la tabla, solo ejecuto puts y removes.
    private String DHTId; // DHT que gestiona las tablas indicadas


    TableAssigment(int leader, int[] replicas, String DHTId){
        tableLeader = leader;
        tableReplicas = replicas;
        this.DHTId = DHTId;
    }

    public String getDHTId() {
        return DHTId;
    }

    public void setDHTId(String DHTId) {
        this.DHTId = DHTId;
    }

    public int getTableLeader() {
        return tableLeader;
    }

    public void setTableLeader(int tableLeader) {
        this.tableLeader = tableLeader;
    }

    public int[] getTableReplicas() {
        return tableReplicas;
    }

    public void setTableReplicas(int[] tableReplicas) {
        this.tableReplicas = tableReplicas;
    }
}
