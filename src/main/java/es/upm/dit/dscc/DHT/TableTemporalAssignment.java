package es.upm.dit.dscc.DHT;

import java.io.Serializable;

//Clase que se escribe en un znode para indicarle a una DHT de que tabla es el lider temporal de una tabla
public class TableTemporalAssignment implements Serializable {
    private int tableLeader; //Tabla de la que asign el lider lider, cuando piden un get contesto el DHTID
    private String DHTId; // DHT que gestiona las tablas indicadas


    TableTemporalAssignment(int leader, String DHTId){
        tableLeader = leader;
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

}



