package es.upm.dit.dscc.DHT;

import java.io.Serializable;

public class TableIntegration implements Serializable {
    public int getTable() {
        return table;
    }

    public void setTable(int table) {
        this.table = table;
    }

    public DHTUserInterface getHashMap() {
        return hashMap;
    }

    public void setHashMap(DHTUserInterface hashMap) {
        this.hashMap = hashMap;
    }

    public TableIntegration(int table, DHTUserInterface hashMap) {
        this.table = table;
        this.hashMap = hashMap;
    }

    private int table;
    private DHTUserInterface hashMap;

}
