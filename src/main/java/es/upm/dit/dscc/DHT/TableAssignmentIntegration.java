package es.upm.dit.dscc.DHT;

import java.io.Serializable;

public class TableAssignmentIntegration extends TableAssigment implements Serializable {

	private boolean done = false;

	public TableAssignmentIntegration(int leader, int[] replicas, String DHTId) {
		super(leader, replicas, DHTId);
		done = false;
		
	}


	public boolean isDone() {
		return done;
	}

	public void setDone(boolean done) {
		this.done = done;
	}
	
	

}
