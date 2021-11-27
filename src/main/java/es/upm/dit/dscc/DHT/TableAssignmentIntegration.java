package es.upm.dit.dscc.DHT;

import java.io.Serializable;

/**
 * Clase a escribir en el nodo Integration. Solicitud para integrar una tabla. Misma clase asignacion de tablas,
 * pero añade el parametro done para evitar que una integracion se realize varias veces mientras se borra el nodo.
 */

public class TableAssignmentIntegration extends TableAssigment implements Serializable {

	private boolean done = false; //Se ha terminado la integracion

	/**
	 * @param leader Tabla de la que es lider el nodo
	 * @param replicas Replicas del nodo
	 * @param DHTId ID del nodoa  integrar
	 */
	public TableAssignmentIntegration(int leader, int[] replicas, String DHTId) {
		super(leader, replicas, DHTId);
		done = false;
		
	}

	/**
	 * Getter for done
	 */
	public boolean isDone() {
		return done;
	}
	/**
	 * Setter for done
	 */
	public void setDone(boolean done) {
		this.done = done;
	}
	
	

}
