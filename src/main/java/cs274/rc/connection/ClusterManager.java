/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc.connection;

import java.util.ArrayList;
import java.util.List;

public class ClusterManager {

	private List<ReplicaConnection> replicas;

	public ClusterManager() {
		replicas = new ArrayList<ReplicaConnection>();
	}

	public void addReplica(String hostname, int port) {
		replicas.add(new ReplicaConnection(hostname, port));
	}

	public List<ReplicaConnection> getReplicas() {
		return replicas;
	}

	public int getReplicaNumber() {
		return replicas.size();
	}

}
