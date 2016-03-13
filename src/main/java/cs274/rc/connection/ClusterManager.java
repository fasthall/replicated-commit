/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc.connection;

import java.util.ArrayList;
import java.util.List;

public class ClusterManager {

	private List<Node> replicas;
	private int coordinatorNum;

	public ClusterManager() {
		replicas = new ArrayList<Node>();
		coordinatorNum = 0;
	}

	public void addReplica(String hostname, int port, boolean coordinator) {
		replicas.add(new Node(hostname, port, coordinator));
		if (coordinator) {
			++coordinatorNum;
		}
	}

	public List<Node> getReplicas() {
		return replicas;
	}

	public int getReplicaNumber() {
		return replicas.size();
	}

	public int getCoordinatorNumber() {
		return coordinatorNum;
	}

}
