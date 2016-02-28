package cs274.rc.connection;

import java.util.ArrayList;
import java.util.List;

public class ClusterManager {

	private static ClusterManager instance;
	private List<ReplicaConnection> replicas;

	public static ClusterManager getInstance() {
		if (instance == null) {
			instance = new ClusterManager();
		}
		return instance;
	}

	private ClusterManager() {
		replicas = new ArrayList<ReplicaConnection>();
	}

	public void addReplica(String hostname, int port) {
		replicas.add(new ReplicaConnection(hostname, port));
	}

}
