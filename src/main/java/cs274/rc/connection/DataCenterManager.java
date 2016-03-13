package cs274.rc.connection;

import java.util.ArrayList;
import java.util.List;

public class DataCenterManager {

	private List<Node> shards;

	public DataCenterManager() {
		shards = new ArrayList<Node>();
	}

	public List<Node> getShards() {
		return shards;
	}

	public int getShardsNumber() {
		return shards.size();
	}

	public void addShard(Node node) {
		shards.add(node);
	}

}
