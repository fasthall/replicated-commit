package cs274.rc;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.codehaus.jettison.json.JSONObject;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public class YCSBClient extends com.yahoo.ycsb.DB {

	private Client client1;

	@Override
	public void init() throws DBException {
		client1 = new Client(UUID.randomUUID().toString());
		client1.addReplicas("DC1_0", "DC1_1", "DC1_2", "DC2_0", "DC2_1", "DC2_2", "DC3_0", "DC3_1", "DC3_2");
		client1.addCoordinator("DC1_0", "DC2_0", "DC3_0");
		client1.addOneWayLatency("DC1_0", 40);
		client1.addOneWayLatency("DC1_1", 40);
		client1.addOneWayLatency("DC1_2", 40);
		client1.addOneWayLatency("DC2_0", 50);
		client1.addOneWayLatency("DC2_1", 50);
		client1.addOneWayLatency("DC2_2", 50);
		client1.addOneWayLatency("DC3_0", 60);
		client1.addOneWayLatency("DC3_1", 60);
		client1.addOneWayLatency("DC3_2", 60);
	}

	@Override
	public Status delete(String table, String key) {
		// TODO Auto-generated method stub
		return Status.OK;
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
//		System.out.println("read");
		Transaction transaction = new Transaction();
		transaction.addReadOperation(key);
		try {
			if (client1.put(transaction)) {
				return Status.OK;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Status.ERROR;
	}

	@Override
	public Status scan(String table, String key, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
//		System.out.println("scan");
		Transaction transaction = new Transaction();
		transaction.addReadOperation(key);
		try {
			if (client1.put(transaction)) {
				return Status.OK;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Status.ERROR;
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
//		System.out.println("insert");
		Transaction transaction = new Transaction();
		transaction.addWriteOperation(key, new JSONObject(values).toString());
		try {
			if (client1.put(transaction)) {
				return Status.OK;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Status.ERROR;
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
//		System.out.println("update " + key + " " + new JSONObject(values).toString());
		Transaction transaction = new Transaction();
		transaction.addWriteOperation(key, new JSONObject(values).toString());
		try {
			if (client1.put(transaction)) {
				return Status.OK;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Status.ERROR;
	}

}
