package cs274.rc;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import org.codehaus.jettison.json.JSONObject;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public class YCSBClient extends com.yahoo.ycsb.DB {

	private Client client;

	@Override
	public void init() throws DBException {
		client = new Client("client", 9, 3);
	}

	@Override
	public Status delete(String table, String key) {
		// TODO Auto-generated method stub
		return Status.OK;
	}

	@Override
	public Status read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		Transaction transaction = new Transaction();
		transaction.addReadOperation(key);
		try {
			client.put(transaction);
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
		return Status.OK;
	}

	@Override
	public Status scan(String table, String key, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		Transaction transaction = new Transaction();
		transaction.addReadOperation(key);
		try {
			client.put(transaction);
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
		return Status.OK;
	}

	@Override
	public Status insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		Transaction transaction = new Transaction();
		transaction.addWriteOperation(key, new JSONObject(values).toString());
		try {
			client.put(transaction);
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
		return Status.OK;
	}

	@Override
	public Status update(String table, String key,
			HashMap<String, ByteIterator> values) {
		Transaction transaction = new Transaction();
		transaction.addWriteOperation(key, new JSONObject(values).toString());
		try {
			client.put(transaction);
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
		return Status.OK;
	}

}
