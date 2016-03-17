/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.util.HashMap;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class JsonHashmapTest extends TestCase {

	public JsonHashmapTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(JsonHashmapTest.class);
	}

	public void testHashmapToJSON() {
		HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
		StringByteIterator sbi = new StringByteIterator("value");
		map.put("key", sbi);
		JSONObject json = new JSONObject(map);
		assertEquals(json.toString(), "{\"key\":\"value\"}");
	}
	
	public void testBytesToJSON() throws JSONException {
		JSONObject json = new JSONObject();
		json.put("key", "value");
		byte[] bytes = json.toString().getBytes();
		JSONObject recover = new JSONObject(new String(bytes));
	}

}
