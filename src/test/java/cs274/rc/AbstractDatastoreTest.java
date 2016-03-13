/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import cs274.rc.datastore.AbstractDatastore;
import cs274.rc.datastore.DatastoreEntry;
import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class AbstractDatastoreTest extends TestCase {

	AbstractDatastore store;

	public AbstractDatastoreTest(String testName) {
		super(testName);
		store = new AbstractDatastore();
	}

	public static Test suite() {
		return new TestSuite(AbstractDatastoreTest.class);
	}

	public void testDatatore() {
		System.out.println("testDatatore");
		DatastoreEntry nullEntry = store.get("key");
		assertNull(nullEntry);
		DatastoreEntry entry = new DatastoreEntry("value", System.currentTimeMillis());
		store.put("key", entry);
		assertNotNull(entry);
		System.out.println(entry.getValue() + ", " + entry.getVersion());
		Assert.assertEquals(entry.getValue(), "value");
	}

}
