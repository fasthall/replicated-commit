/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TransactionTest extends TestCase {

	public TransactionTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(TransactionTest.class);
	}

	public void testTransaction() {
		System.out.println("testTransaction");
		// r[x] r[y] w[z] r[z] w[x]
		Transaction transaction = new Transaction("T1");
		transaction.addReadOperation("X");
		transaction.addReadOperation("Y");
		transaction.addWriteOperation("Z", "1");
		transaction.addReadOperation("Z");
		transaction.addWriteOperation("X", "2");

		Operation operation;
		operation = transaction.popOperation();
		assertEquals(operation.getAction(), Operation.READ);
		assertEquals(operation.getKey(), "X");
		operation = transaction.popOperation();
		assertEquals(operation.getAction(), Operation.READ);
		assertEquals(operation.getKey(), "Y");
		operation = transaction.popOperation();
		assertEquals(operation.getAction(), Operation.WRITE);
		assertEquals(operation.getKey(), "Z");
		operation = transaction.popOperation();
		assertEquals(operation.getAction(), Operation.READ);
		assertEquals(operation.getKey(), "Z");
		operation = transaction.popOperation();
		assertEquals(operation.getAction(), Operation.WRITE);
		assertEquals(operation.getKey(), "X");
		operation = transaction.popOperation();
		Assert.assertNull(operation);
	}

}
