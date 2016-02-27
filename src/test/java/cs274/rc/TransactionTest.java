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
		Transaction transaction = new Transaction();
		transaction.addOperation(new Operation(Operation.READ, "X"));
		transaction.addOperation(new Operation(Operation.READ, "Y"));
		transaction.addOperation(new Operation(Operation.WRITE, "Z"));
		transaction.addOperation(new Operation(Operation.READ, "Z"));
		transaction.addOperation(new Operation(Operation.WRITE, "X"));

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
