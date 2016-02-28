/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class LockManagerTest extends TestCase {

	LockManager lockManager;

	public LockManagerTest(String testName) {
		super(testName);
		lockManager = LockManager.getInstance();
	}

	public static Test suite() {
		return new TestSuite(LockManagerTest.class);
	}

	public void testSetSharedLock() {
		System.out.println("testSetSharedLock");
		assertTrue(lockManager.setShared("X", "T1"));
		assertTrue(lockManager.setShared("X", "T2"));
		assertTrue(lockManager.setShared("Y", "T1"));
		assertTrue(lockManager.unlockShared("X", "T1"));
		assertTrue(lockManager.unlockShared("X", "T2"));
		assertTrue(lockManager.unlockShared("Y", "T1"));
		assertEquals(lockManager.getSharedLock().size(), 0);
	}

	public void testSetExclusiveLock() {
		System.out.println("testSetExclusiveLock");
		assertTrue(lockManager.setExclusive("X", "T1"));
		assertFalse(lockManager.setExclusive("X", "T2"));
		assertTrue(lockManager.setExclusive("Y", "T1"));
		assertTrue(lockManager.unlockExclusive("X", "T1"));
		assertTrue(lockManager.unlockExclusive("Y", "T1"));
		assertTrue(lockManager.setShared("Z", "T3"));
		assertFalse(lockManager.setExclusive("Z", "T1"));
		assertTrue(lockManager.unlockShared("Z", "T3"));
		assertEquals(lockManager.getSharedLock().size(), 0);
		assertEquals(lockManager.getExclusiveLock().size(), 0);
	}

	public void testUpgradeLock() {
		System.out.println("testUpgradeLock");
		assertTrue(lockManager.setShared("Z", "T2"));
		assertTrue(lockManager.setExclusive("Z", "T2"));
		assertTrue(lockManager.unlockExclusive("Z", "T2"));
		assertEquals(lockManager.getSharedLock().size(), 0);
		assertEquals(lockManager.getExclusiveLock().size(), 0);
	}

}
