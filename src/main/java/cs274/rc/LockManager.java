/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class LockManager {

	// private static LockManager instance;
	private HashMap<String, List<String>> sharedLock;
	private HashMap<String, String> exclusiveLock;

	// public static LockManager getInstance() {
	// if (instance == null) {
	// instance = new LockManager();
	// }
	// return instance;
	// }

	public LockManager() {
		this.sharedLock = new HashMap<String, List<String>>();
		this.exclusiveLock = new HashMap<String, String>();
	}

	public HashMap<String, List<String>> getSharedLock() {
		return sharedLock;
	}

	public void setSharedLock(HashMap<String, List<String>> sharedLock) {
		this.sharedLock = sharedLock;
	}

	public HashMap<String, String> getExclusiveLock() {
		return exclusiveLock;
	}

	public void setExclusiveLock(HashMap<String, String> exclusiveLock) {
		this.exclusiveLock = exclusiveLock;
	}

	public synchronized boolean setShared(String key, String transaction) {
		// System.out.println(transaction + " is trying to set shared lock on "
		// + key + "...");
		if (exclusiveLock.containsKey(key)) {
			// System.out.println(exclusiveLock.get(key)
			// + " is holding the exclusive lock.");
			return false;
		} else {
			if (sharedLock.containsKey(key)) {
				List<String> transactions = sharedLock.get(key);
				if (transactions.contains(transaction)) {
					// System.out
					// .println(transaction
					// + " has already owned shared lock on "
					// + key + ".");
					return false;
				}
				sharedLock.get(key).add(transaction);
			} else {
				List<String> transactions = new ArrayList<String>();
				transactions.add(transaction);
				sharedLock.put(key, transactions);
			}
			// System.out.println(transaction
			// + " successfully set shared lock on " + key + ".");
			return true;
		}
	}

	public synchronized boolean setExclusive(String key, String transaction) {
		// System.out.println(transaction +
		// " is trying to set exclusive lock on "
		// + key + "...");
		if (exclusiveLock.containsKey(key)) {
			// System.out.println(exclusiveLock.get(key)
			// + " is holding the exclusive lock.");
			return false;
		} else if (sharedLock.containsKey(key)) {
			List<String> transactions = sharedLock.get(key);
			if (transactions.size() == 1
					&& transactions.get(0).equals(transaction)) {
				// Upgrade shared lock to exclusive lock
				sharedLock.remove(key);
				exclusiveLock.put(key, transaction);
				// System.out.println("Only " + transaction
				// + " holds shared lock on " + key
				// + ", upgrade it to exclusive lock.");
				return true;
			} else {
				// Others hold shared lock
				// System.out
				// .println("Another transaction is holding the shared lock on "
				// + key + ", cannot set the exclusive lock.");
				return false;
			}
		} else {
			exclusiveLock.put(key, transaction);
			// System.out.println(transaction
			// + " successfully set exclusive lock on " + key + ".");
			return true;
		}
	}

	public synchronized boolean unlockShared(String key, String transaction) {
		if (sharedLock.containsKey(key)) {
			List<String> transactions = sharedLock.get(key);
			if (transactions.contains(transaction)) {
				transactions.remove(transaction);
				if (transactions.isEmpty()) {
					sharedLock.remove(key);
				} else {
					sharedLock.put(key, transactions);
				}
				// System.out.println(transaction + " unlocks share lock on "
				// + key + ".");
				return true;
			}
		}
		// System.out.println(transaction + " doesn't own share lock on " + key
		// + ".");
		return false;
	}

	public synchronized boolean unlockExclusive(String key, String transaction) {
		if (exclusiveLock.containsKey(key)
				&& exclusiveLock.get(key).equals(transaction)) {
			exclusiveLock.remove(key);
			// System.out.println(transaction + " unlocks exclusive lock on "
			// + key + ".");
			return true;
		}
		// System.out.println(transaction + " doesn't own exclusive lock on "
		// + key + ".");
		return false;
	}
	
	public synchronized void unlockAllByTransaction(String transaction) {
		sharedLock.values().removeAll(Collections.singleton(transaction));
		exclusiveLock.values().removeAll(Collections.singleton(transaction));
	}
	
	public synchronized boolean testExclusive(List<String> keys) {
		for (String key : keys) {
			if (exclusiveLock.containsKey(key)) {
				return false;
			}
		}
		return true;
	}

}
