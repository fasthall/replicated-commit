/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LockManager {

	private HashMap<String, List<String>> sharedLock;
	private HashMap<String, String> exclusiveLock;

	public LockManager() {
		setSharedLock(new HashMap<String, List<String>>());
		setExclusiveLock(new HashMap<String, String>());
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

	public synchronized boolean setShared(String data, String transaction) {
		System.out.println(transaction + " is trying to set shared lock on "
				+ data + "...");
		if (exclusiveLock.containsKey(data)) {
			System.out.println(exclusiveLock.get(data)
					+ " is holding the exclusive lock.");
			return false;
		} else {
			if (sharedLock.containsKey(data)) {
				List<String> transactions = sharedLock.get(data);
				if (transactions.contains(transaction)) {
					System.out
							.println(transaction
									+ " has already owned shared lock on "
									+ data + ".");
					return false;
				}
				sharedLock.get(data).add(transaction);
			} else {
				List<String> transactions = new ArrayList<String>();
				transactions.add(transaction);
				sharedLock.put(data, transactions);
			}
			System.out.println(transaction
					+ " successfully set shared lock on " + data + ".");
			return true;
		}
	}

	public synchronized boolean setExclusive(String data, String transaction) {
		System.out.println(transaction + " is trying to set exclusive lock on "
				+ data + "...");
		if (exclusiveLock.containsKey(data)) {
			System.out.println(exclusiveLock.get(data)
					+ " is holding the exclusive lock.");
			return false;
		} else if (sharedLock.containsKey(data)) {
			List<String> transactions = sharedLock.get(data);
			if (transactions.size() == 1
					&& transactions.get(0).equals(transaction)) {
				// Upgrade shared lock to exclusive lock
				sharedLock.remove(data);
				exclusiveLock.put(data, transaction);
				System.out.println("Only " + transaction
						+ " holds shared lock on " + data
						+ ", upgrade it to exclusive lock.");
				return true;
			} else {
				// Others hold shared lock
				System.out
						.println("Another transaction is holding the shared lock on "
								+ data + ", cannot set the exclusive lock.");
				return false;
			}
		} else {
			exclusiveLock.put(data, transaction);
			System.out.println(transaction
					+ " successfully set exclusive lock on " + data + ".");
			return true;
		}
	}

	public synchronized boolean unlockShared(String data, String transaction) {
		if (sharedLock.containsKey(data)) {
			List<String> transactions = sharedLock.get(data);
			if (transactions.contains(transaction)) {
				transactions.remove(transaction);
				if (transactions.isEmpty()) {
					sharedLock.remove(data);
				} else {
					sharedLock.put(data, transactions);
				}
				System.out.println(transaction + " unlocks share lock on "
						+ data + ".");
				return true;
			}
		}
		System.out.println(transaction + " doesn't own share lock on " + data
				+ ".");
		return false;
	}

	public synchronized boolean unlockExclusive(String data, String transaction) {
		if (exclusiveLock.containsKey(data)
				&& exclusiveLock.get(data).equals(transaction)) {
			exclusiveLock.remove(data);
			System.out.println(transaction + " unlocks exclusive lock on "
					+ data + ".");
			return true;
		}
		System.out.println(transaction + " doesn't own exclusive lock on "
				+ data + ".");
		return false;
	}

}
