/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc.connection;

public class Node {

	private String hostname;
	private int port;
	private boolean coordinator;

	public Node(String hostname, int port, boolean coordinator) {
		this.hostname = hostname;
		this.port = port;
		this.setCoordinator(coordinator);
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public boolean isCoordinator() {
		return coordinator;
	}

	public void setCoordinator(boolean coordinator) {
		this.coordinator = coordinator;
	}

}
