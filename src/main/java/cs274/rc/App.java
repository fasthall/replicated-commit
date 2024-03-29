/*
 * @Author Wei-Tsung Lin
 * @Date 02/26/2016
 */
package cs274.rc;

public class App {

	public static int commit = 0;
	public static int abort = 0;
	public static String err4Str = "";

	public static void main(String[] args) {
		Server dc1_0 = new Server("DC1_0", true, "dc1", 3, 3);
		Server dc1_1 = new Server("DC1_1", false, "dc1", 3, 3);
		Server dc1_2 = new Server("DC1_2", false, "dc1", 3, 3);
		Server dc2_0 = new Server("DC2_0", true, "dc2", 3, 3);
		Server dc2_1 = new Server("DC2_1", false, "dc2", 3, 3);
		Server dc2_2 = new Server("DC2_2", false, "dc2", 3, 3);
		Server dc3_0 = new Server("DC3_0", true, "dc3", 3, 3);
		Server dc3_1 = new Server("DC3_1", false, "dc3", 3, 3);
		Server dc3_2 = new Server("DC3_2", false, "dc3", 3, 3);
		dc1_0.addCoordinators("DC1_0", "DC2_0", "DC3_0");
		dc2_0.addCoordinators("DC1_0", "DC2_0", "DC3_0");
		dc3_0.addCoordinators("DC1_0", "DC2_0", "DC3_0");
		dc1_0.addOneWayLatency("client", 40);
//		dc1_0.addOneWayLatency("intra", 15);
		dc1_0.addOneWayLatency("DC2_0", 55);
		dc1_0.addOneWayLatency("DC3_0", 65);
		dc1_1.addOneWayLatency("client", 40);
//		dc1_1.addOneWayLatency("intra", 15);
		dc1_2.addOneWayLatency("client", 40);
//		dc1_2.addOneWayLatency("intra", 15);
		dc2_0.addOneWayLatency("client", 50);
		dc2_0.addOneWayLatency("DC1_0", 55);
		dc2_0.addOneWayLatency("DC3_0", 75);
//		dc2_0.addOneWayLatency("intra", 15);
		dc2_1.addOneWayLatency("client", 50);
//		dc2_1.addOneWayLatency("intra", 15);
		dc2_2.addOneWayLatency("client", 50);
//		dc2_2.addOneWayLatency("intra", 15);
		dc3_0.addOneWayLatency("client", 60);
		dc3_0.addOneWayLatency("DC1_0", 65);
		dc3_0.addOneWayLatency("DC2_0", 75);
//		dc3_0.addOneWayLatency("intra", 15);
		dc3_1.addOneWayLatency("client", 60);
//		dc3_1.addOneWayLatency("intra", 15);
		dc3_2.addOneWayLatency("client", 60);
//		dc3_2.addOneWayLatency("intra", 15);
		dc1_0.start();
		dc1_1.start();
		dc1_2.start();
		dc2_0.start();
		dc2_1.start();
		dc2_2.start();
		dc3_0.start();
		dc3_1.start();
		dc3_2.start();

		System.out.println("3 Datacenters are deployed. Each one has 3 shards");

		try {
			dc1_0.join();
			dc1_1.join();
			dc1_2.join();
			dc2_0.join();
			dc2_1.join();
			dc2_2.join();
			dc3_0.join();
			dc3_1.join();
			dc3_2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
