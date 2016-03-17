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
		dc1_0.start();
		dc1_1.start();
		dc1_2.start();
		dc2_0.start();
		dc2_1.start();
		dc2_2.start();
		dc3_0.start();
		dc3_1.start();
		dc3_2.start();

		System.out.println("3 Datacenters are deployed.");

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
