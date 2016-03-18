package cs274.rc;

public class Logger {

	public static int level = 0;

	public static void info(String string) {
		if (level > 0) {
			System.out.println(string);
		}
	}
}
