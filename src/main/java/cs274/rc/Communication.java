package cs274.rc;

public class Communication {

	public static final String OPERATION_READ = "Read";
	public static final String OPERATION_WRITE = "Write";
	public static final int READ_REQUEST = 1;
	public static final int READ_ACCEPT = 2;
	public static final int READ_REJECT = 3;
	public static final int PAXOS_REQUEST = 4;
	public static final int PAXOS_ACCEPT = 5;
	public static final int PAXOS_REJECT = 6;
	public static final int TPC_PREPARE = 7;
	public static final int TPC_ACCEPT = 8;
	public static final int TPC_REJECT = 9;
	public static final int TPC_COMMIT = 10;

	public static final String EXCHANGE_COORDINATORS = "coordinators";
	public static final String EXCHANGE_REPLICAS = "replicas";

}
