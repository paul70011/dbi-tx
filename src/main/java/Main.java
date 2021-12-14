import java.sql.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.sql.Connection.TRANSACTION_SERIALIZABLE;

public class Main {

    final static String STR30 = "000000000000000000000000000000";

    // balance statements
    static PreparedStatement balanceStmt;

    // deposit statements
    static PreparedStatement depositBranchStmt;
    static PreparedStatement depositTellerStmt;
    static PreparedStatement depositAccountStmt;
    static PreparedStatement depositHistoryStmt;

    static CallableStatement depositClbl;

    // analyse statements
    static PreparedStatement analyseStmt;

    static Random rand = new Random();

    static int maxBRANCHID;
    static int maxTELLERID;
    static int maxACCID;

    public static int balance(int ACCID) throws SQLException {
        balanceStmt.setInt(1, ACCID);
        ResultSet rs = balanceStmt.executeQuery();
        rs.next();
        return rs.getInt("balance");
    }

    public static int deposit(int ACCID, int TELLERID, int BRANCHID, int DELTA) throws SQLException {
        depositClbl.setInt(1, BRANCHID);
        depositClbl.setInt(2, TELLERID);
        depositClbl.setInt(3, ACCID);
        depositClbl.setInt(4, DELTA);
        depositClbl.setString(5, STR30);
        depositClbl.registerOutParameter(6, Types.INTEGER);
        depositClbl.execute();

        return depositClbl.getInt(6);
    }

    public static int analyse(int DELTA) throws SQLException {
        analyseStmt.setInt(1, DELTA);
        ResultSet rs = analyseStmt.executeQuery();
        rs.next();
        return rs.getInt(1);
    }

    public static void prepareStatements(Connection conn) throws SQLException {
        System.out.println("Preparing statements...");

        // balance
        balanceStmt = conn.prepareStatement("SELECT balance FROM accounts WHERE accid = ?");

        // deposit
        depositClbl = conn.prepareCall("{CALL deposit(?, ?, ?, ?, ?, ?)}");

        // analyse
        analyseStmt = conn.prepareStatement("SELECT count(*) FROM history WHERE delta = ?");

        System.out.println("Prepared!");
    }

    public static void runRandomTX() throws SQLException {
        int x = rand.nextInt(100);
        if (x < 35) {
            int ACCID = rand.nextInt(maxACCID) + 1;
            // System.out.printf("Balance: ACCID(%d) = %d%n", ACCID, balance(ACCID));
            balance(ACCID);
        } else if (x < 35 + 50) {
            int ACCID = rand.nextInt(maxACCID) + 1;
            int TELLERID = rand.nextInt(maxTELLERID) + 1;
            int BRANCHID = rand.nextInt(maxBRANCHID) + 1;
            int DELTA = rand.nextInt(10000) + 1;
            // System.out.printf("Deposit: ACCID(%d) TELLERID(%d) BRANCHID(%d) DELTA(%d) =
            // %d%n",
            // ACCID, TELLERID, BRANCHID, DELTA, deposit(ACCID, TELLERID, BRANCHID, DELTA));
            deposit(ACCID, TELLERID, BRANCHID, DELTA);
        } else {
            int DELTA = rand.nextInt(10000) + 1;
            // System.out.printf("Analyse: DELTA(%d) = %d%n", DELTA, analyse(DELTA));
            analyse(DELTA);
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Required arguments: [ip] [username] [password] [n]");
            System.exit(1);
        }

        // defining database connection parameters
        final String DB_URL = "jdbc:mariadb://" + args[0] + ":3306/benchmark?rewriteBatchedStatements=true";
        final String USER = args[1];
        final String PASS = args[2];

        // defining limits for IDs
        final int n = Integer.parseInt(args[3]);
        maxBRANCHID = 1 * n;
        maxTELLERID = 10 * n;
        maxACCID = 100000 * n;

        try {
            System.out.println("Connecting to database...");
            Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected!");
            prepareStatements(conn);
            conn.setTransactionIsolation(TRANSACTION_SERIALIZABLE);

            for (long stop = System.nanoTime() + TimeUnit.MINUTES.toNanos(4); stop > System.nanoTime();) {
                runRandomTX();
                TimeUnit.MILLISECONDS.sleep(50);
            }
            int txCount = 0;
            for (long stop = System.nanoTime() + TimeUnit.MINUTES.toNanos(5); stop > System.nanoTime();) {
                runRandomTX();
                txCount++;
                TimeUnit.MILLISECONDS.sleep(50);
            }
            for (long stop = System.nanoTime() + TimeUnit.MINUTES.toNanos(1); stop > System.nanoTime();) {
                runRandomTX();
                TimeUnit.MILLISECONDS.sleep(50);
            }

            System.out.printf("TX count: %d%n", txCount);
            System.out.printf("Avg TX/s: %f%n", txCount / (5f * 60f));

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
