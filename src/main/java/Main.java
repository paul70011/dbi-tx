import java.sql.*;

public class Main {

    final static String STR30 = "000000000000000000000000000000";

    // balance statements
    static PreparedStatement balanceStmt;

    // deposit statements
    static PreparedStatement depositBranchStmt;
    static PreparedStatement depositTellerStmt;
    static PreparedStatement depositAccountStmt;
    static PreparedStatement depositHistoryStmt;

    // analyse statements
    static PreparedStatement analyseStmt;

    public static int balance(int ACCID) throws SQLException {
        balanceStmt.setInt(1, ACCID);
        ResultSet rs = balanceStmt.executeQuery();
        rs.next();
        return rs.getInt("balance");
    }

    public static int deposit(int ACCID, int TELLERID, int BRANCHID, int DELTA) throws SQLException {
        depositBranchStmt.setInt(1, DELTA);
        depositBranchStmt.setInt(2, BRANCHID);
        depositBranchStmt.executeUpdate();

        depositTellerStmt.setInt(1, DELTA);
        depositTellerStmt.setInt(2, TELLERID);
        depositTellerStmt.executeUpdate();

        depositAccountStmt.setInt(1, DELTA);
        depositAccountStmt.setInt(2, ACCID);
        depositAccountStmt.executeUpdate();

        int newAccBalance = balance(ACCID);

        depositHistoryStmt.setInt(1, ACCID);
        depositHistoryStmt.setInt(2, TELLERID);
        depositHistoryStmt.setInt(3, DELTA);
        depositHistoryStmt.setInt(4, BRANCHID);
        depositHistoryStmt.setInt(5, newAccBalance);
        depositHistoryStmt.setString(6, STR30);
        depositHistoryStmt.executeUpdate();

        return newAccBalance;
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
        depositBranchStmt = conn.prepareStatement("UPDATE branches SET balance = balance + ? WHERE branchid = ?");
        depositTellerStmt = conn.prepareStatement("UPDATE tellers SET balance = balance + ? WHERE tellerid = ?");
        depositAccountStmt = conn.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE accid = ?");
        depositHistoryStmt = conn.prepareStatement(
                "INSERT INTO history (accid, tellerid, delta, branchid, accbalance, cmmnt) VALUES(?, ?, ?, ?, ?, ?);");

        // analyse
        analyseStmt = conn.prepareStatement("SELECT count(*) FROM history WHERE delta = ?");

        System.out.println("Prepared!");
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Required arguments: [ip] [username] [password]");
            System.exit(1);
        }

        final String DB_URL = "jdbc:mariadb://" + args[0] + ":3306/benchmark?rewriteBatchedStatements=true";
        final String USER = args[1];
        final String PASS = args[2];

        System.out.println("Connecting to database...");
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);) {
            System.out.println("Connected!");
            prepareStatements(conn);

            System.out.println(balance(3));
            System.out.println(deposit(3, 1, 1, 700));
            System.out.println("Anzahl der Datebsätze mit Einzahlungsbetrag Delta: "
                    + analyse(700));

            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
