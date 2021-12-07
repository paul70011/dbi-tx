import java.sql.*;

import java.sql.*;

public class Main {

    public static int get_current_balance(Connection conn, int ACCID){
        System.out.println("Get current balance from ACCID: " + ACCID);
        int balance = -100;
        String query = "SELECT balance FROM accounts WHERE accid = " + ACCID;
        try(Statement stmt = conn.createStatement()){
            ResultSet result = stmt.executeQuery(query);
            while(result.next()) {
                balance  = result.getInt("balance");
            }
        } catch (SQLException e) {
            e.toString();
        };
        System.out.println("Balance: " + balance);
        return balance;
    }

    public static int AnalyseTx(Connection con, int delta){
        try{
            Statement stmt=con.createStatement();


            ResultSet rs=stmt.executeQuery("SELECT count(*) from history where " +
                    "delta ="+ delta );
            while(rs.next())

            delta = rs.getInt(1);
            con.close();
            return delta;
        }catch(Exception e){ System.out.println(e);}

        return 0;
    }


    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("[ip] [username] [password]");
            System.exit(1);
        }
        final String DB_URL = "jdbc:mariadb://" + args[0] + ":3306/benchmark?rewriteBatchedStatements=true";
        final String USER = args[1];
        final String PASS = args[2];

        System.out.println("Connecting to database...");
        try(Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);) {
            System.out.println("Connected!");
            get_current_balance(conn, 3);

           System.out.println("Anzahl der Datebsätze mit Einzahlungsbetrag Delta: "
                   +AnalyseTx(conn, 5));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
