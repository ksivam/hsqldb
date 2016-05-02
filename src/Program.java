import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sadasik on 5/1/16.
 */
public class Program {

    public static void main(String[] args){
        String filePath = System.getProperty("user.dir") + "/data/Customers.csv";

        try{
            System.setProperty("textdb.allow_full_path", "true");
            Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "");

            createTable(conn);

            Statement stms = conn.createStatement();
            stms.executeUpdate("set table Customers source '"+ filePath +"';");

            printTableData(conn, "SELECT * FROM PUBLIC.Customers");

        } catch (Exception e){
            Print(e.getStackTrace().toString());
        }
    }

    private static void createTable(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = "CREATE TEXT TABLE PUBLIC.Customers " +
                "(id INTEGER not NULL, " +
                " first VARCHAR(255), " +
                " last VARCHAR(255), " +
                " PRIMARY KEY ( id ))";

        stmt.executeUpdate(sql);
    }

    private static void printTableData(Connection conn, String query) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet result = stmt.executeQuery(query);
        int columnCount = result.getMetaData().getColumnCount();
        while(result.next()){
            for (int i = 1; i <= columnCount; i++){
                Print(result.getString(i));
            }
        }
    }

    private static void Print(String msg){
        System.out.println(msg);
    }
}
