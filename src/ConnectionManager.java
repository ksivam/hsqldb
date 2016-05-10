import com.google.common.io.Files;
import com.opencsv.CSVWriter;
import com.sun.corba.se.spi.orbutil.fsm.Guard;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sadasik on 5/4/16.
 */
public class ConnectionManager {
    public static Connection getHSQLDBConnection(String inMemHSQLDBUrl, String dbUserName, String dbPassword) throws ClassNotFoundException, SQLException {
        // register HSQLDB JDBC drive.
        Class.forName("org.hsqldb.jdbcDriver");

        // allow loading files as tables into mem db from a directory.
        System.setProperty("textdb.allow_full_path", "true");

        return DriverManager.getConnection(inMemHSQLDBUrl, dbUserName, dbPassword);
    }

    public static void executeQuery(Connection conn, String query) throws SQLException{
        Statement statement = conn.createStatement();
        statement.executeUpdate(query);
    }

    public static ResultSet query(Connection conn, String query) throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet result = null;
        try {
            result = stmt.executeQuery(query);
        } catch (SQLException ex){
            Logger.log(ex.getStackTrace().toString());
            throw ex;
        }

        return result;
    }

    public static void queryAndSaveOutput(Connection conn, String query, String outFile) throws Exception {
        ResultSet result = query(conn, query);

        FileWriter fileWriter = new FileWriter(outFile);
        CSVWriter csvWriter = new CSVWriter(fileWriter, ',', CSVWriter.NO_QUOTE_CHARACTER);
        csvWriter.writeAll(result,true, true);
        csvWriter.close();
    }

    public static String toSqlQueryString(String file) {
        try{
            return Files.toString(new File(file), Charset.defaultCharset());
        } catch (IOException e){
            return "";
        }
    }

    public static void printResultSet(ResultSet resultSet) throws SQLException {
        StringBuilder builder = new StringBuilder();
        int columnCount = resultSet.getMetaData().getColumnCount();
        //String s = resultSet.getString(1);
        while (resultSet.next()) {
            for (int i = 0; i < columnCount;) {
                builder.append(resultSet.getString(i + 1));
                if (++i < columnCount) builder.append(",");
            }
            builder.append("\r\n");
        }
        String resultSetAsString = builder.toString();

        Logger.log(resultSetAsString);
    }

    public static void setCaseInsensitiveComparisionOnDbLevel(Connection conn) throws SQLException {
        String query = "SET IGNORECASE TRUE";
        executeQuery(conn, query);
    }
}
