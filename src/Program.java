
import com.google.common.io.Files;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;

/**
 * Created by sadasik on 5/1/16.
 */
public class Program {

    public static void main(String[] args){
        String inMemHSQLDBUrl = "jdbc:hsqldb:mem:mymemdb";
        String dbUserName = "sa";
        String dbPassword = "";

        String rootDir = System.getProperty("user.dir");
        //String dataFile =  rootDir + "/../../Documents/crucible/RawVolFcst.csv";
        String dataFile = rootDir + "/data/Customers.csv";
        String transposedDataFile = rootDir + "/data/Customers_Transposed.csv";
        String outFile = rootDir + "/data/out.csv";
        String createTableQuery = toSqlQueryString(rootDir + "/sqlQueries/createTable.sql");
        String importTableQuery = toSqlQueryString(rootDir + "/sqlQueries/importFileAsTable.sql");
        String queryTable = toSqlQueryString(rootDir + "/sqlQueries/queryTable.sql");

        try{

            // transpose the input files.
            Transposer transposer = new Transposer(dataFile, 4).withOutputFileName(transposedDataFile);
            transposer.transpose();

            // get HSQL DB connection.
            Connection conn = getHSQLDBConnection(inMemHSQLDBUrl, dbUserName, dbPassword);

            // create the table.
            executeQuery(conn, createTableQuery);

            // import the table.
            executeQuery(conn, String.format(importTableQuery, dataFile));

            // query and save the output.
            queryAndSaveOutput(conn, queryTable, outFile);

        } catch (Exception e){
            Print(e.getStackTrace().toString());
        }
    }

    private static Connection getHSQLDBConnection(String inMemHSQLDBUrl, String dbUserName, String dbPassword) throws ClassNotFoundException, SQLException {
        // register HSQLDB JDBC drive.
        Class.forName("org.hsqldb.jdbcDriver");

        // allow loading files as tables into mem db from a directory.
        System.setProperty("textdb.allow_full_path", "true");

        return DriverManager.getConnection(inMemHSQLDBUrl, dbUserName, dbPassword);
    }

    private static void executeQuery(Connection conn, String query) throws SQLException{
        Statement statement = conn.createStatement();
        statement.executeUpdate(query);
    }

    private static void queryAndSaveOutput(Connection conn, String query, String outFile) throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet result = stmt.executeQuery(query);
        FileWriter fileWriter = new FileWriter(outFile);
        CSVWriter csvWriter = new CSVWriter(fileWriter, ',', CSVWriter.NO_QUOTE_CHARACTER);
        csvWriter.writeAll(result,true, true);
        csvWriter.close();
    }

    private static String toSqlQueryString(String file) {
        try{
            return Files.toString(new File(file), Charset.defaultCharset());
        } catch (IOException e){
            return "";
        }
    }

    private static void Print(String msg){
        System.out.println(msg);
    }
}
