
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by sadasik on 5/1/16.
 */
public class Program {

    public static void main(String[] args){
        String inMemHSQLDBUrl = "jdbc:hsqldb:mem:mymemdb";
        String dbUserName = "sa";
        String dbPassword = "";

        int rawVolFcstDataIndex = 8;
        int followUpRatesDataIndex = 8;

        int zeroBasedDataIndex = followUpRatesDataIndex;

        String rootDir = System.getProperty("user.dir");
        String toBeTransposedDataFile =  rootDir + "/../../Documents/crucible/" + "1aFollowUpRates.csv"; //"1aRawVolFcst.csv";
        String dataFile = rootDir + "/../../Documents/crucible/" + "1aFollowUpRatesInput.csv"; //"1aRawVolFcstInput.csv";
        //String toBeTransposedDataFile = rootDir + "/data/Customers.csv";
        //String dataFile = rootDir + "/data/Customers_Transposed.csv";
        String outFile = rootDir + "/../../Documents/crucible/out.csv";
        String createTableQuery = toSqlQueryString(rootDir + "/sqlQueries/createTable.sql");
        String importTableQuery = toSqlQueryString(rootDir + "/sqlQueries/importFileAsTable.sql");
        String queryTable = toSqlQueryString(rootDir + "/sqlQueries/queryTable.sql");

        try{

            // transpose the input files.
            Transposer transposer = new Transposer(toBeTransposedDataFile, zeroBasedDataIndex).withOutputFileName(dataFile);
            transposer.transpose();

            if(false) {
                Stopwatch watch = Stopwatch.createStarted();

                // get HSQL DB connection.
                Connection conn = getHSQLDBConnection(inMemHSQLDBUrl, dbUserName, dbPassword);

                // create the table.
                executeQuery(conn, createTableQuery);

                // import the table.
                executeQuery(conn, String.format(importTableQuery, dataFile));

                // query and save the output.
                queryAndSaveOutput(conn, queryTable, outFile);

                Print("sql query elapsed time in ms: " + watch.elapsed(TimeUnit.MILLISECONDS));
            }

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
