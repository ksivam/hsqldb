import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Created by sadasik on 5/4/16.
 */
public class FollowUpVolOutput {
    String rootDir = System.getProperty("user.dir");
    String outFile = rootDir + "/../../Documents/crucible/sandbox/1aFollowUpVolOutput.csv";
    String actualOutFile = rootDir + "/../../Documents/crucible/sandbox/qOut.csv";

    String followUpVolQuery = ConnectionManager.toSqlQueryString(rootDir + "/sqlQueries/queryTable.sql");

    String createOutputTable = "CREATE TEXT TABLE %s (lang VARCHAR(255), ou VARCHAR(255), ags VARCHAR(255), fcstGrp VARCHAR(255), staffGrp VARCHAR(255), HndlMthd VARCHAR(255), fcstGrpId VARCHAR(255), week VARCHAR(255), val DECIMAL(18,4), valFR DECIMAL(18,4), valRVF DECIMAL(18,4));";
    String importTableQuery = "set table %1$s source '%2$s;ignore_first=true'";

    String actualOutputTableName = "O1";
    String outputTableName = "O2";

    String compareTableQuery = "SELECT * FROM %1$s MINUS SELECT * FROM %2$s";

    public void run(Connection conn) throws Exception {
        // query and save the output.
        ConnectionManager.queryAndSaveOutput(conn, followUpVolQuery, outFile);
    }

    public void validate(Connection conn) throws Exception {

        // create/import actual output file
        ConnectionManager.executeQuery(conn, String.format(createOutputTable, actualOutputTableName));
        ConnectionManager.executeQuery(conn, String.format(importTableQuery, actualOutputTableName, actualOutFile));

        // create/import the output file
        ConnectionManager.executeQuery(conn, String.format(createOutputTable, outputTableName));
        ConnectionManager.executeQuery(conn, String.format(importTableQuery, outputTableName, outFile));

        ResultSet resultSet = ConnectionManager.query(conn, String.format(compareTableQuery, actualOutputTableName, outputTableName));
        ConnectionManager.printResultSet(resultSet);
    }
}
