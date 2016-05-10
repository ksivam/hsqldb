import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by sadasik on 5/4/16.
 */
public class NetworkVolAllocationsInput {
    int zeroBasedDataIndex = 9;
    String rootDir = System.getProperty("user.dir");
    String toBeTransposedDataFile =  rootDir + "/../../Documents/crucible/sandbox/" + "1aNetworkVolAllocations.csv";

    // table with out catpive data
    String tableWithOutCativeDataFile =  rootDir + "/../../Documents/crucible/sandbox/" + "1aNetworkVolAllocationsTransposed.csv";
    String createTableWithoutCaptiveDataQuery = "CREATE TEXT TABLE NetworkVolAllocationsWithoutCaptiveInput (lang VARCHAR(255), ou VARCHAR(255), ags VARCHAR(255), fcstGrp VARCHAR(255), staffGrp VARCHAR(255), HndlMthd VARCHAR(255), langOu VARCHAR(255), fcstGrpId VARCHAR(255), house VARCHAR(255), week VARCHAR(255), val VARCHAR(255));";
    String setWritePremissionsToTableWithoutCaptiveData = "SET TABLE NetworkVolAllocationsWithoutCaptiveInput READONLY FALSE";
    String importTableWithOutCativaDataQuery = "set table NetworkVolAllocationsWithoutCaptiveInput source '%s;ignore_first=true';";

    // query to inset captive data
    String insertCaptiveDataToTableWithOutCaptiveDataQuery = "INSERT INTO NetworkVolAllocationsWithoutCaptiveInput SELECT T.LANG, T.OU, T.AGS, T.FCSTGRP, T.STAFFGRP, T.HNDLMTHD, T.LANGOU, T.FCSTGRPID, 'Captive', T.WEEK, 1-TRUNCATE(TO_NUMBER(T.VAL), 4) FROM NetworkVolAllocationsWithoutCaptiveInput T";

    // table with captive/outsource data
    String dataFile = rootDir + "/../../Documents/crucible/sandbox/" + "1aNetworkVolAllocationsInput.csv";
    String createTableQuery = "CREATE TEXT TABLE NetworkVolAllocationsInput (lang VARCHAR(255), ou VARCHAR(255), ags VARCHAR(255), fcstGrp VARCHAR(255), staffGrp VARCHAR(255), HndlMthd VARCHAR(255), langOu VARCHAR(255), fcstGrpId VARCHAR(255), house VARCHAR(255), week VARCHAR(255), val VARCHAR(255));";
    String setWritePremissions = "SET TABLE NetworkVolAllocationsInput READONLY FALSE";
    String importTableQuery = "set table NetworkVolAllocationsInput source '%s;ignore_first=true';";

    public void transpose() throws Exception {
        // transpose the input files.
        Transposer transposer = new Transposer(toBeTransposedDataFile, zeroBasedDataIndex).withOutputFileName(tableWithOutCativeDataFile);
        transposer.transpose();
    }

    public void run(Connection conn) throws Exception {

        // create the table without captive data
        tableWithoutCaptiveData(conn);

        // calculate 'captive' data and insert to the table with out captive data
        ConnectionManager.executeQuery(conn, insertCaptiveDataToTableWithOutCaptiveDataQuery);

        // save the table with 'captive' data as csv
        ConnectionManager.queryAndSaveOutput(conn, "SELECT * FROM NetworkVolAllocationsWithoutCaptiveInput", dataFile);

        // create and load the table with captive and outsource data
        tableWithCaptiveAndOutSourceData(conn);
    }

    private void tableWithoutCaptiveData(Connection conn) throws SQLException {
        // create the table
        ConnectionManager.executeQuery(conn, createTableWithoutCaptiveDataQuery);

        // give write premission to table.
        ConnectionManager.executeQuery(conn, setWritePremissionsToTableWithoutCaptiveData);

        // create table index
        ConnectionManager.executeQuery(conn, "CREATE INDEX NetworkVolAllocationsWithoutCaptiveInputIndex ON NetworkVolAllocationsWithoutCaptiveInput (fcstGrpId,week,house)");

        // import the table.
        ConnectionManager.executeQuery(conn, String.format(importTableWithOutCativaDataQuery, tableWithOutCativeDataFile));
    }

    private void tableWithCaptiveAndOutSourceData(Connection conn) throws Exception {

        // create the table
        ConnectionManager.executeQuery(conn, createTableQuery);

        // give write premission to table.
        ConnectionManager.executeQuery(conn, setWritePremissions);

        // create table index
        ConnectionManager.executeQuery(conn, "CREATE INDEX NetworkVolAllocationsInputIndex ON NetworkVolAllocationsInput (fcstGrpId,week,house)");

        // import the table.
        ConnectionManager.executeQuery(conn, String.format(importTableQuery, dataFile));
    }
}
