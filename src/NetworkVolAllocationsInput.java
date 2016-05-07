import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by sadasik on 5/4/16.
 */
public class NetworkVolAllocationsInput {
    int zeroBasedDataIndex = 9;
    String rootDir = System.getProperty("user.dir");
    String toBeTransposedDataFile =  rootDir + "/../../Documents/crucible/sandbox/" + "1aNetworkVolAllocations.csv";
    String dataFile = rootDir + "/../../Documents/crucible/sandbox/" + "1aNetworkVolAllocationsInput.csv";

    String createTableQuery = "CREATE TEXT TABLE NetworkVolAllocationsInput (lang VARCHAR(255), ou VARCHAR(255), ags VARCHAR(255), fcstGrp VARCHAR(255), staffGrp VARCHAR(255), HndlMthd VARCHAR(255), lang-ou VARCHAR(255), fcstGrpId VARCHAR(255), house VARCHAR(255), week VARCHAR(255), val VARCHAR(255));";
    String importTableQuery = "set table NetworkVolAllocationsInput source '%s;ignore_first=true'";

    public void transpose() throws Exception {
        // transpose the input files.
        Transposer transposer = new Transposer(toBeTransposedDataFile, zeroBasedDataIndex).withOutputFileName(dataFile);
        transposer.transpose();
    }

    public void run(Connection conn) throws SQLException {
        // create the table.
        ConnectionManager.executeQuery(conn, createTableQuery);

        // import the table.
        ConnectionManager.executeQuery(conn, String.format(importTableQuery, dataFile));
    }
}
