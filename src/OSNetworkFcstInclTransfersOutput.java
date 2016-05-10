import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sadasik on 5/4/16.
 */
public class OSNetworkFcstInclTransfersOutput {
    String rootDir = System.getProperty("user.dir");
    String outFile = rootDir + "/../../Documents/crucible/sandbox/1aOSNetworkFcstInclTransfersOutput.csv";

    String oSNetworkFcstInclTransfersQuery = "SELECT T1.lang, T1.ou, T1.ags, T1.fcstGrp, T1.StaffGrp, T1.HndlMthd, T1.fcstGrpId, T1.week, TRUNCATE(TO_NUMBER(T1.val) * TO_NUMBER(T2.val), 4)AS VAL FROM NetworkVolAllocationsInput AS T1 INNER JOIN RawVolFcstInput AS T2 ON T1.fcstGrpId = T2.fcstGrpId AND T1.week = T2.week AND T1.house = 'OUTSOURCE'";


    public void run(Connection conn) throws Exception {
        // query and save the output.
        ConnectionManager.queryAndSaveOutput(conn, oSNetworkFcstInclTransfersQuery, outFile);

    }
}
