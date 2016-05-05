import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by sadasik on 5/4/16.
 */
public class FollowUpVolOutput {
    String rootDir = System.getProperty("user.dir");
    String outFile = rootDir + "/../../Documents/crucible/sandbox/out.csv";

    String followUpVolQuery = ConnectionManager.toSqlQueryString(rootDir + "/sqlQueries/queryTable.sql");

    public void run(Connection conn) throws Exception {
        // query and save the output.
        ConnectionManager.queryAndSaveOutput(conn, followUpVolQuery, outFile);
    }
}
