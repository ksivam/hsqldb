
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
        //String inMemHSQLDBUrl = "jdbc:hsqldb:file:./db/filedb3;hsqldb.write_delay=true;shutdown=true;hsqldb.sqllog=0;hsqldb.applog=0;hsqldb.log_data=false;hsqldb.log_size=0;";
        String inMemHSQLDBUrl = "jdbc:hsqldb:mem:mymemdb;hsqldb.write_delay=true;shutdown=true;hsqldb.sqllog=0;hsqldb.applog=0;hsqldb.log_data=false;hsqldb.log_size=0;";

        String dbUserName = "sa";
        String dbPassword = "";

        try{

            Stopwatch watch = Stopwatch.createStarted();

            // get HSQL DB connection.
            Connection conn = ConnectionManager.getHSQLDBConnection(inMemHSQLDBUrl, dbUserName, dbPassword);

            ConnectionManager.setCaseInsensitiveComparisionOnDbLevel(conn);

            RawVolFcstInput rawVolFcstInput = new RawVolFcstInput();
            rawVolFcstInput.transpose();
            rawVolFcstInput.run(conn);

            FollowUpRatesInput followUpRatesInput = new FollowUpRatesInput();
            followUpRatesInput.transpose();
            followUpRatesInput.run(conn);

            FollowUpVolOutput followUpVolOutput = new FollowUpVolOutput();
            followUpVolOutput.run(conn);
            //followUpVolOutput.validate(conn);

            NetworkVolAllocationsInput networkVolAllocationsInput = new NetworkVolAllocationsInput();
            networkVolAllocationsInput.transpose();
            networkVolAllocationsInput.run(conn);

            OSNetworkFcstInclTransfersOutput osNetworkFcstInclTransfersOutput = new OSNetworkFcstInclTransfersOutput();
            osNetworkFcstInclTransfersOutput.run(conn);

            Logger.log("sql query elapsed time in ms: " + watch.elapsed(TimeUnit.MILLISECONDS));

        } catch (Exception e){
            Logger.log(e.getStackTrace().toString());
        }
    }
}
