
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


        try{

            Stopwatch watch = Stopwatch.createStarted();

            // get HSQL DB connection.
            Connection conn = ConnectionManager.getHSQLDBConnection(inMemHSQLDBUrl, dbUserName, dbPassword);

            RawVolFcstInput rawVolFcstInput = new RawVolFcstInput();
            rawVolFcstInput.transpose();
            rawVolFcstInput.run(conn);

            FollowUpRatesInput followUpRatesInput = new FollowUpRatesInput();
            followUpRatesInput.transpose();
            followUpRatesInput.run(conn);

            FollowUpVolOutput followUpVolOutput = new FollowUpVolOutput();
            followUpVolOutput.run(conn);
            //followUpVolOutput.validate(conn);

            Logger.log("sql query elapsed time in ms: " + watch.elapsed(TimeUnit.MILLISECONDS));

        } catch (Exception e){
            Logger.log(e.getStackTrace().toString());
        }
    }
}
