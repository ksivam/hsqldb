import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.io.*;
import java.util.List;

/**
 * Created by sadasik on 5/3/16.
 input:
 - csv flat file
 - starting column index of week data

 process:
 - create the output file as <inputFileName>_Transposed.csv
 - create the week data column metadata array by taking the first line of input file and split the string from week data column index to end of line into comma sep array [Max size ~= 52*3]
 -- get the first part string till week data index and append it with string ",week,value" and write to the output file.
 - Read from 2nd line till end of file
 -- for each row, split the line into 2 parts: first part the string till week data start column index and second part split into comma sep array
 -- validate the second part array lenght is equal to the column metadata array lenght
 -- for each item in the second part array, form a string with first part string + "," + week data column metadata[item-index] +"," + item and write to the outfile.

 csv file:
 ---------
 lang,ou,ags,fcstGrp,week1,week2,week3,week4
 US,US,NonAGS,IND,10,20,30,40
 US,US,NonAGS,Feedback,1,2,3,4
 CN,US,AGS,Pro,100,200,300,400

 week data index: 5

 output:
 -------
 lang,ou,ags,fcstGrp,week,value
 US,US,NonAGS,IND,week1,10
 US,US,NonAGS,IND,week2,20
 US,US,NonAGS,IND,week3,30
 US,US,NonAGS,IND,week4,40
 US,US,NonAGS,Feedback,week1,1
 US,US,NonAGS,Feedback,week2,2
 US,US,NonAGS,Feedback,week3,3
 US,US,NonAGS,Feedback,week4,4
 CN,US,AGS,Pro,week1,100
 CN,US,AGS,Pro,week2,200
 CN,US,AGS,Pro,week3,300
 CN,US,AGS,Pro,week4,400
 */
public class Transposer {

    private final String fileName;
    private final int dataIndex;
    private String outFileName;

    public Transposer(String filePath, int zeroBasedDataIndex){
        this.fileName = filePath;
        this.dataIndex = zeroBasedDataIndex;
    }

    public Transposer withOutputFileName(String outputFileName){
        this.outFileName = outputFileName;
        return this;
    }

    public void transpose() throws Exception {
        Joiner joiner = Joiner.on(",");
        BufferedReader reader = new BufferedReader(new FileReader(this.fileName));
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.outFileName));

        // read the first line to get the column metadata
        String columnMetadataString = reader.readLine();
        List<String> columnMetadata = Lists.newArrayList(columnMetadataString.split(","));

        // get the non data column as string
        String newTableColumn = joiner.join(columnMetadata.subList(0, dataIndex)) + ",week,value";
        writer.write(newTableColumn);
        writer.newLine();

        String rowString = null;
        // read each row
        while((rowString = reader.readLine()) != null){
            List<String> row = Lists.newArrayList(rowString.split(","));

            // validation
            if(columnMetadata.size() != row.size()) {
                log("row doesn't match column size: " + rowString);
                continue;
            }

            // get the first row part till the week data
            String outTableRowPart1 = joiner.join(row.subList(0, dataIndex)) + ",";

            for(int i = dataIndex; i < row.size(); i++) {
                // transpose the row data into column
                String outTableRowPart2 = columnMetadata.get(i) + "," + row.get(i);
                String outTableRow = outTableRowPart1 + outTableRowPart2;

                // write to the outfile
                writer.write(outTableRow);
                writer.newLine();
            }
        }

        writer.flush();
        writer.close();
        reader.close();
    }

    private static void log(String s) {
        System.out.println(s);
    }
}
