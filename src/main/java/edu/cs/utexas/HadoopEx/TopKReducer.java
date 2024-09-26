package edu.cs.utexas.HadoopEx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.log4j.Logger;
import java.util.HashSet;


public class TopKReducer extends Reducer<Text, IntPairWritable, Text, FloatWritable> {
    private PriorityQueue<DriverInfo> pq = new PriorityQueue<DriverInfo>();;
    // private HashSet<String> processedTaxis = new HashSet<>();


    private Logger logger = Logger.getLogger(TopKReducer.class);

    List<DriverInfo> topKDrivers = new ArrayList<DriverInfo>();




//    public void setup(Context context) {
//
//        pq = new PriorityQueue<WordAndCount>(10);
//    
    int count = 0;


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntPairWritable> values, Context context)
    throws IOException, InterruptedException {


        String currentDriver = key.toString(); // Will need to change
        // Task 2: Calculate average error ratio
            int totalErrors = 0;
            int totalTrips = 0;

            
            for (IntPairWritable value : values) {
                totalErrors += value.getTotalErrors(); 
                totalTrips += value.getTotalTrips(); 
            }

            // logger.info("CurrentDriver: " + currentDriver + ", totalErrors: " + totalErrors + ", totalTrips: " + totalTrips);

            DriverInfo driver = new DriverInfo(new Text(key), new IntPairWritable(totalErrors, totalTrips));
            // topKDrivers.add(driver);

            float errorRatio = (float) (totalErrors / totalTrips);

            if (pq.size() < 5) {
                pq.add(driver);
            } else if (pq.peek().getErrorRatio() < errorRatio) {
                pq.poll();
                pq.add(driver);
            }

        // Task 3
            // float totalBank = 0;
            // int totalSeconds = 0;

            // for (IntPairWritable value : values) {
            //     totalBank += value.getTotalBank();
            //     totalSeconds += value.getTotalSeconds();
            // }

            // DriverInfo driver = new DriverInfo(new Text(key), new IntPairWritable(totalBank, totalSeconds));

            // float errorRatio = (float) (totalBank / totalSeconds);

            // // logger.info("Driver: " + driver.getDriverID() + " AvgErrorRate: " + errorRatio);


            // if (pq.size() < 10) {
            //     pq.add(driver);
            // } else if (pq.peek().getErrorRatio() < errorRatio) {
            //     pq.poll();
            //     pq.add(driver);
            // }

   }


    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<DriverInfo> values = new ArrayList<DriverInfo>(5);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());


        // reverse so they are ordered in descending order
        Collections.reverse(values);


        

        /* Task 2 */

            for (DriverInfo value : values) {

                logger.info("\nTopKReducer - Top-5 Drivers are:  " + value.getDriverID() + "  ErrorRatio:"+ value.getErrorRatio() );


                context.write(value.getDriverID(), new FloatWritable(value.getErrorRatio() * 100));

            } 

        /* Task 3  */
            // for (DriverInfo value : values) {

            //     context.write(value.getDriverID(), new FloatWritable((float) value.getErrorRatio() * 100));

                
            //     logger.info("TopKReducer - Top-10 Drivers are:  " + value.getDriverID() + "  ErrorRatio:"+ value.getErrorRatio() );
            // }  
    }
}