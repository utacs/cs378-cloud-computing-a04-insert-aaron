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


public class TopKReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private PriorityQueue<DriverInfo> pq = new PriorityQueue<DriverInfo>(5);;
    private HashSet<String> processedTaxis = new HashSet<>();


    private Logger logger = Logger.getLogger(TopKReducer.class);


//    public void setup(Context context) {
//
//        pq = new PriorityQueue<WordAndCount>(10);
//    }


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {

       // task 2 try:


        String taxiID = key.toString();
        
        // Check if this taxi has already been processed
        if (processedTaxis.contains(taxiID)) {
            logger.info("Duplicate taxi: " + taxiID + " found. Skipping...");
            return;
        }
        
        processedTaxis.add(taxiID);  // Add the taxi ID to the set

        // Task 2: Calculate average error ratio
        float totalErrorRatio = 0;
        int count = 0;

        for (FloatWritable value : values) {
            totalErrorRatio += value.get();
            count++;
        }

        float averageErrorRatio = totalErrorRatio / count;

        // Add to priority queue if the size is less than 5 or if this driver has a higher percentage
        if (pq.size() < 5) {
            pq.add(new DriverInfo(key, new FloatWritable(averageErrorRatio)));
        } else if (pq.peek().getAvgErrorRatio().get() < averageErrorRatio) {
            pq.poll();  // Remove the driver with the lowest ratio
            pq.add(new DriverInfo(key, new FloatWritable(averageErrorRatio)));
        }
        
        // // Task 2
        //     float totalErrorRatio = 0;
        //     int count = 0;

        //     for (FloatWritable value : values) {
        //         totalErrorRatio += value.get();
        //         count++;
        //     }

        //     float averageErrorRatio = totalErrorRatio / count;

        //     // Add to priority queue if the size is less than 5 or if this driver has a higher percentage
        //     if (pq.size() < 5) {
        //         pq.add(new DriverInfo(key, new FloatWritable(averageErrorRatio)));
        //     } else if (pq.peek().getAvgErrorRatio().get() < averageErrorRatio) {
        //         pq.poll();
        //         pq.add(new DriverInfo(key, new FloatWritable(averageErrorRatio)));
        //     }

        // Task 3
            // float totalBank = 0;

            // for (FloatWritable value : values) {
            //     totalBank += value.get();
            //     totalPercentage += value.get();
            //     count++;
            // }

            // float averageEarningPerMin = totalBank / (totalPercentage / 60);

            // // Add to priority queue if the size is less than 10 or if this driver has a higher earnings per minute
            // if (pq.size() < 10) {
            //     pq.add(new DriverInfo(key, new FloatWritable(averageEarningPerMin)));
            // } else if (pq.peek().getAvgErrorRatio().get() < averageEarningPerMin) {
            //     pq.poll();
            //     pq.add(new DriverInfo(key, new FloatWritable(averageEarningPerMin)));
            // }

   }


    public void cleanup(Context context) throws IOException, InterruptedException {
        // logger.info("TopKReducer cleanup cleanup.");
        // logger.info("pq.size() is " + pq.size());

        // List<DriverInfo> values = new ArrayList<DriverInfo>(5);

        // while (pq.size() > 0) {
        //     values.add(pq.poll());
        // }

        // logger.info("values.size() is " + values.size());
        // logger.info(values.toString());


        // // reverse so they are ordered in descending order
        // Collections.reverse(values);


        // for (DriverInfo value : values) {

        //     context.write(value.getDriverID(), value.getAvgErrorRatio());

            
        //     logger.info("TopKReducer - Top-5 Drivers are:  " + value.getDriverID() + "  ErrorRatio:"+ value.getAvgErrorRatio() );
        // }

        List<DriverInfo> values = new ArrayList<DriverInfo>(5);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());

        // reverse so they are ordered in descending order
        Collections.reverse(values);

        for (DriverInfo value : values) {
            context.write(value.getDriverID(), value.getAvgErrorRatio());
            logger.info("TopKReducer - Top-5 Drivers are:  " + value.getDriverID() + "  ErrorRatio:"+ value.getAvgErrorRatio() );
        }   
    }
}