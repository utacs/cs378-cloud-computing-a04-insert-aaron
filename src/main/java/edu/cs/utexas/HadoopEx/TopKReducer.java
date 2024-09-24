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


public class TopKReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private PriorityQueue<DriverInfo> pq = new PriorityQueue<DriverInfo>(5);;


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

       
        // Task 2
            float totalPercentage = 0;
            int count = 0;

            for (FloatWritable value : values) {
                totalPercentage += value.get();
                count++;
            }

            float averagePercentage = totalPercentage / count;

            // Add to priority queue if the size is less than 5 or if this driver has a higher percentage
            if (pq.size() < 5) {
                pq.add(new DriverInfo(key, new FloatWritable(averagePercentage)));
            } else if (pq.peek().getAvgErrorRatio().get() < averagePercentage) {
                pq.poll();
                pq.add(new DriverInfo(key, new FloatWritable(averagePercentage)));
            }

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


        for (DriverInfo value : values) {

            context.write(value.getDriverID(), value.getAvgErrorRatio());

            
            logger.info("TopKReducer - Top-5 Drivers are:  " + value.getDriverID() + "  ErrorRatio:"+ value.getAvgErrorRatio() );
        }
    }
}