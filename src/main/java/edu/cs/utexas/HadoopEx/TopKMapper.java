package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.Map;
// import edu.cs.utexas.HadoopEx.MapReduceTest.Map;

public class TopKMapper extends Mapper<Text, Text, Text, IntPairWritable> {
    private Logger logger = Logger.getLogger(TopKMapper.class);


	private PriorityQueue<DriverInfo> pq = new PriorityQueue<>(5);
	// int count =0;

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Task 2 */

			// String [] parts = value.toString().split("-");

			// int gpsErrors = Integer.parseInt(parts[0]);
			// int trips = Integer.parseInt(parts[1]);
		

			// DriverInfo newDriver = new DriverInfo(new Text(key), new IntPairWritable(gpsErrors, trips));

			// pq.add(newDriver);


			// if (pq.size() > 5){
			// 	pq.poll();
			// }

			
		/* Task 3 */

			String [] parts = value.toString().split("-");

			float totalBank = Float.parseFloat(parts[0]);
			int totalSeconds = Integer.parseInt(parts[1]);


			DriverInfo newDriver = new DriverInfo(new Text(key), new IntPairWritable(totalBank, totalSeconds));

			pq.add(newDriver);
			

			if (pq.size() > 10){
				pq.poll();
			}

	}

	public void cleanup(Context context) throws IOException, InterruptedException {

		while (pq.size() > 0) {
			DriverInfo driverInfo = pq.poll();

			
			// logger.info(count++);
			

			/* Task 2 */

				// int currentRatio = driverInfo.getTotalTrips() != 0 ? (int) (((float) driverInfo.getTotalErrors() / driverInfo.getTotalTrips()) * 100) : 0;

				// logger.info("Driver: " + driverInfo.getDriverID() + " AvgErrorRate: " + currentRatio);
				// context.write(driverInfo.getDriverID(), new IntPairWritable(driverInfo.getTotalErrors(), driverInfo.getTotalTrips()));		

			/* Task 3  */
				
				context.write(driverInfo.getDriverID(), new IntPairWritable(driverInfo.getTotalBank(), driverInfo.getTotalSeconds()));
				// logger.info("TopKMapper PQ Status: " + pq.toString());
			}
	}
}