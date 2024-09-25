package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;

public class TopKMapper extends Mapper<Text, Text, Text, FloatWritable> {
    private Logger logger = Logger.getLogger(TopKMapper.class);


	private PriorityQueue<DriverInfo> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Task 2 */

			String [] parts = value.toString().split("-");

			int gpsErrors = Integer.parseInt(parts[0]);
			int trips = Integer.parseInt(parts[1]);
			// logger.info("GPS: " + gpsErrors + "Trips: "+ trips);


			// Calculate error ratio

			DriverInfo newDriver = new DriverInfo(new Text(key), new IntPairWritable(gpsErrors, trips));

			// pq.add(new DriverInfo(new Text(key), new FloatWritable(errorRatio)));

			boolean found = false;
			for (DriverInfo driver : pq) {
				if (driver.getDriverID().equals(newDriver.getDriverID())) {
					driver.updateTotals(gpsErrors, trips);  // Assuming updateTotals is a method that adds totals
					found = true;
					break;
				}
			}
		
			// If the driver is not in the priority queue, add it
			if (!found) {
				pq.add(newDriver);
			}

			if (pq.size() > 5) {
				pq.poll();
			}
		
		/* Task 3 */

			// if (pq.size() > 10){
			// 	pq.poll();
			// }

	}

	public void cleanup(Context context) throws IOException, InterruptedException {

		while (pq.size() > 0) {
			DriverInfo driverInfo = pq.poll();

			logger.info("Driver: " + driverInfo.getDriverID() + " AvgErrorRate: " + driverInfo.getAvgErrorRatio());

			/* Task 2 */
			context.write(driverInfo.getDriverID(), driverInfo.getAvgErrorRatio());		

			/* Task 3  */
				// 	float totalBank = driverInfo.getTotalBank();
				//     int totalSeconds = driverInfo.getTotalSeconds();
				// 	float earningPerMin = totalBank / (totalSeconds / 60);
				
				//     context.write(driverInfo.getDriverID(), new FloatWritable(earningPerMin));
				// logger.info("TopKMapper PQ Status: " + pq.toString());
			}
	}
}