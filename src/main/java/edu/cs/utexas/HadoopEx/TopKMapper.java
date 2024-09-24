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
			int firstInteger = Integer.parseInt(parts[0]);
			int secondInteger = Integer.parseInt(parts[1]);

			pq.add(new DriverInfo(new Text(key), new IntPairWritable(firstInteger, secondInteger)));

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

			/* Task 2 */
				int gpsErrors = driverInfo.getTotalErrors();
				int trips = driverInfo.getTotalTrips();
				
				float currentRatio = (float) gpsErrors / trips;

				context.write(driverInfo.getDriverID(), new FloatWritable(currentRatio) );
				logger.info("TopKMapper PQ Status: " + pq.toString());

			/* Task 3  */
				// 	float totalBank = driverInfo.getTotalBank();
				//     int totalSeconds = driverInfo.getTotalSeconds();
				// 	float earningPerMin = totalBank / (totalSeconds / 60);
				
				//     context.write(driverInfo.getDriverID(), new FloatWritable(earningPerMin));
				// logger.info("TopKMapper PQ Status: " + pq.toString());
			}
	}
}