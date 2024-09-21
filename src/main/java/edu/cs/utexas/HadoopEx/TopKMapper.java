package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;

public class TopKMapper extends Mapper<Text, IntPairWritable, Text, IntPairWritable> {
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
	public void map(Text key, IntPairWritable value, Context context)
			throws IOException, InterruptedException {


		pq.add(new DriverInfo(new Text(key), new IntPairWritable(value.getTotalErrors(), value.getTotalTrips())));

		if (pq.size() > 3) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			WordAndCount wordAndCount = pq.poll();
			String [] currentInfo = wordAndCount.getInfo().toString().split(",");
            int currentRatio = (int) (Float.parseFloat(currentInfo[0]) / Integer.parseInt(currentInfo[1]));
			Text ratioText = new Text(String.valueOf(currentRatio));
			context.write(wordAndCount.getWord(), ratioText);
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}
}