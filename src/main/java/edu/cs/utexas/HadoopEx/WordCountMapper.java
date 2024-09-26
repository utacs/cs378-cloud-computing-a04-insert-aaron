package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.cs.utexas.HadoopEx.IntPairWritable;


// /* Task 1 */
// public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
/* Task 2 & 3 WCM */
public class WordCountMapper extends Mapper<Object, Text, Text, IntPairWritable> {


	 private Logger logger = Logger.getLogger(WordCountMapper.class);

	// Create a counter and initialize with 1
	// private final IntWritable counter = new IntWritable(1);
	int trip = 1;
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] parsedText = value.toString().split(","); //value is line of Hadoop text

		if(isInvalidLine(parsedText)){
		    logger.info("Invalid line: " + value);
			return;
		}

		int error = 0;



		// logger.info("Before GPS");
		// Extracting GPS coordinates
		String pickupLatitude = parsedText[6];
		String pickupLongitude = parsedText[7];
		String dropoffLatitude = parsedText[8];
		String dropoffLongitude = parsedText[9];

		// logger.info("AFter GPS try");

		if(invalidGPS(pickupLatitude, pickupLongitude, dropoffLatitude, dropoffLongitude)){
			/* Task 1 */
				// String[] dateInfo = parsedText[2].split(" "); // dateInfo format = 2013-01-01 00:02:00
				// String timeFormat = dateInfo[1];
				// logger.info("TimeFormat: " + timeFormat);

				// String [] hourTime = timeFormat.split(":");
				// int hourInt = Integer.parseInt(hourTime[0]); // 00 -> returns 0.
				// logger.info("\nhour: " + hourInt);
				
					// if(hourInt > -1 && hourInt < 24){
					// 	// logger.info("NINCEEEE");
					// 	word.set(hourTime[0]);
					// 	context.write(word, new IntWritable(1));
					// }
					// return;
			// logger.info("whattttttt");
			
			/* Task 2 and 3 */
			error = 1;
		}

		// logger.info("PIZZZAAAA");

		


		/* Task 2 */	
		// IntPairWritable dataPair = new IntPairWritable(error, trip);
		// word.set(parsedText[0]);
		// context.write(word, dataPair); 


		/* Task 3  */
			float amount = Float.parseFloat(parsedText[16]);
			int seconds = Integer.parseInt(parsedText[4]);
			// logger.info("Driver: " + key.toString() +  ",Amount: " + amount + " seconds: " + seconds);
			IntPairWritable moneyPair = new IntPairWritable(amount, seconds);
			context.write(word, moneyPair); 
	}

	// Helper function to check if any of the GPS coordinates contain errors (zeros or empty strings)
    private boolean invalidGPS(String pickupLat, String pickupLon, String dropoffLat, String dropoffLon) {
		try {
			double pickupLatVal = Double.parseDouble(pickupLat);
			double pickupLonVal = Double.parseDouble(pickupLon);
			double dropoffLatVal = Double.parseDouble(dropoffLat);
			double dropoffLonVal = Double.parseDouble(dropoffLon);
	
			// Check if any of the GPS values are 0.0 or empty
			return pickupLatVal == 0.0 || pickupLonVal == 0.0 || 
				   dropoffLatVal == 0.0 || dropoffLonVal == 0.0;
		} catch (NumberFormatException e) {
			// If the parsing fails, it means the value is not a valid number
			return true;
		}
	}
	

	public boolean isInvalidLine(String [] line){
		if (line.length != 17){
			return true;
		}
		try{
			float fare = Float.parseFloat(line[11]);
			float surcharge = Float.parseFloat(line[12]);
			float mtaTax = Float.parseFloat(line[13]);
			float tip = Float.parseFloat(line[14]);
			float tolls = Float.parseFloat(line[15]);
			float totalAmount = Float.parseFloat(line[16]);

			float calculatedTotal = fare + surcharge + mtaTax + tip + tolls;
			if(Math.abs(calculatedTotal - totalAmount) > .01){
				return true;
			}
		}catch (NumberFormatException e){
			return true;
		}

		return false;
	}
}