package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class DriverInfo implements Comparable<DriverInfo> {
    private final Text DriverID;
    private final int gpsErrors;
    private final int totalTrips;

    public DriverInfo(Text DriverID, IntPairWritable values) {
       this.DriverID = DriverID;
       this.gpsErrors = values.getTotalErrors();
       this.totalTrips = values.getTotalTrips();
    }

    public Text getDriverID() {
        return DriverID;
    }

    public int getTotalErrors() {
        return gpsErrors;
    }

    public int getTotalTrips() {
        return totalTrips;
    }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
    @Override  // For sorting in descending order of driver error ratio
        public int compareTo(DriverInfo other) {
            int currentRatio = (int) (((float) getTotalErrors() / getTotalTrips()) * 100);
            int otherRatio = (int) (((float) other.getTotalErrors() / other.getTotalErrors()) * 100);
            int diff = currentRatio - otherRatio;
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){
            int currentRatio = (int) (((float) getTotalErrors() / getTotalTrips()) * 100);
            return "("+DriverID.toString() +" , "+ currentRatio+")";
        }
}




