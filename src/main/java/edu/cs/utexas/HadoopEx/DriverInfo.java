package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class DriverInfo implements Comparable<DriverInfo> {
    private final Text DriverID;
    private  int gpsErrors;
    private  int totalTrips;
    private  float errorRatio;
    private  float totalBank;
    private  int totalSeconds;

    public DriverInfo(Text DriverID, IntPairWritable values) {
       this.DriverID = DriverID;
       this.gpsErrors = values.getTotalErrors();
       this.totalTrips = values.getTotalTrips();
       this.errorRatio = (float) this.gpsErrors / this.totalTrips;
       this.totalBank = values.getTotalBank();
       this.totalSeconds = values.getTotalSeconds();
    }

    // public DriverInfo(Text DriverID, FloatWritable avgErrorRatio) {
    //     this.DriverID = DriverID;
    //     this.avgErrorRatio = avgErrorRatio;
    //     this.gpsErrors = 0;
    //     this.totalTrips = 0;
    //     this.totalBank = 0;
    //     this.totalSeconds = 0;
    // }

    public IntPairWritable getIntPairWritable(){
        return new IntPairWritable(gpsErrors, totalTrips);
    }

    public float getErrorRatio() {
        return errorRatio;
    }

    public Text getDriverID() {
        return DriverID;
    }

    // Task 2
    public int getTotalErrors() {
        return gpsErrors;
    }

    public int getTotalTrips() {
        return totalTrips;
    }
    
    public void updateTotals(int first, int second) {
        /* Task 2 */
            this.gpsErrors += first;
            this.totalTrips += second;
    }

    public void setAvgErrorRatio(float errorratio){
        this.errorRatio = errorratio;
    }

    // Task 3
    public float getTotalBank() {
        return totalBank;
    }


    public int getTotalSeconds() {
        return totalSeconds;
    }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
    @Override  
        public int compareTo(DriverInfo other) {

            /* Task 2 */
                // int currentRatio = totalTrips != 0 ? (int) (((float) getTotalErrors() / getTotalTrips()) * 100) : 0;
                // int otherRatio = other.getTotalTrips() != 0 ? (int) (((float) other.getTotalErrors() / other.getTotalTrips()) * 100) : 0;
                // return Integer.compare(currentRatio, otherRatio);
            /* Task 3  */
                float currentRatio = totalSeconds != 0 ? ((float) getTotalBank() / (getTotalSeconds() / 60 )) : 0;
                float otherRatio = other.getTotalSeconds() != 0 ? ((float) other.getTotalBank() / (other.getTotalSeconds() / 60 )) : 0;
                return Float.compare(currentRatio, otherRatio);
        }


        public String toString(){
            /* Task 2 */
                // int currentRatio = (int) (((float) getTotalErrors() / getTotalTrips()) * 100);
                // return "("+DriverID.toString() +" , "+ currentRatio+")";
            
            /* Task 3  */
                float currentRate = totalSeconds != 0 ? ((float) getTotalBank() / (getTotalSeconds() / 60 )) : 0;
                return "("+DriverID.toString() +" , "+ currentRate+")";

        }
}




