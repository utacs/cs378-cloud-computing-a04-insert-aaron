package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntPairWritable implements Writable {
    private int errors;
    private int trips;
    private float bank;
    private int seconds;
    
    // Default constructor
    public IntPairWritable() {

    }


    // Task 2
    public IntPairWritable(int errors, int trips) {
        this.errors = errors;
        this.trips = trips;
        bank = 0;
        seconds = 0;
    }

    // Task 3
    public IntPairWritable(float bank, int seconds) {
        this.bank = bank;
        this.seconds = seconds;
        errors = 0;
        trips = 0;
    }

    // Task 2
    public int getTotalErrors() {
        return errors;
    }

    public int getTotalTrips() {
        return trips;
    }

    // Task 3
    public float getTotalBank(){
        return bank;
    }

    public int getTotalSeconds(){
        return seconds;
    }



    // Serialization
    @Override
    public void write(DataOutput out) throws IOException {

       /* Task 2 */
        out.writeInt(errors);
        out.writeInt(trips);

        // /* Task 3 */
        //     out.writeFloat(bank);
        //     out.writeInt(seconds);
    }

    // Deserialization
    @Override
    public void readFields(DataInput in) throws IOException {
        /* Task 2 */
            errors = in.readInt();
            trips = in.readInt();

        // /* Task 3 */
        //     bank = in.readFloat();
        //     seconds = in.readInt();
    }

    // @Override
    // public int compareTo(IntPairWritable o) {
    //     int cmp = Integer.compare(first, o.first);
    //     if (cmp != 0) {
    //         return cmp;
    //     }
    //     return Integer.compare(second, o.second);
    // }

    @Override
    public String toString() {
        /* Task 2  */
            return errors + "-" + trips;
        
        // /* Task 3 */
        //     return "Bank: " + bank + ", seconds: " + seconds;
    }
}
