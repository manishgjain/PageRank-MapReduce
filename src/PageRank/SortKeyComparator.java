package PageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 

public class SortKeyComparator extends WritableComparator {
     
    protected SortKeyComparator() {
        super(DoubleWritable.class, true);
    }
 
    /**
     * Compares in the descending order of the keys.
     */
    @SuppressWarnings("rawtypes")
	@Override
    public int compare(WritableComparable a, WritableComparable b) {
        DoubleWritable o1 = (DoubleWritable) a;
        DoubleWritable o2 = (DoubleWritable) b;
        if(o1.get() < o2.get()) {
            return 1;
        }else if(o1.get() > o2.get()) {
            return -1;
        }else {
            return 0;
        }
    }
     
}