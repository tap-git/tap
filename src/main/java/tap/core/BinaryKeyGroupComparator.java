package tap.core;

import org.apache.avro.mapred.AvroWrapper;

import tap.core.io.BinaryKey;



public class BinaryKeyGroupComparator extends BinaryKeyComparator {

	
	 public int compare(AvroWrapper<BinaryKey> x, AvroWrapper<BinaryKey> y) {
	    	BinaryKey k1 = x.datum(), k2 = y.datum();
	    	
	    	int length1 = k1.groupBytesLength() > 0 ? k1.groupBytesLength() : k1.keyBytesLength();
	    	int length2 = k2.groupBytesLength() > 0 ? k2.groupBytesLength() : k2.keyBytesLength();
		    	
	    	length1 += BinaryKey.KEY_BYTES_OFFSET;
	    	length2 += BinaryKey.KEY_BYTES_OFFSET;
	    	return super.compareBinary(
	    			k1.getBuffer(), 0, length1,
	    			k2.getBuffer(), 0, length2);
	    }
}
