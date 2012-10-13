package tap.core;

import junit.framework.Assert;

import org.apache.avro.mapred.AvroValue;
import org.junit.Test;

import tap.CountRec;

public class AvroConversionTests {

    @Test
    public void testAvroConversion() {
        tap.CountRec value = new CountRec();
        value.count = 23;
        value.word = "Best";
        
        AvroValue<CountRec> av = new AvroValue<CountRec>(value);

        CountRec value2 = av.datum();
        Assert.assertEquals(23, value2.count);
        Assert.assertEquals("Best", value2.word);
    }
    
    @Test
    public void testAvroRawConversion() {
        CountRec value = new CountRec();
        value.count = 23;
        value.word = "Best";
        
        AvroValue<CountRec> av3 = new AvroValue(value);

        CountRec value2 = av3.datum();
        Assert.assertEquals(23, value2.count);
        Assert.assertEquals("Best", value2.word);
    }
}
