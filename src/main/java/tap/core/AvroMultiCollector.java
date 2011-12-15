package tap.core;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;

public abstract class AvroMultiCollector<OUT> extends AvroCollector<OUT> {
    public void collect(OUT datum, String multiName) throws IOException {
        collect(datum);
    }
}