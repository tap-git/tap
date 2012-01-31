package tap.sample;

// TODO: this needs to be modified to work with the new java 7 date class

import tap.core.*;
import tap.sample.Samples.*;  // include protoc generated classes

/**
 * Usage: .... -s 2011-01-03T10:40:00.000Z -e 2011-01-03T10:50:00.000Z -i //cta/candle/1min[AAPL]
 *
 */
public class Subscribe {
    public static void main(String[] args) throws Exception {
	CommandOptions o = new CommandOptions(args);
        Tap tap = new Tap(o);
        Pipe<Candle> candles = tap.subscribe(o.input);
        for (Candle m: candles) {
            Tapfile.Timestamp time = m.getTime();
            System.out.println(time.toString() + " symbol " + m.getSymbol() + 
                               " open " + m.getOpen() + " volume " + m.getVolume());
        }
    }
}