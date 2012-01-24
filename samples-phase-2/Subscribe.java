// this needs to be modified to work with the new java 7 date class

package tap.sample;

import tap.*;
import quantbench.Candle;

public class Subscribe {
    
    // Usage: ... -s 2011-01-03T10:40:00.000 -e 2011-01-03T10:50:00.000
    public static main(String[] args) throws Exception {
        CommandOptions o = new CommandOptions(args);
        Tap tap = new Tap(o);
        
        Pipe<Candle> candles = tap.subscribe("//cta/candle/1min[AAPL]");
        while (candles.more()) {
            Candle m = candles.get();
            Date time = Tap.newDate(m.getStartTime());
            System.out.println(time.toString + " symbol " + m.getSymbol() + 
                               " open " + m.getOpen() + " volume " + m.getVolume());
        }
    }
}