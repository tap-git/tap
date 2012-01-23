
// this needs to be modified to work with the new java 7 date class

package tap.sample;

import tap.*;
import quantbench.Candle;

public class Subscribe {
    public static main(String[] args) throws Exception {
        Tap tap = new Tap();
        tap.startTime("2011-01-03 10:40:00.000");
        tap.endTime("2011-01-03 10:50:00.000");
        Pipe<Candle> candles = tap.subscribe("//cta/candle/1min[AAPL]");
        while (candles.more()) {
            Candle m = candles.get();
            Date time = Tap.newDate(m.getStartTime());
            System.out.println(time.toString + " symbol " + m.getSymbol() + 
                               " open " + m.getOpen() + " volume " + m.getVolume());
        }
    }
}