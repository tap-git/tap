package tap.core;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import tap.formats.*;

/*
 * @Author Douglas Moore
 * Test file path format detection
 */
public class PipeTests {

    public class CountRec {
        String word;
        int count;
    }

    @Test
    public void prototype() {
        Pipe pipe = new Pipe(CountRec.class);
        Assert.assertNotNull(pipe);
        Assert.assertNotNull(pipe.getPrototype());
        System.out.println("pipe prototype " + pipe.getPrototype());
    }

    @Test
    public void stringFormat() {
        Pipe<CountRec> pipe = new Pipe<CountRec>("nonexistent.txt");
        assertEquals(Formats.STRING_FORMAT, pipe.getFormat());
    }

    @Test
    public void avroFormat() {
        Pipe pipe = new Pipe("nonexistent.avro");
        assertEquals(Formats.AVRO_FORMAT, pipe.getFormat());
    }

    @Test
    public void testFormat() {
        Pipe pipe = new Pipe("nonexistent.json");
        assertEquals(Formats.JSON_FORMAT, pipe.getFormat());
    }

    @Test
    public void testUnknownFormat() {
        Pipe pipe = new Pipe("nonexistent.unknown");
        assertEquals(Formats.UNKNOWN_FORMAT, pipe.getFormat());
    }

    @Test
    public void testTextGziped() {
        Pipe<String> pipe = new Pipe("nonexistent.txt.gz");
        assertEquals(pipe.getFormat(), Formats.STRING_FORMAT);
        assertTrue(pipe.isCompressed());
    }

    @Test
    public void testEquals() {
        Pipe<String> pipe1 = new Pipe("nonexistent.txt");
        Pipe<String> pipe2 = new Pipe("nonexistent.txt");
        Pipe<Text> pipe3 = new Pipe<Text>("nonexistent.txt");
        assertTrue(pipe1.equals(pipe2));
        assertTrue(pipe1.equals(pipe3));
    }
}