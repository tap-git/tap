package tap.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.*;

import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.Test;

import tap.util.RecordUtils;



public class RecordUtilsTests {

    private static class SubBase {
        protected int inner;
    }
    private static class Base extends SubBase {
        public transient String x;
        private Long y;
        static int z;
    }
    static class Super extends Base {
        public int ignore;
    }
    
    static class Nested {
        CharSequence name;
        Integer value;
    }
    
    static class Gend {
        Utf8 charseq;
        List<Nested> collection;
        Nested[] array;
        Map<String,Nested> map;
    }
    
    Base a, b;
    Super c;

    @Before
    public void setup() {
        a = new Base();
        b = new Base();
        c = new Super();
        a.x = "a";
        b.x = "b";
        c.x = "c";
        a.y = 11L;
        c.ignore = 12;
    }
    
    @Test
    public void copySameClass() {
        RecordUtils.copy(a, b);
        assertEquals("a", b.x);
        assertEquals(new Long(11), b.y);
    }
    
    
    @Test
    public void copySuperClass() {
        RecordUtils.copy(c, a);
        assertEquals("c", a.x);
        assertNull(a.y);
    }
    
    @Test
    public void copySubClass() {
        RecordUtils.copy(a, c);
        assertEquals("a", c.x);
        assertEquals(new Long(11), ((Base)c).y);
    }

    @Test
    public void deepCopyMutateUtf8() {
        Nested n = new Nested();
        
        Utf8 utf8 = new Utf8("Sample");
        n.name = utf8;
        n.value = 12;
        
        Nested copy = new Nested();
        RecordUtils.copy(n, copy);
        modify(n.name);
        n.value = null;
        
        // but our copies should NOT be affected
        assertEquals("Sample", copy.name.toString());
        assertEquals(12, (int)copy.value);
    }
    
    static void modify(CharSequence cs) {
        Utf8 utf8 = (Utf8)cs;
        // avro reads like this for efficiency
        utf8.getBytes()[0] = 'N';
        utf8.getBytes()[1] = 'o';
        utf8.setByteLength(2);
        assertEquals("No", cs.toString());
    }
    
    @Test
    public void deepCopyMutateList() {
        Gend in = new Gend();
        in.collection = new ArrayList<Nested>();
        Nested n1 = new Nested();
        n1.name = new Utf8("one");
        n1.value = 11;
        in.collection.add(n1);
        
        Nested n2 = new Nested();
        n2.name = new Utf8("two");
        in.collection.add(n2);
        
        Gend out = new Gend();
        RecordUtils.copy(in, out);
        n1.name = null;
        modify(n2.name);
        in.collection.clear();
        assertEquals(2, out.collection.size());
        assertEquals("one", out.collection.get(0).name.toString());
        assertEquals(11, (int)out.collection.get(0).value);
        assertNull(out.collection.get(1).value);
    }
    
    @Test
    public void deepCopyMutateArray() {
        
    }
    
    
    @Test
    public void deepCopyMutateMap() {
        
    }
}
