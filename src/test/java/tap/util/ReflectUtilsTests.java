package tap.util;

import junit.framework.Assert;

import org.junit.Test;

public class ReflectUtilsTests {
    
    class Base<A, B> {}
    class Derv extends Base<String, Integer> {}

    @Test
    public void canGetParameterClassByPosition() {
        Class<?> first = ReflectUtils.getParameterClass(Derv.class, 0);
        Assert.assertEquals(String.class, first);
        
        Class<?> second = ReflectUtils.getParameterClass(Derv.class, 1);
        Assert.assertEquals(Integer.class, second);
    }
    
}
