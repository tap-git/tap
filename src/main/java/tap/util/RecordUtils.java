package tap.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

public class RecordUtils {

    public static <T> void copy(T from, T to) {
        Class<?> toClass = to.getClass();
        Class<?> fromClass = from.getClass();
        Class<?> level;
        if (toClass.isAssignableFrom(fromClass)) {
            level = toClass;
        } else if (fromClass.isAssignableFrom(fromClass)) {
            level = fromClass;
        } else {
            throw new IllegalArgumentException("Not yet supported: copying fields from classes without a common superclass");
        }
        try {
            while (level != null && level != Object.class){
                // replace this with cached codegen        
                for (Field f : level.getDeclaredFields()) {
                    f.setAccessible(true);
                    if (!Modifier.isStatic(f.getModifiers())) {                        
                    	Object o = deepCopy(f.get(from));
                	    f.set(to, o);
                    }
                }
                level = level.getSuperclass();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object deepCopy(Object o) throws InstantiationException, IllegalAccessException {
        if(o == null)
            return null;
        
        if (o instanceof Utf8){
            Utf8 old = (Utf8)o;
            int len = old.getByteLength();
            byte[] copy = new byte[len];
            System.arraycopy(old.getBytes(), 0, copy, 0, len);
            return new Utf8(copy);
        } else if (o instanceof Collection) {
            Collection copy = (Collection) o.getClass().newInstance();
            for (Object item : (Collection)o) {
                copy.add(deepCopy(item));
            }
            return copy;
        } else if (o instanceof Map) {
            Map copy = (Map)o.getClass().newInstance();
            Map map = (Map)o;
            for (Map.Entry entry : (Set<Map.Entry>)map.entrySet()) {
                copy.put(deepCopy(entry.getKey()), deepCopy(entry.getValue()));
            }
            return copy;
        } else if (o instanceof Integer || o instanceof String || o instanceof Long || o instanceof Double || o instanceof Float) {
            return o;
        } else {
            Object copy = o.getClass().newInstance();
            RecordUtils.copy(o, copy);
            return copy;
        }

    }

    public static<T> T jsonToAvro(String json, Schema inSchema) {
        return null;
    }

    public static <T> String toJson(T value) {
        // TODO Auto-generated method stub
        return null;
    }
}
