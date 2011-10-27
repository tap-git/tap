package tap.util;

import com.google.protobuf.Message;

public class ObjectFactory {
    public static <T> T newInstance(Class<? extends T> cls) throws Exception {
        if(Message.class.isAssignableFrom(cls)) {
            return (T) cls.getMethod("getDefaultInstance").invoke(null);
        }
        else
            return (T) cls.newInstance();
    }
}
