/**
 * 
 */
package tap.util;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;

import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.reflect.ReflectData;

import com.google.protobuf.Message;

/**
 * @author Douglas Moore
 * 
 */
public class ReflectUtils {

	/**
	 * Obtain the Parameterized type from the base class. Assumes base class is
	 * of type Example<U,V,T,X,Y,Z...>
	 * 
	 * @param aClass
	 *            The class having a parameterized base class, such as a Mapper
	 *            class
	 * @param position
	 *            The parameter position (zero based)
	 * @return The Parameter[position]'s class
	 */
	public static Class<?> getParameterClass(Class<?> aClass, int position) {
		ParameterizedType parameterizedType = (ParameterizedType) aClass
				.getGenericSuperclass();
		return (Class) parameterizedType.getActualTypeArguments()[position];
	}

	public static Schema getSchema(Object proto) {
		if (proto instanceof Message)
			return ProtobufData.get().getSchema(proto.getClass());
		try {
			Field schemaField = proto.getClass().getField("SCHEMA$");
			return (Schema) schemaField.get(null);
		} catch (NoSuchFieldException e) {
			// use reflection
			return ReflectData.get().getSchema(proto.getClass());
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
