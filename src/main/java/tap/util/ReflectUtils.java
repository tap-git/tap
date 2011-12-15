/**
 * 
 */
package tap.util;

import java.lang.reflect.ParameterizedType;

/**
 * @author Douglas Moore
 *
 */
public class ReflectUtils {

	/**
	 * Obtain the Parameterized type from the base class. 
	 * Assumes base class is of type Example<U,V,T,X,Y,Z...>
	 * @param aClass The class having a parameterized base class, such as a Mapper class
	 * @param position The parameter position (zero based)
	 * @return The Parameter[position]'s class
	 */
	public static Class<?> getParameterClass(Class<?> aClass, int position) {
		 ParameterizedType parameterizedType =
			        (ParameterizedType) aClass.getGenericSuperclass();
		 return (Class)parameterizedType.getActualTypeArguments()[position];
	 }
}
