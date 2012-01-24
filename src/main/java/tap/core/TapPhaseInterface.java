/**
 * 
 */
package tap.core;
import tap.core.TapMapper;

/**
 * @author Douglas Moore
 *
 */
public interface TapPhaseInterface {
	public Phase reads(String path);
    public Phase writes(String path);
    public Phase map(Class<TapMapper> mapper);
    public Phase reduce(Class<TapMapper> reducer);
    public Phase combine(Class<TapMapper> combiner);
    public Phase groupBy(String expression);
    public Phase sortBy(String expression);
}
