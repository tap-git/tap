package tap.sample;

import tap.CommandOptions;
import tap.Pipe;
import tap.Tap;
import tap.formats.tapproto.Testmsg;

public class Subscribe {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String a[] = {};
		CommandOptions o = new CommandOptions(a);
		Tap tap = new Tap(o);
		Pipe<Testmsg.TestRecord> pipe = tap.subscribe("share/test_data.tapproto");
		for(Testmsg.TestRecord  r : pipe)
		{
			System.out.println(r.getExtra());
		}

	}

}
