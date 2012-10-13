package tap.core;


import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Date;
import java.util.StringTokenizer;

import org.joda.time.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tap.CommandOptions;
import tap.Phase;
import tap.Pipe;
import tap.Tap;
import tap.TapMapper;
import tap.TapReducer;
import tap.formats.tapproto.Testmsg;

public class SubscribeTests {

	@Test
	
	public void Test1()
	{
		String a[] = {};
		CommandOptions o = new CommandOptions(a);
		Tap tap = new Tap(o);
		Pipe<Testmsg.TestRecord> pipe = tap.subscribe("share/test_data.tapproto");
		for(Testmsg.TestRecord  r : pipe)
		{
			//System.out.println(r.getExtra());
		}
	}
}
