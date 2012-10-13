package tap;

import java.util.List;

import tap.core.TapAlerter;

import junit.framework.Assert;

public class TapUnitTestAlerter extends TapAlerter{
	
	@Override
	public void alert(Exception exception, String summary) {
		super.alert(exception, summary);
		Assert.fail(summary);
	}

	@Override
	public void alert(List<PhaseError> result) {
		super.alert(result);
		Assert.fail(result.get(0).getMessage());
	}

	@Override
	public void alert(String problem) {
		super.alert(problem);
		Assert.fail(problem);
	}
}
