package tap.core;

import java.util.List;

import tap.util.Alerter;

import junit.framework.Assert;

public class TapUnitTestAlerter implements Alerter{
	
	@Override
	public void alert(Exception exception, String summary) {
		Assert.fail(summary);
		
	}

	@Override
	public void alert(List<PhaseError> result) {
		if (!result.isEmpty()) {
			for(PhaseError e: result) {
				System.out.println(e.getMessage());
			}
			Assert.fail(result.size() + " errors");
		}
	}

	@Override
	public void alert(String problem) {
		Assert.fail(problem);
	}

	@Override
	public void pipeCompletion(String pipeName, String summary) {
		// TODO Auto-generated method stub
		
	}

}
