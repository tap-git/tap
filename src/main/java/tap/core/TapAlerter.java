/**
 * 
 */
package tap.core;

import java.util.List;
import tap.PhaseError;
import tap.util.TapAlerterInterface;

public class TapAlerter implements TapAlerterInterface{
	
	private boolean success=true;
	
	public boolean checkSuccess() {
		return success;
	}
	public void reset() {
		success = true;
	}
	
	@Override
	public void alert(Exception exception, String summary) {
		success=false;
		System.out.println("alert: " + summary);
		System.out.println("alert: " + exception);
	}

	@Override
	public void alert(List<PhaseError> result) {
		success=false;
		if (!result.isEmpty()) {
			for(PhaseError e: result) {
				System.out.println(e.getMessage());
			}
		}
	}

	@Override
	public void alert(String problem) {
		success=false;
		System.out.println("alert: " + problem);
	}

	@Override
	public void pipeCompletion(String pipeName, String summary) {
		System.out.println("Completed: " + pipeName);
		System.out.println("Summary: " + summary);
	}
}
