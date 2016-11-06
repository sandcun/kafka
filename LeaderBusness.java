package sand.selector;

import sand.selector.*;

public class LeaderBusness implements LeaderSelectorListener {
	@Override
	public void takeLeadership(LeaderSelector b) {
		System.out.println("I am leader, in callback function,do anything I want");

	}

}
