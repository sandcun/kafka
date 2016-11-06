package sand.selector;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;


public class CallBackTest {
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
    	
    	LeaderSelector ls=new LeaderSelector();
        ls.setListener(new LeaderBusness());
        
        System.out.println("开始竞争Leader");
		ls.start();
        Thread.sleep(100000);
        

    }
}
