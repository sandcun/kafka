package sand.selector;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import sand.latch.LeaderLatch;
import sand.util.TestingServer;

/**
 * 回调函数
 * @author Ivan
 *
 */
public class LeaderSelector {

	private static TestingServer ts = null;
    private LeaderSelectorListener listener;
   
    /**
     * 设置listener
     * @param listener
     */
    public void setListener(LeaderSelectorListener listener){
        this.listener=listener;
    }
    
    /**
     * 设置click事件，并绑定listener
     */
    public void click() {
        listener.takeLeadership(this);
    }
    
	public boolean start() throws IOException, KeeperException, InterruptedException {
		ts = new TestingServer();
		ts.init();
		ZooKeeper zk = ts.getZk();
		//String myPath = ts.getPath();
		int mySeq = ts.getSeq();
		//获取前序节点
		String preNode = ts.getPreNode(zk,mySeq);
		//System.out.println("preNode :" + preNode);
		//最小节点没有前序节点，竞争为leader
		if(preNode == null){
			System.out.println("I am leader, my LeaderSeq = "+mySeq);
			//触发事件
			click();
			return true;
		}else{
			//其他节点均为follower节点
			System.out.println("I am follower, my LeaderSeq = "+mySeq);
			//创建watcher，watch前序节点
			System.out.println("I am watching : " + preNode);
			this.setWatcher(zk, mySeq, preNode);
		}

		return false;
	}
	
	private void setWatcher(final ZooKeeper zk,final int mySeq,String preNode) throws KeeperException, InterruptedException{
		Stat stat = zk.exists(preNode, new Watcher() {
			@Override
			public void process(WatchedEvent event){
				
				System.out.println(event.getPath() + "|" + event.getType().name());
				try{
					//前序节点挂掉，则重新选择前序节点
					if(event.getType().name().equals("NodeDeleted")){
						String preNode = ts.getPreNode(zk,mySeq);
						//无前序节点，则当前为最小节点，竞争为leader
						if(preNode == null){
							System.out.println("I am leader and seq = "+mySeq);
							//触发事件
							click();
						}else{
							//继续watch前序节点
							zk.exists(preNode, this);
							System.out.println("PreNode is down , change preNode as : " + preNode);								
						}
					}
					
				}catch(KeeperException | InterruptedException e){
					//
				}
			}
		}
		);
	}	

}
