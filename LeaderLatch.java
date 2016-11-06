package sand.latch;

import java.io.EOFException;
import java.io.IOException;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;

import sand.util.TestingServer;

/**
 * 阻塞调用
 * @author Ivan
 *
 */
public class LeaderLatch {
	private static TestingServer ts = null;
	private boolean LeaderState = false;
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		//ZooKeeper zk = TestingServer.init();
		ts = new TestingServer();
		ts.init();
		LeaderLatch latch = new LeaderLatch();
		latch.start();
		Thread.sleep(5000);
		System.out.println("In main");

	}
	

	public boolean start() throws IOException, KeeperException, InterruptedException {
		boolean state = false;
		ZooKeeper zk = ts.getZk();
		//String myPath = ts.getPath();
		int mySeq = ts.getSeq();
		//获取前序节点
		String preNode = ts.getPreNode(zk,mySeq);
		System.out.println("preNode :" + preNode);
		//最小节点没有前序节点，竞争为leader
		if(preNode == null){
			System.out.println("I am leader, my LeaderSeq = "+mySeq);
			LeaderState = true;
			return true;
		}else{
			LeaderState = false;
			//其他节点均为follower节点
			System.out.println("I am follower, my LeaderSeq = "+mySeq);
			//创建watcher，watch前序节点
			System.out.println("I am watching : " + preNode);
			this.setWatcher(zk, mySeq, preNode);
			await();
		}

		return false;
	}
	
    public void await() throws InterruptedException, EOFException
    {
        synchronized(this)
        {
            while (!LeaderState){
            	System.out.println("I am waiting...");
                wait();
            }
        }
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
							LeaderState = true;
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
