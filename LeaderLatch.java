package sand.latch;

import java.io.EOFException;
import java.io.IOException;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;

import sand.util.TestingServer;

/**
 * ��������
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
		//��ȡǰ��ڵ�
		String preNode = ts.getPreNode(zk,mySeq);
		System.out.println("preNode :" + preNode);
		//��С�ڵ�û��ǰ��ڵ㣬����Ϊleader
		if(preNode == null){
			System.out.println("I am leader, my LeaderSeq = "+mySeq);
			LeaderState = true;
			return true;
		}else{
			LeaderState = false;
			//�����ڵ��Ϊfollower�ڵ�
			System.out.println("I am follower, my LeaderSeq = "+mySeq);
			//����watcher��watchǰ��ڵ�
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
					//ǰ��ڵ�ҵ���������ѡ��ǰ��ڵ�
					if(event.getType().name().equals("NodeDeleted")){
						String preNode = ts.getPreNode(zk,mySeq);
						//��ǰ��ڵ㣬��ǰΪ��С�ڵ㣬����Ϊleader
						if(preNode == null){
							System.out.println("I am leader and seq = "+mySeq);
							LeaderState = true;
						}else{
							//����watchǰ��ڵ�
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
