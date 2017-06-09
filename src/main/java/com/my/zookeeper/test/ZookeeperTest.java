package com.my.zookeeper.test;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * zookeeper demo
 * 
 * @author Administrator
 *
 */
public class ZookeeperTest {

	protected static final Logger logger = Logger.getLogger(ZookeeperTest.class);

	/** zookeeper�����ӵ�ַ */
	public static final String CONNECT_STRING = "127.0.0.1:2181";

	/** zookeeper ��ʱʱ�� */
	public static final int SESSION_TIME_OUT = 50*1000;

	/** �ӽڵ����� */
	public static final String PATH = "/test1";
	/** znodeTest ��value */
	public static final String data = "测试数据";

	
	
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug("main(String[]) - start"); //$NON-NLS-1$
		}

		ZooKeeper zooKeeper = getZookeeper();
		String znodeValue = "";

		Stat exists = zooKeeper.exists(PATH, false);
		if (exists == null) {
			createZnode(zooKeeper);
		}

		znodeValue = getZnodeValue(zooKeeper);

		stopZooKeeper(zooKeeper);

		System.out.println(znodeValue);

		if (logger.isDebugEnabled()) {
			logger.debug("main(String[]) - end"); //$NON-NLS-1$
		}
	}

	/**
	 * ��ȡzookeeper��ʵ������
	 * 
	 * @return
	 */
	public static ZooKeeper getZookeeper() {
		if (logger.isDebugEnabled()) {
			logger.debug("getZookeeper() - start"); //$NON-NLS-1$
		}

		Watcher watcher = null;
		ZooKeeper zooKeeper = null;
		try {
			zooKeeper = new ZooKeeper(CONNECT_STRING, SESSION_TIME_OUT, watcher);
		} catch (IOException e) {
			logger.error("getZookeeper()", e); //$NON-NLS-1$

			e.printStackTrace();
		}

		if (logger.isDebugEnabled()) {
			logger.debug("getZookeeper() - end"); //$NON-NLS-1$
		}
		return zooKeeper;
	}

	/** �ر�zookeeper ���ӵĴ����� */
	public static void stopZooKeeper(ZooKeeper zooKeeper) {
		if (logger.isDebugEnabled()) {
			logger.debug("stopZooKeeper(ZooKeeper) - start"); //$NON-NLS-1$
		}

		if (zooKeeper != null) {
			try {
				zooKeeper.close();
			} catch (InterruptedException e) {
				logger.error("stopZooKeeper(ZooKeeper)", e); //$NON-NLS-1$

				e.printStackTrace();
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("stopZooKeeper(ZooKeeper) - end"); //$NON-NLS-1$
		}
	}

	/** zookeeper�����ڵ�Ĵ����� */
	public static void createZnode(ZooKeeper zooKeeper) {
		if (logger.isDebugEnabled()) {
			logger.debug("createZnode(ZooKeeper) - start"); //$NON-NLS-1$
		}

		if (zooKeeper != null) {
			try {
				zooKeeper.create(PATH, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				logger.error("createZnode(ZooKeeper)", e); //$NON-NLS-1$

				e.printStackTrace();
			} catch (InterruptedException e) {
				logger.error("createZnode(ZooKeeper)", e); //$NON-NLS-1$

				e.printStackTrace();
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("createZnode(ZooKeeper) - end"); //$NON-NLS-1$
		}
	}

	/**
	 * ��ȡ�ڵ�value�Ĵ�����
	 * 
	 * @param zooKeeper
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static String getZnodeValue(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug("getZnodeValue(ZooKeeper) - start"); //$NON-NLS-1$
		}

		byte[] data2 = null;
		if (zooKeeper != null) {
			data2 = zooKeeper.getData(PATH, false, new Stat());
		}
		String returnString = new String(data2);
		if (logger.isDebugEnabled()) {
			logger.debug("getZnodeValue(ZooKeeper) - end"); //$NON-NLS-1$
		}
		return returnString;
	}
}
