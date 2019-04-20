package org.apache.rocketmq.store;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StoreTest {
    private int QUEUE_TOTAL = 2;
    private AtomicInteger QueueId = new AtomicInteger(0);

    private SocketAddress BornHost;
    private SocketAddress StoreHost;

    @Test
    public void testReadAndWrite() throws Exception {
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);

        DefaultMessageStore messageStore = buildMessageStore();

        boolean load = messageStore.load();

        assertTrue(load);

        messageStore.start();

        // do some testing
        MessageExtBrokerInner msg = buildMessage();
        PutMessageResult putResult = messageStore.putMessage(msg);
        assertNotNull(putResult);

        GetMessageResult result = messageStore.getMessage("GROUP_A",
            "FooBar",
            0,
            0,
            1024 * 1024,
            null);

        assertNotNull(result);
        // end testing

        messageStore.shutdown();
        messageStore.destroy();

        MessageStoreConfig messageStoreConfig = messageStore.getMessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    private DefaultMessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);

        messageStoreConfig.setStorePathRootDir("e:/test/store");
        messageStoreConfig.setStorePathCommitLog("e:/test/store/commitlog");

        BrokerStatsManager statManager = new BrokerStatsManager("simpleTest");

        MessageArrivingListener listener = new TestListener();

        BrokerConfig brokerConfig = new BrokerConfig();

        DefaultMessageStore store = new DefaultMessageStore(messageStoreConfig,
                statManager,
                listener,
                brokerConfig);

        return store;
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody("Hello world".getBytes());
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("KEYS", "boo-bar");

        msg.setPropertiesString(MessageDecoder.messageProperties2String(properties));
        return msg;
    }

    class TestListener implements MessageArrivingListener {
        public void arriving(String topic, int queueId, long logicOffset,
            long tagsCode, long msgStoreTime, byte[] filterBitMap,
            Map<String, String> properties) {

        }
    }
}
