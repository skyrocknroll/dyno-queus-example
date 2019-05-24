package in.hack4geek.dynoqueues.demo;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.redis.v2.QueueBuilder;
import com.netflix.dyno.queues.shard.DynoShardSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DynoQProducer {
    private static final Logger logger = LoggerFactory.getLogger(DynoQProducer.class);
    public static void main(String[] args) {
        final Host node1 = new Host("10.7.0.10", "10.7.0.10", 8102, "rack", "dc", Host.Status.Up,"{}");
        final Host node2 = new Host("10.7.0.11", "10.7.0.11", 8102, "rack", "dc", Host.Status.Up,"{}");
        final Host node3 = new Host("10.7.0.12", "10.7.0.12", 8102, "rack", "dc", Host.Status.Up,"{}");

        final HostToken node1Token = new HostToken(10L, node1);
        final HostToken node2Token = new HostToken(11L, node2);
        final HostToken node3Token = new HostToken(12L, node3);
        HostSupplier hostSupplier = () -> {
            ArrayList<Host> hosts = new ArrayList<>();
            hosts.add(node1);
            hosts.add(node2);
            hosts.add(node3);
            return hosts;
        };
        final TokenMapSupplier tokenSupplier = new TokenMapSupplier() {


            @Override
            public List<HostToken> getTokens(Set<Host> activeHosts) {
                ArrayList<HostToken> hostTokens = new ArrayList<>();
                hostTokens.add(node1Token);
                hostTokens.add(node2Token);
                hostTokens.add(node3Token);

                return hostTokens;
            }

            @Override
            public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
                return null;
            }

//            @Override
//            public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
//                return null;
//            }
        };

        DynoJedisClient dynoJedisClient = new DynoJedisClient.Builder().withApplicationName("sdsds")
                .withHostSupplier(hostSupplier)
                .withTokenMapSupplier(tokenSupplier)
//                .withDynomiteClusterName("dyn_o_mite")

                // .withCPConfig(
                // new ConnectionPoolConfigurationImpl("demo")
                // .setCompressionStrategy(ConnectionPoolConfiguration.CompressionStrategy.THRESHOLD)
                // .setCompressionThreshold(2048)
                // .setLocalRack(this.localRack)
                // )
                .build();
//        final DynoQueueDemo demo = new DynoQueueDemo("test", "rack");

        System.out.println(dynoJedisClient.getClusterName());
        dynoJedisClient.set("hey","hi");

        System.out.println("----->" + dynoJedisClient.get("hey"));
        DynoShardSupplier ss = new DynoShardSupplier(dynoJedisClient.getConnPool().getConfiguration().getHostSupplier(), "dc", "rack");

        DynoQueue queue = new QueueBuilder()
                .setQueueName("foo")
                .setCurrentShard(ss.getCurrentShard())
                .setRedisKeyPrefix("dynoQueue_")
                .useDynomite(dynoJedisClient, dynoJedisClient, dynoJedisClient.getConnPool().getConfiguration().getHostSupplier())
                .setUnackTime(50_000)
                .build();
        Message msg = new Message("id1", "message payload");
        msg.setTimeout(2000L);

        queue.push(Arrays.asList(msg));

        int count = 10;
        List<Message> polled = queue.pop(count, 5, TimeUnit.SECONDS);
        logger.info("******======>" + polled.toString());

        queue.ack("id1");
        try {
            queue.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
