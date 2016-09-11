package com.pluralsight.dpaws.acceptance;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(VertxUnitRunner.class)
public class QuoteGeneratorTest {
    VertxOptions options;
    String ipAddress;
    List<JsonObject> mch = new ArrayList<>();
    List<JsonObject> dvn = new ArrayList<>();
    List<JsonObject> bct = new ArrayList<>();

    @Before
    public void before(TestContext text) throws UnknownHostException {
        ipAddress = Inet4Address.getLocalHost().getHostAddress();
        ClusterManager mgr = new HazelcastClusterManager();
        options = new VertxOptions().setClusterManager(mgr).setClusterHost(ipAddress);
    }

    @Test
    public void testMarketData(TestContext context) throws IOException {
        Async async = context.async();
        Vertx.clusteredVertx(options, r -> {
            if (r.succeeded()) {
                Vertx vertx = r.result();
                vertx.eventBus().consumer("markets", msg -> {
                    JsonObject quote = (JsonObject) msg.body();
                    context.assertTrue(quote.getDouble("bid") > 0);
                    context.assertTrue(quote.getDouble("ask") > 0);
                    context.assertTrue(quote.getInteger("volume") > 0);
                    context.assertTrue(quote.getInteger("shares") > 0);
                    switch (quote.getString("symbol")) {
                        case "MCH":
                            mch.add(quote);
                            break;
                        case "DVN":
                            dvn.add(quote);
                            break;
                        case "BCT":
                            bct.add(quote);
                            break;
                    }
                });
            } else {
                context.fail("Unable to start Vert.x");
            }
        });
        await().atMost(30, SECONDS).until(() -> mch.size() > 2);
        await().atMost(30, SECONDS).until(() -> dvn.size() > 2);
        await().atMost(30, SECONDS).until(() -> bct.size() > 2);
        async.complete();
    }
}
