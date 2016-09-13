package com.pluralsight.dockerproductionaws.acceptance;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(VertxUnitRunner.class)
public class QuoteGeneratorTest {
    VertxOptions options;
    String ipAddress;
    JsonObject config;
    List<JsonObject> mch = new ArrayList<>();
    List<JsonObject> dvn = new ArrayList<>();
    List<JsonObject> bct = new ArrayList<>();

    @Before
    public void before(TestContext text) throws IOException {
        // Setup clustering
        ipAddress = Inet4Address.getLocalHost().getHostAddress();
        ClusterManager mgr = new HazelcastClusterManager();
        options = new VertxOptions().setClusterManager(mgr).setClusterHost(ipAddress);

        // Get config
        byte[] bytes = Files.readAllBytes(new File("src/conf/config.json").toPath());
        config = new JsonObject(new String(bytes, "UTF-8"));
    }

    @Test
    public void testQuoteGeneratorApi(TestContext context) {
        Async async = context.async();
        Vertx vertx = Vertx.vertx();
        HttpClientOptions options = new HttpClientOptions().setDefaultHost(config.getString("HTTP_HOST") + ".");
        options.setDefaultPort(config.getInteger("HTTP_PORT"));
        HttpClient client = vertx.createHttpClient(options);
        client.get("/", response -> {
            response.bodyHandler(buffer -> {
                JsonObject body = buffer.toJsonObject();
                context.assertEquals(response.statusCode(), 200);
                context.assertNotNull(body.getJsonObject("MacroHard"));
                context.assertNotNull(body.getJsonObject("Black Coat"));
                context.assertNotNull(body.getJsonObject("Divinator"));
                async.complete();
            });
        }).end();
    }

    @Test
    public void testMarketData(TestContext context) throws IOException {
        Async async = context.async();
        Vertx.clusteredVertx(options, r -> {
            if (r.succeeded()) {
                Vertx vertx = r.result();
                vertx.eventBus().consumer(config.getString("MARKET_DATA_ADDRESS"), msg -> {
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
