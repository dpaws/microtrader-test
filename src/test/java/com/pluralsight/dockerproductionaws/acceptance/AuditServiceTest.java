package com.pluralsight.dockerproductionaws.acceptance;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.types.HttpEndpoint;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.Inet4Address;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by jmenga on 12/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class AuditServiceTest {
    private Vertx vertx;
    private EventBus eventBus;
    private Config config;
    private String ipAddress;
    private HttpClient client;

    private JsonObject stockQuote() {
        return new JsonObject()
                .put("exchange", "vert.x stock exchange")
                .put("symbol", "MCH")
                .put("name", "MacroHard")
                .put("bid", 3389.0)
                .put("ask", 3391.0)
                .put("volume", 90000)
                .put("open", 1000)
                .put("shares", 88000);
    }

    private JsonObject stockTrade(String action, int amount, int newAmount) {
        return new JsonObject()
                .put("action", action)
                .put("quote",stockQuote())
                .put("date", System.currentTimeMillis())
                .put("amount", amount)
                .put("owned", newAmount);
    }

    @Before
    public void testSetup(TestContext context) throws UnknownHostException {
        Async async = context.async();
        // Setup clustering
        config = ConfigFactory.load();
        ipAddress = Inet4Address.getLocalHost().getHostAddress();
        ClusterManager mgr = new HazelcastClusterManager();
        VertxOptions options = new VertxOptions().setClusterManager(mgr).setClusterHost(ipAddress);
        Vertx.clusteredVertx(options, ar -> {
            vertx = ar.result();
            eventBus = vertx.eventBus();
            ServiceDiscovery discovery = ServiceDiscovery
                    .create(vertx, new ServiceDiscoveryOptions()
                    .setBackendConfiguration(vertx.getOrCreateContext().config()));
            HttpEndpoint.getClient(discovery, new JsonObject().put("name", "audit"), client -> {
                this.client = client.result();
                assertThat(client.succeeded()).isTrue();
                async.complete();
            });
        });
    }

    @Test
    public void testStockTradesAudited(TestContext context) {
        Async async = context.async();
        String portfolioAddress = config.getString("portfolio.address");
        vertx.eventBus().send(portfolioAddress, stockTrade("BUY", 3, 3));
        vertx.eventBus().send(portfolioAddress, stockTrade("SELL", 2, 1));
        vertx.eventBus().send(portfolioAddress, stockTrade("SELL", 1, 0));

        // After 5 seconds, check the audit events have been persisted and presented via audit web service
        vertx.setTimer(5000, id -> {
            client.get("/", response -> {
                context.assertEquals(response.statusCode(), 200);
                response.bodyHandler(buffer -> {
                    JsonArray body = buffer.toJsonArray();
                    context.assertEquals(body.size(), 3);
                    async.complete();
                });
            }).end();
        });
    }
}
