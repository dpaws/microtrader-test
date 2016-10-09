package com.pluralsight.dockerproductionaws.acceptance;

import com.pluralsight.dockerproductionaws.portfolio.Portfolio;
import com.pluralsight.dockerproductionaws.portfolio.PortfolioService;
import com.pluralsight.dockerproductionaws.trader.TraderUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.types.EventBusService;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by jmenga on 9/10/16.
 */
@RunWith(VertxUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PortfolioServiceTest {
    Config config;
    VertxOptions options;
    String ipAddress;
    private Vertx vertx;
    private EventBus eventBus;
    private PortfolioService svc;
    private Portfolio portfolio;
    private JsonObject getQuote() {
        return new JsonObject()
                .put("exchange", "vert.x stock exchange")
                .put("symbol", "MCH")
                .put("name", "MacroHard")
                .put("bid", 3328.0)
                .put("ask", 3329.0)
                .put("volume", 3)
                .put("open", 600.0)
                .put("shares", 3);

    }

    @Before
    public void before(TestContext context) throws UnknownHostException {
        Async async = context.async();

        // Setup clustering
        config = ConfigFactory.load();
        ipAddress = Inet4Address.getLocalHost().getHostAddress();
        ClusterManager mgr = new HazelcastClusterManager();
        options = new VertxOptions().setClusterManager(mgr).setClusterHost(ipAddress);
        Vertx.clusteredVertx(options, ar -> {
            vertx = ar.result();
            eventBus = vertx.eventBus();
            ServiceDiscovery discovery = ServiceDiscovery
                    .create(vertx, new ServiceDiscoveryOptions()
                    .setBackendConfiguration(vertx.getOrCreateContext().config()));
            EventBusService.getProxy(discovery, PortfolioService.class, result -> {
                svc = result.result();
                svc.getPortfolio(p -> {
                    portfolio = p.result();
                    async.complete();
                });
            });
        });
    }

    @Test
    public void testPortfolioService(TestContext context) {
        assertThat(portfolio.getCash()).isGreaterThan(0);
    }

    @Test
    public void testPortfolioEvent(TestContext context) {
        Async async = context.async();
        MessageConsumer<JsonObject> portfolioConsumer = eventBus.consumer(config.getString("portfolio.address"));
        portfolioConsumer.handler(message -> {
            JsonObject trade = message.body();
            assertThat(trade.getString("action")).isEqualTo("BUY");
            assertThat(trade.getInteger("amount")).isEqualTo(3);
            async.complete();
        });
        svc.buy(3, getQuote(), result -> {
            assertThat(result.succeeded()).isTrue();
            assertThat(portfolio.getCash()).isEqualTo(13);
        });
    }
}
