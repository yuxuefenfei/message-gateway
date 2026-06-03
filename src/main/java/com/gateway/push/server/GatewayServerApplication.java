package com.gateway.push.server;

import com.gateway.push.auth.TokenAuthenticator;
import com.gateway.push.config.GatewayConfig;
import com.gateway.push.session.SessionRegistry;

public final class GatewayServerApplication {
    private GatewayServerApplication() {
    }

    public static void main(String[] args) throws InterruptedException {
        GatewayConfig config = GatewayConfig.fromSystemProperties();
        GatewayServer server = new GatewayServer(config, new SessionRegistry(), TokenAuthenticator.nonBlankToken());
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "gateway-shutdown"));
        server.start().channel().closeFuture().sync();
    }
}
