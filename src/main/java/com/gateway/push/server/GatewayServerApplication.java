package com.gateway.push.server;

import com.gateway.push.auth.TokenAuthenticator;
import com.gateway.push.config.GatewayConfig;
import com.gateway.push.session.SessionRegistry;

/**
 * 网关进程入口。
 *
 * <p>通过系统属性读取配置，创建默认的内存会话表和非空 token 鉴权器。
 * 该入口适合本地启动、压测和最小部署；生产环境可在此处替换真实鉴权器、
 * BizReportSink 与 GatewayMetrics 实现。</p>
 */
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
