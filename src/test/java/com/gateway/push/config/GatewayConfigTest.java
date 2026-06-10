package com.gateway.push.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GatewayConfigTest {
    @Test
    void keepsBackwardCompatibleThreeArgumentConstructor() {
        GatewayConfig config = new GatewayConfig(8080, "/ws", Duration.ofSeconds(90));

        assertEquals(8080, config.getPort());
        assertEquals("/ws", config.getWebsocketPath());
        assertEquals(Duration.ofSeconds(90), config.getReaderIdleTimeout());
        assertEquals(Duration.ofSeconds(10), config.getConnectTimeout());
        assertEquals(64 * 1024, config.getMaxHttpContentLength());
        assertEquals(64 * 1024, config.getMaxWebSocketFrameBytes());
        assertEquals(32 * 1024, config.getWriteBufferLowWaterMark());
        assertEquals(64 * 1024, config.getWriteBufferHighWaterMark());
        assertEquals(4096, config.getBusinessExecutorMaxPendingTasks());
        assertEquals(1024, config.getAuthExecutorMaxPendingTasks());
        assertEquals(64, config.getPushBatchChunkSize());
    }

    @Test
    void rejectsInvalidLimitsAndTimeouts() {
        assertThrows(IllegalArgumentException.class,
                () -> new GatewayConfig(8080, "/ws", Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> new GatewayConfig(8080, "/ws", Duration.ofNanos(1)));
        assertThrows(IllegalArgumentException.class,
                () -> new GatewayConfig(8080, "ws", Duration.ofSeconds(90)));
        assertThrows(IllegalArgumentException.class,
                () -> new GatewayConfig(
                        8080,
                        "/ws",
                        Duration.ofSeconds(90),
                        Duration.ZERO,
                        64 * 1024,
                        64 * 1024));
        assertThrows(IllegalArgumentException.class,
                () -> new GatewayConfig(
                        8080,
                        "/ws",
                        Duration.ofSeconds(90),
                        Duration.ofSeconds(10),
                        0,
                        64 * 1024));
        assertThrows(IllegalArgumentException.class,
                () -> new GatewayConfig(
                        8080,
                        "/ws",
                        Duration.ofSeconds(90),
                        Duration.ofSeconds(10),
                        64 * 1024,
                        0));
        assertThrows(IllegalArgumentException.class,
                () -> new GatewayConfig(
                        8080,
                        "/ws",
                        Duration.ofSeconds(90),
                        Duration.ofSeconds(10),
                        64 * 1024,
                        64 * 1024,
                        8,
                        8,
                        2));
        assertThrows(IllegalArgumentException.class,
                () -> new GatewayConfig(
                        8080,
                        "/ws",
                        Duration.ofSeconds(90),
                        Duration.ofSeconds(10),
                        64 * 1024,
                        64 * 1024,
                        8,
                        16,
                0));
    }

    @Test
    void readsMillisecondTimeoutProperties() {
        System.setProperty("gateway.idle.millis", "150");
        System.setProperty("gateway.connect.timeout.millis", "250");
        try {
            GatewayConfig config = GatewayConfig.fromSystemProperties();
            assertEquals(Duration.ofMillis(150), config.getReaderIdleTimeout());
            assertEquals(Duration.ofMillis(250), config.getConnectTimeout());
        } finally {
            System.clearProperty("gateway.idle.millis");
            System.clearProperty("gateway.connect.timeout.millis");
        }
    }
}
