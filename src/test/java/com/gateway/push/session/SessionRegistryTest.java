package com.gateway.push.session;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

class SessionRegistryTest {
    @Test
    void removesPreviousClientIdWhenSameChannelIsRebound() {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel();

        registry.bind("client-a", channel);
        registry.bind("client-b", channel);

        assertFalse(registry.findChannel("client-a").isPresent());
        assertSame(channel, registry.findChannel("client-b").orElseThrow());
        assertEquals("client-b", registry.findClientId(channel).orElseThrow());
        assertEquals(1, registry.size());

        channel.finishAndReleaseAll();
    }

    @Test
    void closesPreviousChannelWhenClientReconnects() {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel oldChannel = new EmbeddedChannel();
        EmbeddedChannel newChannel = new EmbeddedChannel();

        registry.bind("client-a", oldChannel);
        registry.bind("client-a", newChannel);

        assertFalse(oldChannel.isActive());
        assertFalse(registry.findClientId(oldChannel).isPresent());
        assertSame(newChannel, registry.findChannel("client-a").orElseThrow());
        assertEquals(1, registry.size());

        oldChannel.finishAndReleaseAll();
        newChannel.finishAndReleaseAll();
    }

    @Test
    void closesAllChannelsAndClearsMappings() {
        SessionRegistry registry = new SessionRegistry();
        EmbeddedChannel first = new EmbeddedChannel();
        EmbeddedChannel second = new EmbeddedChannel();
        registry.bind("client-a", first);
        registry.bind("client-b", second);

        registry.closeAll();

        assertFalse(first.isActive());
        assertFalse(second.isActive());
        assertFalse(registry.findClientId(first).isPresent());
        assertFalse(registry.findClientId(second).isPresent());
        assertFalse(registry.findChannel("client-a").isPresent());
        assertFalse(registry.findChannel("client-b").isPresent());
        assertEquals(0, registry.size());

        first.finishAndReleaseAll();
        second.finishAndReleaseAll();
    }
}
