package com.gateway.push.auth;

import com.gateway.push.protocol.ConnectRequest;

@FunctionalInterface
public interface TokenAuthenticator {
    boolean authenticate(ConnectRequest request);

    static TokenAuthenticator nonBlankToken() {
        return request -> {
            if (request == null) {
                return false;
            }
            return !request.getToken().isBlank();
        };
    }
}
