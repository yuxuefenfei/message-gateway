package com.gateway.push.auth;

import com.gateway.push.protocol.ConnectRequest;

/**
 * CONNECT 鉴权接口。
 *
 * <p>当前默认实现只校验 token 非空，便于独立运行和测试。
 * 生产环境通常会在这里接入 JWT 校验、鉴权中心 RPC 或设备密钥校验。</p>
 */
@FunctionalInterface
public interface TokenAuthenticator {
    /**
     * 返回 true 表示连接允许建立业务会话。
     */
    boolean authenticate(ConnectRequest request);

    /**
     * 示例鉴权器：token 非空即通过。
     */
    static TokenAuthenticator nonBlankToken() {
        return request -> {
            if (request == null) {
                return false;
            }
            return !request.getToken().isBlank();
        };
    }
}
