# Message Gateway

基于 Netty 与 Protobuf 的独立 WebSocket 推送网关实现。

## 已实现能力

- 使用 `src/main/proto/push.proto` 定义统一顶级 `Frame`，覆盖 `PING`、`PONG`、`CONNECT`、`CONNECT_ACK`、`NOTIFY`、`BIZ_REPORT`。
- 使用 `WebSocketProtobufDecoder` 将 `BinaryWebSocketFrame` 解码为 Protobuf `Frame`。
- 使用 `WebSocketProtobufEncoder` 将 Protobuf `Frame` 编码为 WebSocket 二进制帧。
- 使用 `GatewayPushHandler` 处理连接鉴权、本地会话映射、业务心跳、业务上报和连接清理。
- 在 Netty pipeline 中加入 `IdleStateHandler`，默认 90 秒无读事件即关闭连接。
- 提供 `PushService`，可向已绑定的 `clientId` 推送 `Notification`。

## 目录结构

```text
src/main/proto
  push.proto
src/main/java/com/gateway/push
  auth        鉴权接口
  codec       WebSocket 与 Protobuf 编解码器
  config      网关配置
  handler     核心业务处理器
  server      Netty 启动与 pipeline 配置
  session     本地会话注册表与推送服务
src/test/java/com/gateway/push
  codec       编解码测试
  handler     业务处理器测试
```

## 构建与运行

```bash
mvn test
mvn package
java -jar target/message-gateway-1.0.0-SNAPSHOT.jar
```

默认监听地址为 `ws://localhost:8080/ws`。

测试覆盖编解码、连接鉴权、心跳响应、空闲断开，以及一次真实 Netty WebSocket 客户端到服务端的 Protobuf 端到端通信。

可通过系统属性调整运行参数：

```bash
java -Dgateway.port=9000 -Dgateway.websocket.path=/push -Dgateway.idle.seconds=90 \
  -jar target/message-gateway-1.0.0-SNAPSHOT.jar
```

支持的系统属性：

```text
gateway.port                      服务端口，默认 8080
gateway.websocket.path            WebSocket 路径，默认 /ws
gateway.idle.seconds              读空闲关闭时间，默认 90 秒
gateway.connect.timeout.seconds   WebSocket 握手后 CONNECT 鉴权超时，默认 10 秒
gateway.max.http.content.bytes    HTTP 聚合上限，默认 65536
gateway.max.websocket.frame.bytes WebSocket 二进制帧上限，默认 65536
gateway.write.buffer.low.bytes    写缓冲低水位，默认 32768
gateway.write.buffer.high.bytes   写缓冲高水位，默认 65536
gateway.business.executor.threads 业务处理线程数，默认 max(2, CPU 核数)
```

性能相关说明：

- `BIZ_REPORT` 通过 `BizReportSink` 扩展，业务处理器会运行在独立业务线程池，避免阻塞 Netty IO 线程。
- `PushService` 会在推送前检查 `Channel.isWritable()`，慢客户端触发背压时直接拒绝本次推送。
- 同一客户端批量推送可使用 `pushManyToClient`，会对同一连接执行多次 `write` 后一次 `flush`。
- `SessionRegistry` 会把 `clientId` 缓存在 Netty `Channel` 属性上，认证后读路径优先走本地属性，减少并发 map 查询。
- `InMemoryGatewayMetrics` 提供压测可用的轻量计数快照，生产可替换为 Micrometer/Prometheus 实现。
