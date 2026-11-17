package org.java;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Netty WebSocket gateway with shared-memory ingress and asynchronous audit ACK delivery. */
public final class GatewayServer {
    private static final int DEFAULT_PORT = 8080;
    private static final int DEFAULT_RING_QUEUE_CAPACITY = 4_096;

    public static void main(String[] args) throws InterruptedException, IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        if (port < 1 || port > 65535) throw new IllegalArgumentException("Port must be between 1 and 65535");
        new GatewayServer().start(port);
    }

    public void start(int port) throws InterruptedException, IOException {
        Path inboundRingPath = resolvePath("g12.inbound.ring", "G12_INBOUND_RING_PATH", "g12-inbound.ring");
        Path ackRingPath = resolvePath("g12.ack.ring", "G12_ACK_RING_PATH", "g12-ack.ring");
        int queueCapacity = Integer.getInteger("g12.inbound.queue-capacity", DEFAULT_RING_QUEUE_CAPACITY);
        PendingRequestRegistry pendingRequests = new PendingRequestRegistry();

        // Audit is started before Gateway in Compose and creates the ACK ring.
        // Gateway maps it as the sole SPSC consumer.
        try (MappedSpscRingWriter ringWriter = new MappedSpscRingWriter(inboundRingPath, queueCapacity);
             MappedAckRingReader ackReader = new MappedAckRingReader(ackRingPath, pendingRequests::route)) {
            if (!ackReader.isRunning()) {
                throw new IllegalStateException("ACK ring reader did not start");
            }
            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .option(ChannelOption.SO_BACKLOG, 128)
                        .childOption(ChannelOption.SO_KEEPALIVE, true)
                        .childHandler(new io.netty.channel.ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel channel) {
                                var pipeline = channel.pipeline();
                                pipeline.addLast(new HttpServerCodec());
                                pipeline.addLast(new HttpObjectAggregator(TransactionValidator.MAX_PAYLOAD_BYTES));
                                pipeline.addLast(new WebSocketServerProtocolHandler(
                                        "/ws", null, true, TransactionValidator.MAX_PAYLOAD_BYTES));
                                pipeline.addLast(new WebSocketFrameAggregator(TransactionValidator.MAX_PAYLOAD_BYTES));
                                pipeline.addLast(new WebSocketFrameHandler(
                                        new TransactionValidator(), ringWriter, pendingRequests));
                            }
                        });

                Channel serverChannel = bootstrap.bind(port).sync().channel();
                System.out.println("Netty Gateway listening at ws://localhost:" + port + "/ws");
                System.out.println("Inbound ring: " + inboundRingPath.toAbsolutePath());
                System.out.println("ACK ring: " + ackRingPath.toAbsolutePath());
                serverChannel.closeFuture().sync();
            } finally {
                bossGroup.shutdownGracefully().sync();
                workerGroup.shutdownGracefully().sync();
            }
        }
    }

    private static Path resolvePath(String property, String environment, String fallbackName) {
        String configured = System.getProperty(property);
        if (configured == null || configured.isBlank()) configured = System.getenv(environment);
        if (configured != null && !configured.isBlank()) return Path.of(configured);
        Path linuxShm = Path.of("/dev/shm");
        if (Files.isDirectory(linuxShm) && Files.isWritable(linuxShm)) return linuxShm.resolve(fallbackName);
        return Path.of("./" + fallbackName);
    }
}
