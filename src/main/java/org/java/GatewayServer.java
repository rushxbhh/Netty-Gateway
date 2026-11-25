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
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;

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
        Path journalRingPath = resolvePath("g12.journal.ring", "G12_JOURNAL_RING_PATH", "g12-journal.ring");
        String jdbcUrl = System.getenv("G12_DB_URL");
        if (jdbcUrl == null || jdbcUrl.isBlank()) jdbcUrl = System.getProperty("g12.db.url", "jdbc:postgresql://postgres:5432/g12audit");
        String dbUser = System.getenv("G12_DB_USER");
        if (dbUser == null || dbUser.isBlank()) dbUser = System.getProperty("g12.db.user", "g12");
        String dbPassword = System.getenv("G12_DB_PASSWORD");
        if (dbPassword == null || dbPassword.isBlank()) dbPassword = System.getProperty("g12.db.password", "g12_dev_password");

        int queueCapacity = Integer.getInteger("g12.inbound.queue-capacity", DEFAULT_RING_QUEUE_CAPACITY);
        PendingRequestRegistry pendingRequests = new PendingRequestRegistry();

        // Final references for handler closures
        final String fJdbcUrl = jdbcUrl;
        final String fDbUser = dbUser;
        final String fDbPassword = dbPassword;

        // Audit is started before Gateway in Compose and creates the ACK ring.
        // Gateway maps it as the sole SPSC consumer.
        // DashboardHttpHandler is isolated onto a separate thread pool to ensure ZERO jitter on hot-path worker loops.
        try (MappedSpscRingWriter ringWriter = new MappedSpscRingWriter(inboundRingPath, queueCapacity);
             MappedAckRingReader ackReader = new MappedAckRingReader(ackRingPath, pendingRequests::route);
             DashboardHttpHandler dashboardHandler = new DashboardHttpHandler(
                     inboundRingPath, journalRingPath, ackRingPath,
                     fJdbcUrl, fDbUser, fDbPassword, pendingRequests)) {
            if (!ackReader.isRunning()) {
                throw new IllegalStateException("ACK ring reader did not start");
            }
            EventLoopGroup bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("gateway-boss", true));
            EventLoopGroup workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("gateway-worker", true));
            EventExecutorGroup dashboardGroup = new DefaultEventExecutorGroup(2, new DefaultThreadFactory("dashboard-io", true));
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
                                pipeline.addLast(new HttpObjectAggregator(65536));
                                // Dashboard HTTP requests are completely isolated to dashboardGroup
                                pipeline.addLast(dashboardGroup, "dashboardHandler", dashboardHandler);
                                pipeline.addLast(new WebSocketServerProtocolHandler(
                                        "/ws", null, true, TransactionValidator.MAX_PAYLOAD_BYTES));
                                pipeline.addLast(new WebSocketFrameAggregator(TransactionValidator.MAX_PAYLOAD_BYTES));
                                pipeline.addLast(new WebSocketFrameHandler(
                                        new TransactionValidator(), ringWriter, pendingRequests));
                            }
                        });

                Channel serverChannel = bootstrap.bind(port).sync().channel();
                System.out.println("Netty Gateway listening at ws://localhost:" + port + "/ws");
                System.out.println("Dashboard interface: http://localhost:" + port + "/");
                System.out.println("Inbound ring: " + inboundRingPath.toAbsolutePath());
                System.out.println("ACK ring: " + ackRingPath.toAbsolutePath());
                System.out.println("Journal ring: " + journalRingPath.toAbsolutePath());
                serverChannel.closeFuture().sync();
            } finally {
                bossGroup.shutdownGracefully().sync();
                workerGroup.shutdownGracefully().sync();
                dashboardGroup.shutdownGracefully().sync();
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
