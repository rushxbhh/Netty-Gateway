
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

/**
 * Starts the Netty WebSocket ingress gateway.
 *
 * Endpoint: ws://localhost:8080/ws
 */
public final class GatewayServer {

    // Default listening port; can be overridden with a command-line argument.
    private static final int DEFAULT_PORT = 8080;

    public static void main(String[] args) throws InterruptedException {

        // Use a provided port when launching the application; otherwise use 8080.
        int port = args.length > 0
                ? Integer.parseInt(args[0])
                : DEFAULT_PORT;

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "Port must be between 1 and 65535"
            );
        }

        new GatewayServer().start(port);
    }

    public void start(int port) throws InterruptedException {

        /*
         * The boss group accepts incoming TCP connections.
         * A single thread is sufficient for this initial gateway.
         */
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);

        /*
         * The worker group handles I/O for accepted connections.
         * Netty uses event loops instead of creating a thread per request.
         */
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap
                    // Associate the acceptor and worker event-loop groups.
                    .group(bossGroup, workerGroup)

                    // Use Java NIO for TCP networking.
                    .channel(NioServerSocketChannel.class)

                    // Allow a reasonable number of pending connections.
                    .option(ChannelOption.SO_BACKLOG, 128)

                    // Enable TCP keep-alive for accepted connections.
                    .childOption(ChannelOption.SO_KEEPALIVE, true)

                    /*
                     * Configure the pipeline independently for each new client.
                     */
                    .childHandler(new io.netty.channel.ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {

                            var pipeline = channel.pipeline();

                            /*
                             * Converts raw HTTP bytes into HTTP request/response
                             * objects and encodes outbound HTTP messages.
                             */
                            pipeline.addLast(new HttpServerCodec());

                            /*
                             * Aggregates HTTP fragments so the WebSocket handshake
                             * can be processed as a complete HTTP request.
                             */
                            pipeline.addLast(
                                    new HttpObjectAggregator(
                                            TransactionValidator.MAX_PAYLOAD_BYTES
                                    )
                            );

                            /*
                             * Handles the HTTP-to-WebSocket upgrade at /ws.
                             * The size limit prevents oversized WebSocket frames.
                             */
                            pipeline.addLast(
                                    new WebSocketServerProtocolHandler(
                                            "/ws",
                                            null,
                                            true,
                                            TransactionValidator.MAX_PAYLOAD_BYTES
                                    )
                            );

                            /*
                             * Combines fragmented WebSocket data messages before
                             * they reach our transaction handler.
                             */
                            pipeline.addLast(
                                    new WebSocketFrameAggregator(
                                            TransactionValidator.MAX_PAYLOAD_BYTES
                                    )
                            );

                            // Final application stage: read, validate, and respond.
                            pipeline.addLast(
                                    new WebSocketFrameHandler(
                                            new TransactionValidator()
                                    )
                            );
                        }
                    });

            // Start accepting TCP connections on the configured port.
            Channel serverChannel = bootstrap.bind(port).sync().channel();

            System.out.println(
                    "Netty ingress gateway listening at ws://localhost:"
                            + port
                            + "/ws"
            );

            // Keep the application alive until the server channel is closed.
            serverChannel.closeFuture().sync();

        } finally {
            /*
             * Gracefully stop both event-loop groups when the server exits,
             * so Netty threads do not remain running.
             */
            bossGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
        }
    }
}

