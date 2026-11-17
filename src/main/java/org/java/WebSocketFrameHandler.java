package org.java;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Validates WebSocket frames, publishes inbound records, and registers their final ACK route. */
public final class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
    private static final Logger LOGGER = Logger.getLogger(WebSocketFrameHandler.class.getName());
    private static final AtomicLong NEXT_CONNECTION_ID = new AtomicLong(newConnectionSeed());
    private static final AttributeKey<Long> CONNECTION_ID = AttributeKey.valueOf("g12.connectionId");

    private final TransactionValidator validator;
    private final MappedSpscRingWriter ringWriter;
    private final PendingRequestRegistry pendingRequests;

    public WebSocketFrameHandler(TransactionValidator validator,
                                 MappedSpscRingWriter ringWriter,
                                 PendingRequestRegistry pendingRequests) {
        this.validator = Objects.requireNonNull(validator, "validator");
        this.ringWriter = Objects.requireNonNull(ringWriter, "ringWriter");
        this.pendingRequests = Objects.requireNonNull(pendingRequests, "pendingRequests");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        long connectionId = NEXT_CONNECTION_ID.getAndIncrement();
        if (connectionId <= 0) {
            ctx.close();
            throw new IllegalStateException("Connection ID space exhausted");
        }
        ctx.channel().attr(CONNECTION_ID).set(connectionId);
        ctx.fireChannelActive();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
        if (frame instanceof BinaryWebSocketFrame binaryFrame) {
            handleBinaryFrame(ctx, binaryFrame.content());
            return;
        }

        GatewayResponse response = frame instanceof TextWebSocketFrame
                ? GatewayResponse.reject("BINARY_FRAME_REQUIRED", "Send the transaction as a binary WebSocket frame")
                : GatewayResponse.reject("UNSUPPORTED_FRAME", "This WebSocket frame type is not supported");
        ctx.writeAndFlush(new TextWebSocketFrame(response.toWireText()));
    }

    private void handleBinaryFrame(ChannelHandlerContext ctx, ByteBuf content) {
        int readerIndex = content.readerIndex();
        int readableBytes = content.readableBytes();
        ByteBuffer payloadView = content.nioBuffer(readerIndex, readableBytes);
        GatewayResponse validation = validator.validate(payloadView);
        if (!validation.isAccepted()) {
            ctx.writeAndFlush(new TextWebSocketFrame(validation.toWireText()));
            return;
        }

        // Netty releases inbound ByteBuf after this method; the ring writer is asynchronous.
        byte[] payloadCopy = new byte[readableBytes];
        content.getBytes(readerIndex, payloadCopy);

        Long connectionId = ctx.channel().attr(CONNECTION_ID).get();
        if (connectionId == null) {
            throw new IllegalStateException("WebSocket connection has no connectionId");
        }

        PendingRequestRegistry.PendingRequest pending = pendingRequests.register(
                connectionId, validation.getTransactionId(), ctx.channel());
        if (pending == null) {
            if (pendingRequests.isAtCapacity()) {
                writeReject(ctx, "GATEWAY_BACKPRESSURE",
                        "Too many transactions are awaiting audit acknowledgement; retry later");
            } else {
                writeReject(ctx, "DUPLICATE_IN_FLIGHT",
                        "A transaction with this ID is already pending on this connection");
            }
            return;
        }

        ringWriter.submit(payloadCopy, connectionId, validation.getTransactionId())
                .whenComplete((result, failure) -> ctx.executor().execute(() -> {
                    if (!ctx.channel().isActive()) {
                        pendingRequests.removeConnection(connectionId);
                        return;
                    }
                    if (failure != null) {
                        Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                                ? failure.getCause() : failure;
                        LOGGER.log(Level.WARNING, "Unable to publish inbound transaction", cause);
                        pending.rejectIngress("INGRESS_WRITE_FAILED", "Unable to publish transaction");
                        return;
                    }

                    switch (result.status()) {
                        case PUBLISHED -> pending.markIngressPublished(result.inboundSequence());
                        case QUEUE_FULL -> pending.rejectIngress("INGRESS_OVERLOADED",
                                "Gateway is busy; retry the transaction");
                        case RING_FULL -> pending.rejectIngress("SEQUENCER_BACKPRESSURE",
                                "Sequencer ring is full; retry the transaction");
                        case SHUTTING_DOWN -> pending.rejectIngress("GATEWAY_SHUTTING_DOWN",
                                "Gateway is not accepting new transactions");
                    }
                }));
    }

    private static long newConnectionSeed() {
        long seed = new SecureRandom().nextLong() & Long.MAX_VALUE;
        return seed == 0 ? 1 : seed;
    }

    private static void writeReject(ChannelHandlerContext ctx, String code, String message) {
        GatewayResponse rejection = GatewayResponse.reject(code, message);
        ctx.writeAndFlush(new TextWebSocketFrame(rejection.toWireText()));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        Long connectionId = ctx.channel().attr(CONNECTION_ID).get();
        if (connectionId != null) pendingRequests.removeConnection(connectionId);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.log(Level.WARNING, "WebSocket connection error from " + ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
