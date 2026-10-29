
package org.java;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processes WebSocket application data frames.
 *
 * Binary frames are expected to contain FlatBuffers transactions.
 * Text frames are rejected because they are not part of the input protocol.
 */
public final class WebSocketFrameHandler
        extends SimpleChannelInboundHandler<WebSocketFrame> {

    private static final Logger LOGGER =
            Logger.getLogger(WebSocketFrameHandler.class.getName());

    private final TransactionValidator validator;

    public WebSocketFrameHandler(TransactionValidator validator) {
        // Fail fast if the handler is constructed without a validator.
        this.validator = Objects.requireNonNull(validator, "validator");
    }

    /**
     * Netty calls this method when an application WebSocket frame arrives.
     *
     * SimpleChannelInboundHandler releases the incoming frame after this
     * method finishes, so we should not retain or use its ByteBuf afterward.
     */
    @Override
    protected void channelRead0(
            ChannelHandlerContext ctx,
            WebSocketFrame frame
    ) {
        GatewayResponse response;

        if (frame instanceof BinaryWebSocketFrame binaryFrame) {

            ByteBuf content = binaryFrame.content();

            // Respect the buffer's current readable range, not its total capacity.
            int readableBytes = content.readableBytes();

            // Expose the readable portion of the Netty buffer as a ByteBuffer.
// This avoids our explicit byte[] allocation when the buffer supports a view.
            ByteBuffer payload = content.nioBuffer(
                    content.readerIndex(),
                    content.readableBytes()
            );

// Validation must finish before Netty releases the incoming frame.
            response = validator.validate(payload);

        } else if (frame instanceof TextWebSocketFrame) {

            // This gateway expects binary FlatBuffers, not JSON or plain text.
            response = GatewayResponse.reject(
                    "BINARY_FRAME_REQUIRED",
                    "Send the transaction as a binary WebSocket frame"
            );

        } else {

            // Reject any unexpected application frame type.
            response = GatewayResponse.reject(
                    "UNSUPPORTED_FRAME",
                    "This WebSocket frame type is not supported"
            );
        }

        /*
         * Return the application-level result to the same WebSocket client.
         * The response is text, even though the request is binary.
         */
        ctx.writeAndFlush(new TextWebSocketFrame(response.toWireText()));
    }

    /**
     * Handle unexpected Netty/network errors without leaving the connection
     * in an uncertain state.
     */
    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx,
            Throwable cause
    ) {
        LOGGER.log(
                Level.WARNING,
                "WebSocket connection error from " + ctx.channel().remoteAddress(),
                cause
        );

        // Close a connection after an unexpected pipeline/network error.
        ctx.close();
    }
}
