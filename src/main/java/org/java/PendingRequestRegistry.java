package org.java;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/** Correlates an asynchronous Audit ACK with the originating WebSocket channel. */
public final class PendingRequestRegistry {
    private static final Logger LOGGER = Logger.getLogger(PendingRequestRegistry.class.getName());

    private record Key(long connectionId, long transactionId) { }

    private static final int MAX_PENDING = AckRingProtocol.CAPACITY;

    private final Map<Key, PendingRequest> pending = new ConcurrentHashMap<>();
    private final AtomicInteger pendingCount = new AtomicInteger();

    public PendingRequest register(long connectionId, long transactionId, Channel channel) {
        // Bound in-flight correlation state to the ACK ring capacity. This turns
        // a stalled Audit consumer into explicit Gateway backpressure.
        while (true) {
            int current = pendingCount.get();
            if (current >= MAX_PENDING) return null;
            if (pendingCount.compareAndSet(current, current + 1)) break;
        }

        Key key = new Key(connectionId, transactionId);
        PendingRequest request = new PendingRequest(key, channel);
        if (pending.putIfAbsent(key, request) != null) {
            pendingCount.decrementAndGet();
            return null;
        }
        return request;
    }

    public boolean isAtCapacity() {
        return pendingCount.get() >= MAX_PENDING;
    }

    private void removePending(Key key, PendingRequest request) {
        if (pending.remove(key, request)) pendingCount.decrementAndGet();
    }

    /** Called by the ACK reader thread. Completion means the ACK was written or safely discarded. */
    public CompletableFuture<Void> route(AuditAcknowledgement acknowledgement) {
        Key key = new Key(acknowledgement.connectionId(), acknowledgement.transactionId());
        PendingRequest request = pending.get(key);
        if (request == null) {
            // The client may have disconnected or this may be a replayed ACK.
            return CompletableFuture.completedFuture(null);
        }
        return request.acceptAcknowledgement(acknowledgement);
    }

    public void removeConnection(long connectionId) {
        pending.forEach((key, request) -> {
            if (key.connectionId() == connectionId) {
                removePending(key, request);
                request.disconnected();
            }
        });
    }

    public int pendingCount() {
        return pending.size();
    }

    public final class PendingRequest {
        private final Key key;
        private final Channel channel;
        private final AtomicBoolean terminal = new AtomicBoolean(false);
        private AuditAcknowledgement acknowledgement;
        private boolean ingressWriteSucceeded;
        private volatile CompletableFuture<Void> acknowledgementDelivery;

        private PendingRequest(Key key, Channel channel) {
            this.key = key;
            this.channel = channel;
        }

        /** Must be invoked on this channel's Netty event loop. */
        public void markIngressPublished(long inboundSequence) {
            if (terminal.get() || !channel.isActive()) {
                finishWithoutDelivery();
                return;
            }
            ChannelFuture ingressWrite = channel.writeAndFlush(new TextWebSocketFrame(
                    "ACCEPT|stage=INGRESS|transactionId=" + key.transactionId()
                            + "|inboundSequence=" + inboundSequence));
            ingressWrite.addListener(future -> {
                if (!future.isSuccess()) {
                    LOGGER.fine("Ingress acknowledgement could not be delivered; channel is closing");
                    finishWithoutDelivery();
                    channel.close();
                    return;
                }
                ingressWriteSucceeded = true;
                flushAuditAcknowledgementIfReady();
            });
        }

        /** Must be invoked on this channel's Netty event loop. */
        public void rejectIngress(String code, String message) {
            if (!terminal.compareAndSet(false, true)) return;
            removePending(key, this);
            if (!channel.isActive()) return;
            String safeMessage = message.replace('|', '/').replace('\n', ' ').replace('\r', ' ');
            channel.writeAndFlush(new TextWebSocketFrame(
                    "REJECT|code=" + code + "|message=" + safeMessage));
        }

        private CompletableFuture<Void> acceptAcknowledgement(AuditAcknowledgement incoming) {
            CompletableFuture<Void> completion = new CompletableFuture<>();
            if (!channel.isActive()) {
                finishWithoutDelivery();
                completion.complete(null);
                return completion;
            }
            try {
                channel.eventLoop().execute(() -> {
                    if (terminal.get()) {
                        completion.complete(null);
                        return;
                    }
                    if (acknowledgement != null) {
                        // A replay can occur after a crash between ACK publication and
                        // journal consumer advancement. Only one client ACK is emitted.
                        completion.complete(null);
                        return;
                    }
                    acknowledgement = incoming;
                    acknowledgementDelivery = completion;
                    flushAuditAcknowledgementIfReady();
                });
            } catch (RuntimeException rejected) {
                finishWithoutDelivery();
                completion.complete(null);
            }
            return completion;
        }

        private void flushAuditAcknowledgementIfReady() {
            if (!ingressWriteSucceeded || acknowledgement == null || terminal.get()) return;
            if (!channel.isActive()) {
                finishWithoutDelivery();
                return;
            }

            channel.writeAndFlush(new TextWebSocketFrame(acknowledgement.toWireText()))
                    .addListener(future -> {
                        if (future.isSuccess()) {
                            if (terminal.compareAndSet(false, true)) removePending(key, this);
                            if (acknowledgementDelivery != null) acknowledgementDelivery.complete(null);
                        } else {
                            // A failed WebSocket write cannot be repaired through this
                            // channel. Close it and consume the ACK; the client can query
                            // transaction status after reconnecting in a later API.
                            LOGGER.warning("Unable to write final audit ACK; closing WebSocket: "
                                    + future.cause());
                            finishWithoutDelivery();
                            channel.close();
                        }
                    });
        }

        private void disconnected() {
            if (terminal.compareAndSet(false, true) && acknowledgementDelivery != null) {
                acknowledgementDelivery.complete(null);
            }
        }

        private void finishWithoutDelivery() {
            if (terminal.compareAndSet(false, true)) removePending(key, this);
            if (acknowledgementDelivery != null) acknowledgementDelivery.complete(null);
        }
    }
}
