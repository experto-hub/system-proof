package io.github.jacekkardys.systemproof.examples.sms.environment.component.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.configuration.Secret;
import io.github.jacekkardys.systemproof.endpoint.SmppEndpoint;

class ReferenceRelayOperationsTest {
    private static final Duration FIXTURE_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration CLOSE_ASSERTION_TIMEOUT = Duration.ofSeconds(5);
    private static final int MAX_TEST_FRAME_BYTES = 64 * 1024;
    private static final long BIND_TRANSCEIVER = 0x00000009L;
    private static final long BIND_TRANSCEIVER_RESP = 0x80000009L;
    private static final long DELIVER_SM = 0x00000005L;
    private static final long DELIVER_SM_RESP = 0x80000005L;
    private static final long DELIVERY_SEQUENCE = 77;

    @Test
    void closeUnblocksHttpResponseReadAndReleasesBothConnections() throws Exception {
        try (ControlledPeers peers = ControlledPeers.open()) {
            SmppEndpoint smsc = new SmppEndpoint(
                "127.0.0.1",
                peers.smppPort(),
                "reference-relay",
                Secret.secret("password")
            );
            URI callback = URI.create(
                "http://127.0.0.1:" + peers.httpPort() + "/callback"
            );

            try (ReferenceRelayOperations relay = ReferenceRelayOperations.open(smsc, callback)) {
                peers.awaitCompleteCallbackRequest();

                assertTimeoutPreemptively(CLOSE_ASSERTION_TIMEOUT, relay::close);

                assertThat(peers.awaitHttpObservation().peerClosed()).isTrue();
                SmppObservation smpp = peers.awaitSmppObservation();
                assertThat(smpp.peerClosed()).isTrue();
                assertThat(smpp.positiveDeliveryResponse()).isFalse();
                assertThatThrownBy(relay::awaitDelivery)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Reference relay delivery failed")
                    .hasRootCauseMessage("Reference relay closed before completing a delivery");
                assertDoesNotThrow(relay::close);
            }
        }
    }

    private static final class ControlledPeers implements AutoCloseable {
        private final ServerSocket smppListener;
        private final ServerSocket httpListener;
        private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
        private final AtomicReference<Socket> smppConnection = new AtomicReference<>();
        private final AtomicReference<Socket> httpConnection = new AtomicReference<>();
        private final CompletableFuture<Void> callbackRequest = new CompletableFuture<>();
        private final CompletableFuture<SmppObservation> smppObservation;
        private final CompletableFuture<HttpObservation> httpObservation;

        private ControlledPeers(ServerSocket smppListener, ServerSocket httpListener) {
            this.smppListener = smppListener;
            this.httpListener = httpListener;
            smppObservation = CompletableFuture.supplyAsync(this::serveSmpp, tasks);
            httpObservation = CompletableFuture.supplyAsync(this::serveHttp, tasks);
        }

        private static ControlledPeers open() throws IOException {
            ServerSocket smpp = loopbackListener();
            ServerSocket http = null;
            try {
                http = loopbackListener();
                return new ControlledPeers(smpp, http);
            } catch (IOException | RuntimeException failure) {
                if (http != null) {
                    try {
                        http.close();
                    } catch (IOException closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                try {
                    smpp.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        private int smppPort() {
            return smppListener.getLocalPort();
        }

        private int httpPort() {
            return httpListener.getLocalPort();
        }

        private void awaitCompleteCallbackRequest() throws Exception {
            CompletableFuture.anyOf(callbackRequest, smppObservation)
                .get(FIXTURE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!callbackRequest.isDone()) {
                throw new IllegalStateException("SMPP peer closed before the HTTP callback arrived");
            }
            await(callbackRequest, "complete HTTP callback request");
        }

        private SmppObservation awaitSmppObservation() throws Exception {
            return await(smppObservation, "relay SMPP connection closure");
        }

        private HttpObservation awaitHttpObservation() throws Exception {
            return await(httpObservation, "relay HTTP connection closure");
        }

        private SmppObservation serveSmpp() {
            try (Socket socket = smppListener.accept()) {
                smppConnection.set(socket);
                socket.setSoTimeout(Math.toIntExact(FIXTURE_TIMEOUT.toMillis()));
                Pdu bind = readPdu(socket.getInputStream());
                if (bind.commandId != BIND_TRANSCEIVER
                    || bind.commandStatus != 0
                    || bind.sequence == 0) {
                    throw new IOException("Reference relay sent an invalid SMPP bind");
                }
                writePdu(
                    socket.getOutputStream(),
                    pdu(
                        BIND_TRANSCEIVER_RESP,
                        0,
                        bind.sequence,
                        cOctet("controlled-smsc")
                    )
                );
                writePdu(socket.getOutputStream(), deliverSm());

                boolean positiveDeliveryResponse = false;
                try {
                    while (true) {
                        Pdu response = readPduOrEof(socket.getInputStream());
                        if (response == null) {
                            return new SmppObservation(true, positiveDeliveryResponse);
                        }
                        if (response.commandId == DELIVER_SM_RESP
                            && response.commandStatus == 0
                            && response.sequence == DELIVERY_SEQUENCE) {
                            positiveDeliveryResponse = true;
                        }
                    }
                } catch (EOFException | SocketException closedByRelay) {
                    return new SmppObservation(true, positiveDeliveryResponse);
                }
            } catch (Throwable failure) {
                throw new CompletionException(failure);
            }
        }

        private HttpObservation serveHttp() {
            try (Socket socket = httpListener.accept()) {
                httpConnection.set(socket);
                socket.setSoTimeout(Math.toIntExact(FIXTURE_TIMEOUT.toMillis()));
                readCompleteHttpRequest(socket.getInputStream());
                callbackRequest.complete(null);
                try {
                    return new HttpObservation(socket.getInputStream().read() < 0);
                } catch (SocketException closedByRelay) {
                    return new HttpObservation(true);
                }
            } catch (Throwable failure) {
                callbackRequest.completeExceptionally(failure);
                throw new CompletionException(failure);
            }
        }

        @Override
        public void close() throws Exception {
            Exception cleanupFailure = null;
            cleanupFailure = closeSocket(smppConnection.get(), cleanupFailure);
            cleanupFailure = closeSocket(httpConnection.get(), cleanupFailure);
            cleanupFailure = closeServer(smppListener, cleanupFailure);
            cleanupFailure = closeServer(httpListener, cleanupFailure);
            tasks.shutdownNow();
            boolean interrupted = false;
            try {
                if (!tasks.awaitTermination(
                    FIXTURE_TIMEOUT.toMillis(),
                    TimeUnit.MILLISECONDS
                )) {
                    cleanupFailure = append(
                        cleanupFailure,
                        new IllegalStateException("Controlled socket peers did not terminate")
                    );
                }
            } catch (InterruptedException failure) {
                interrupted = true;
                cleanupFailure = append(cleanupFailure, failure);
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            if (cleanupFailure != null) {
                throw cleanupFailure;
            }
        }
    }

    private static ServerSocket loopbackListener() throws IOException {
        ServerSocket listener = new ServerSocket();
        listener.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        return listener;
    }

    private static void readCompleteHttpRequest(InputStream source) throws IOException {
        String headers = new String(readHttpHeaders(source), StandardCharsets.ISO_8859_1);
        String[] lines = headers.substring(0, headers.length() - 4).split("\\r\\n");
        if (lines.length == 0 || !lines[0].equals("POST /callback HTTP/1.1")) {
            throw new IOException("Reference relay sent an invalid HTTP request line");
        }
        int contentLength = -1;
        for (int index = 1; index < lines.length; index++) {
            int colon = lines[index].indexOf(':');
            if (colon <= 0) {
                throw new IOException("Reference relay sent a malformed HTTP header");
            }
            if (lines[index].substring(0, colon).strip().toLowerCase(Locale.ROOT)
                .equals("content-length")) {
                contentLength = Integer.parseInt(lines[index].substring(colon + 1).strip());
            }
        }
        if (contentLength <= 0 || contentLength > MAX_TEST_FRAME_BYTES) {
            throw new IOException("Reference relay sent an invalid HTTP request body length");
        }
        if (source.readNBytes(contentLength).length != contentLength) {
            throw new EOFException("Reference relay sent a truncated HTTP request body");
        }
    }

    private static byte[] readHttpHeaders(InputStream source) throws IOException {
        ByteArrayOutputStream headers = new ByteArrayOutputStream();
        while (headers.size() < MAX_TEST_FRAME_BYTES) {
            int value = source.read();
            if (value < 0) {
                throw new EOFException("Reference relay ended the HTTP request inside headers");
            }
            headers.write(value);
            byte[] bytes = headers.toByteArray();
            int length = bytes.length;
            if (length >= 4
                && bytes[length - 4] == '\r'
                && bytes[length - 3] == '\n'
                && bytes[length - 2] == '\r'
                && bytes[length - 1] == '\n') {
                return bytes;
            }
        }
        throw new IOException("Reference relay HTTP request headers exceed the test limit");
    }

    private static byte[] deliverSm() {
        byte[] content = "lifecycle".getBytes(StandardCharsets.UTF_16BE);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeCOctet(body, "");
        body.write(0);
        body.write(0);
        writeCOctet(body, "48123456789");
        body.write(0);
        body.write(0);
        writeCOctet(body, "48987654321");
        body.write(0);
        body.write(0);
        body.write(0);
        writeCOctet(body, "");
        writeCOctet(body, "");
        body.write(0);
        body.write(0);
        body.write(8);
        body.write(0);
        body.write(content.length);
        body.writeBytes(content);
        return pdu(DELIVER_SM, 0, DELIVERY_SEQUENCE, body.toByteArray());
    }

    private static Pdu readPdu(InputStream source) throws IOException {
        Pdu pdu = readPduOrEof(source);
        if (pdu == null) {
            throw new EOFException("SMPP peer closed before sending a PDU");
        }
        return pdu;
    }

    private static Pdu readPduOrEof(InputStream source) throws IOException {
        byte[] header = source.readNBytes(16);
        if (header.length == 0) {
            return null;
        }
        if (header.length != 16) {
            throw new EOFException("SMPP peer closed inside a PDU header");
        }
        ByteBuffer values = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        int length = values.getInt();
        long commandId = Integer.toUnsignedLong(values.getInt());
        long commandStatus = Integer.toUnsignedLong(values.getInt());
        long sequence = Integer.toUnsignedLong(values.getInt());
        if (length < 16 || length > MAX_TEST_FRAME_BYTES) {
            throw new IOException("SMPP peer sent an invalid PDU length");
        }
        byte[] body = source.readNBytes(length - 16);
        if (body.length != length - 16) {
            throw new EOFException("SMPP peer closed inside a PDU body");
        }
        return new Pdu(commandId, commandStatus, sequence, body);
    }

    private static byte[] pdu(
        long commandId,
        long commandStatus,
        long sequence,
        byte[] body
    ) {
        return ByteBuffer.allocate(16 + body.length)
            .order(ByteOrder.BIG_ENDIAN)
            .putInt(16 + body.length)
            .putInt((int) commandId)
            .putInt((int) commandStatus)
            .putInt((int) sequence)
            .put(body)
            .array();
    }

    private static byte[] cOctet(String value) {
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        writeCOctet(target, value);
        return target.toByteArray();
    }

    private static void writeCOctet(OutputStream target, String value) {
        try {
            target.write(value.getBytes(StandardCharsets.US_ASCII));
            target.write(0);
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot write a test SMPP C-octet", failure);
        }
    }

    private static void writePdu(OutputStream target, byte[] bytes) throws IOException {
        target.write(bytes);
        target.flush();
    }

    private static <T> T await(CompletableFuture<T> future, String description)
        throws Exception {
        try {
            return future.get(FIXTURE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw new AssertionError("Controlled peer failed: " + description, cause);
        } catch (TimeoutException failure) {
            throw new AssertionError("Timed out waiting for " + description, failure);
        }
    }

    private static Exception closeSocket(Socket socket, Exception cleanupFailure) {
        if (socket == null) {
            return cleanupFailure;
        }
        try {
            socket.close();
            return cleanupFailure;
        } catch (IOException failure) {
            return append(cleanupFailure, failure);
        }
    }

    private static Exception closeServer(
        ServerSocket listener,
        Exception cleanupFailure
    ) {
        try {
            listener.close();
            return cleanupFailure;
        } catch (IOException failure) {
            return append(cleanupFailure, failure);
        }
    }

    private static Exception append(Exception cleanupFailure, Exception nextFailure) {
        if (cleanupFailure == null) {
            return nextFailure;
        }
        cleanupFailure.addSuppressed(nextFailure);
        return cleanupFailure;
    }

    private record Pdu(
        long commandId,
        long commandStatus,
        long sequence,
        byte[] body
    ) {}

    private record SmppObservation(boolean peerClosed, boolean positiveDeliveryResponse) {}

    private record HttpObservation(boolean peerClosed) {}
}
