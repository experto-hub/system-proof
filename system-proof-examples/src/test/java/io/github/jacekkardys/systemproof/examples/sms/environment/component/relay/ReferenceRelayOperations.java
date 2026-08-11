package io.github.jacekkardys.systemproof.examples.sms.environment.component.relay;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import io.github.jacekkardys.systemproof.endpoint.SmppEndpoint;

/**
 * Success-capable reference SMPP-to-HTTP relay for one concurrent, one-part UCS2 delivery.
 * Unsupported or malformed traffic fails closed without a positive {@code deliver_sm_resp}.
 */
public final class ReferenceRelayOperations implements AutoCloseable {
    private static final Duration IO_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final int MAX_SMPP_PDU_BYTES = 64 * 1024;
    private static final int MAX_HTTP_HEADER_BYTES = 16 * 1024;
    private static final int MAX_HTTP_BODY_BYTES = 1024;
    private static final byte[] HTTP_ACK = "ACK/Jasmin".getBytes(StandardCharsets.US_ASCII);
    private static final long BIND_TRANSCEIVER = 0x00000009L;
    private static final long BIND_TRANSCEIVER_RESP = 0x80000009L;
    private static final long DELIVER_SM = 0x00000005L;
    private static final long DELIVER_SM_RESP = 0x80000005L;
    private static final long ENQUIRE_LINK = 0x00000015L;
    private static final long ENQUIRE_LINK_RESP = 0x80000015L;

    private final Socket smpp;
    private final URI callback;
    private final CompletableFuture<Delivery> completion = new CompletableFuture<>();
    private final Object lifecycleMonitor = new Object();
    private final Thread receiver;
    private boolean closed;
    private Socket activeHttpSocket;

    private ReferenceRelayOperations(Socket smpp, URI callback) {
        this.smpp = smpp;
        this.callback = callback;
        receiver = Thread.ofVirtual()
            .name("system-proof-reference-relay")
            .start(this::receiveOneDelivery);
    }

    public static ReferenceRelayOperations open(SmppEndpoint endpoint, URI callback)
        throws IOException {
        Objects.requireNonNull(endpoint, "SMPP endpoint must not be null");
        Objects.requireNonNull(callback, "HTTP callback must not be null");
        if (!"http".equalsIgnoreCase(callback.getScheme())
            || callback.getHost() == null
            || callback.getPort() <= 0
            || callback.getRawPath() == null
            || callback.getRawPath().isBlank()) {
            throw new IllegalArgumentException("Reference relay requires an explicit HTTP endpoint");
        }

        Socket socket = new Socket();
        try {
            socket.connect(
                new InetSocketAddress(endpoint.host(), endpoint.port()),
                Math.toIntExact(IO_TIMEOUT.toMillis())
            );
            socket.setSoTimeout(Math.toIntExact(IO_TIMEOUT.toMillis()));
            long bindSequence = 1;
            writePdu(
                socket.getOutputStream(),
                bindRequest(
                    bindSequence,
                    endpoint.systemId(),
                    endpoint.password().reveal()
                )
            );
            Pdu response = readPdu(socket.getInputStream());
            if (response.commandId != BIND_TRANSCEIVER_RESP
                || response.commandStatus != 0
                || response.sequence != bindSequence) {
                throw new IOException("Reference relay SMPP bind was rejected");
            }
            return new ReferenceRelayOperations(socket, callback);
        } catch (IOException | RuntimeException failure) {
            try {
                socket.close();
            } catch (IOException suppressed) {
                failure.addSuppressed(suppressed);
            }
            throw failure;
        }
    }

    /** Waits for the single supported delivery to complete through the positive SMPP response. */
    public Delivery awaitDelivery() {
        try {
            return completion.get(DELIVERY_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while awaiting reference relay delivery", failure);
        } catch (ExecutionException failure) {
            throw new IllegalStateException(
                "Reference relay delivery failed",
                failure.getCause()
            );
        } catch (TimeoutException failure) {
            throw new IllegalStateException("Timed out awaiting reference relay delivery", failure);
        }
    }

    private void receiveOneDelivery() {
        try {
            while (!isClosed()) {
                Pdu pdu = readPdu(smpp.getInputStream());
                if (pdu.commandId == ENQUIRE_LINK) {
                    requireRequest(pdu, ENQUIRE_LINK, 0);
                    writePdu(
                        smpp.getOutputStream(),
                        pdu(ENQUIRE_LINK_RESP, 0, pdu.sequence, new byte[0])
                    );
                    continue;
                }
                requireRequest(pdu, DELIVER_SM, -1);
                ParsedDelivery delivery = parseDelivery(pdu);
                String callbackId = "reference-relay-" + UUID.randomUUID();
                HttpResponse response = postCallback(delivery, callbackId);
                if (response.status != 200 || !Arrays.equals(response.body, HTTP_ACK)) {
                    throw new IOException(
                        "Reference relay rejected non-positive HTTP callback response"
                    );
                }
                writePdu(
                    smpp.getOutputStream(),
                    pdu(DELIVER_SM_RESP, 0, pdu.sequence, new byte[] {0})
                );
                completion.complete(new Delivery(
                    pdu.sequence,
                    delivery.sourceAddress,
                    delivery.destinationAddress,
                    delivery.dataCoding,
                    delivery.content,
                    callbackId
                ));
                return;
            }
        } catch (Throwable failure) {
            if (!isClosed()) {
                completion.completeExceptionally(failure);
                closeSocketAfterFailure(failure);
            }
        }
    }

    private HttpResponse postCallback(ParsedDelivery delivery, String callbackId)
        throws IOException {
        byte[] body = callbackBody(delivery, callbackId);
        byte[] request = ("POST " + callback.getRawPath() + " HTTP/1.1\r\n"
            + "Host: " + callback.getHost() + ":" + callback.getPort() + "\r\n"
            + "Content-Type: application/x-www-form-urlencoded\r\n"
            + "Content-Length: " + body.length + "\r\n"
            + "Connection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
        Socket socket = new Socket();
        registerHttpSocket(socket);
        try (socket) {
            socket.connect(
                new InetSocketAddress(callback.getHost(), callback.getPort()),
                Math.toIntExact(IO_TIMEOUT.toMillis())
            );
            socket.setSoTimeout(Math.toIntExact(IO_TIMEOUT.toMillis()));
            OutputStream output = socket.getOutputStream();
            output.write(request);
            output.write(body);
            output.flush();
            return readHttpResponse(socket.getInputStream());
        } finally {
            releaseHttpSocket(socket);
        }
    }

    private void registerHttpSocket(Socket socket) throws IOException {
        IOException rejection;
        synchronized (lifecycleMonitor) {
            if (closed) {
                rejection = new IOException("Reference relay is closed");
            } else if (activeHttpSocket != null) {
                rejection = new IOException("Reference relay already has an active HTTP callback");
            } else {
                activeHttpSocket = socket;
                return;
            }
        }
        try {
            socket.close();
        } catch (IOException closeFailure) {
            rejection.addSuppressed(closeFailure);
        }
        throw rejection;
    }

    private void releaseHttpSocket(Socket socket) {
        synchronized (lifecycleMonitor) {
            if (activeHttpSocket == socket) {
                activeHttpSocket = null;
            }
        }
    }

    private boolean isClosed() {
        synchronized (lifecycleMonitor) {
            return closed;
        }
    }

    private static HttpResponse readHttpResponse(InputStream source) throws IOException {
        byte[] headerBytes = readHttpHeaders(source);
        String headers = new String(headerBytes, StandardCharsets.ISO_8859_1);
        String[] lines = headers.substring(0, headers.length() - 4).split("\\r\\n");
        if (lines.length == 0) {
            throw new IOException("HTTP response has no status line");
        }
        String[] statusParts = lines[0].split(" ", 3);
        if (statusParts.length < 2
            || !(statusParts[0].equals("HTTP/1.1") || statusParts[0].equals("HTTP/1.0"))
            || !statusParts[1].matches("[0-9]{3}")) {
            throw new IOException("Unsupported HTTP response status line");
        }
        int status = Integer.parseInt(statusParts[1]);
        Map<String, String> fields = new LinkedHashMap<>();
        for (int index = 1; index < lines.length; index++) {
            int colon = lines[index].indexOf(':');
            if (colon <= 0) {
                throw new IOException("Malformed HTTP response header");
            }
            String name = lines[index].substring(0, colon).strip().toLowerCase(Locale.ROOT);
            String value = lines[index].substring(colon + 1).strip();
            if (name.isEmpty() || fields.putIfAbsent(name, value) != null) {
                throw new IOException("Ambiguous HTTP response header");
            }
        }
        String transferEncoding = fields.get("transfer-encoding");
        String contentLength = fields.get("content-length");
        if (transferEncoding != null && contentLength != null) {
            throw new IOException("Ambiguous HTTP response framing");
        }
        byte[] body;
        if (transferEncoding != null) {
            if (!transferEncoding.equalsIgnoreCase("chunked")) {
                throw new IOException("Unsupported HTTP transfer encoding");
            }
            body = readChunkedBody(source);
        } else if (contentLength != null) {
            int length = parseBoundedLength(contentLength, MAX_HTTP_BODY_BYTES);
            body = readExactly(source, length, "HTTP response body");
        } else {
            body = readUntilEof(source, MAX_HTTP_BODY_BYTES);
        }
        return new HttpResponse(status, body);
    }

    private static byte[] readHttpHeaders(InputStream source) throws IOException {
        ByteArrayOutputStream headers = new ByteArrayOutputStream();
        while (headers.size() < MAX_HTTP_HEADER_BYTES) {
            int value = source.read();
            if (value < 0) {
                throw new EOFException("HTTP response ended before its headers completed");
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
        throw new IOException("HTTP response headers exceed the configured limit");
    }

    private static byte[] readChunkedBody(InputStream source) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        while (true) {
            String sizeLine = readAsciiLine(source, 128);
            if (sizeLine.indexOf(';') >= 0 || sizeLine.isBlank()) {
                throw new IOException("Unsupported HTTP chunk size");
            }
            int size;
            try {
                size = Integer.parseInt(sizeLine, 16);
            } catch (NumberFormatException failure) {
                throw new IOException("Malformed HTTP chunk size", failure);
            }
            if (size < 0 || body.size() + size > MAX_HTTP_BODY_BYTES) {
                throw new IOException("HTTP response body exceeds the configured limit");
            }
            if (size == 0) {
                if (!readAsciiLine(source, 128).isEmpty()) {
                    throw new IOException("HTTP response trailers are unsupported");
                }
                return body.toByteArray();
            }
            body.writeBytes(readExactly(source, size, "HTTP response chunk"));
            if (source.read() != '\r' || source.read() != '\n') {
                throw new IOException("Malformed HTTP chunk terminator");
            }
        }
    }

    private static String readAsciiLine(InputStream source, int limit) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        while (line.size() <= limit) {
            int value = source.read();
            if (value < 0) {
                throw new EOFException("HTTP response ended inside a line");
            }
            if (value == '\r') {
                if (source.read() != '\n') {
                    throw new IOException("Malformed HTTP line terminator");
                }
                return line.toString(StandardCharsets.US_ASCII);
            }
            if (value > 0x7f) {
                throw new IOException("HTTP framing line is not ASCII");
            }
            line.write(value);
        }
        throw new IOException("HTTP framing line exceeds the configured limit");
    }

    private static int parseBoundedLength(String value, int maximum) throws IOException {
        try {
            long length = Long.parseLong(value);
            if (length < 0 || length > maximum) {
                throw new IOException("HTTP response body exceeds the configured limit");
            }
            return Math.toIntExact(length);
        } catch (NumberFormatException failure) {
            throw new IOException("Malformed HTTP Content-Length", failure);
        }
    }

    private static byte[] readUntilEof(InputStream source, int maximum) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        while (body.size() <= maximum) {
            int value = source.read();
            if (value < 0) {
                return body.toByteArray();
            }
            body.write(value);
        }
        throw new IOException("HTTP response body exceeds the configured limit");
    }

    private static byte[] callbackBody(ParsedDelivery delivery, String callbackId) {
        String body = "id=" + percentEncode(callbackId.getBytes(StandardCharsets.UTF_8))
            + "&from=" + percentEncode(
                delivery.sourceAddress.getBytes(StandardCharsets.US_ASCII)
            )
            + "&to=" + percentEncode(
                delivery.destinationAddress.getBytes(StandardCharsets.US_ASCII)
            )
            + "&origin-connector=system-proof-reference-relay"
            + "&content=" + percentEncode(delivery.messageBytes)
            + "&binary=" + HexFormat.of().formatHex(delivery.messageBytes)
            + "&coding=" + delivery.dataCoding;
        return body.getBytes(StandardCharsets.US_ASCII);
    }

    private static ParsedDelivery parseDelivery(Pdu pdu) throws IOException {
        ByteBuffer body = ByteBuffer.wrap(pdu.body).order(ByteOrder.BIG_ENDIAN);
        if (!readCOctet(body, 5, "service_type").isEmpty()) {
            throw new IOException("Reference relay supports only the default SMPP service type");
        }
        readUnsignedByte(body, "source_addr_ton");
        readUnsignedByte(body, "source_addr_npi");
        String sourceAddress = readCOctet(body, 20, "source_addr");
        readUnsignedByte(body, "dest_addr_ton");
        readUnsignedByte(body, "dest_addr_npi");
        String destinationAddress = readCOctet(body, 20, "destination_addr");
        int esmClass = readUnsignedByte(body, "esm_class");
        readUnsignedByte(body, "protocol_id");
        readUnsignedByte(body, "priority_flag");
        String schedule = readCOctet(body, 16, "schedule_delivery_time");
        String validity = readCOctet(body, 16, "validity_period");
        readUnsignedByte(body, "registered_delivery");
        readUnsignedByte(body, "replace_if_present_flag");
        int dataCoding = readUnsignedByte(body, "data_coding");
        readUnsignedByte(body, "sm_default_msg_id");
        int messageLength = readUnsignedByte(body, "sm_length");
        if (esmClass != 0 || !schedule.isEmpty() || !validity.isEmpty() || dataCoding != 8) {
            throw new IOException("Reference relay supports only one-part immediate UCS2 delivery");
        }
        if (sourceAddress.isBlank() || destinationAddress.isBlank()) {
            throw new IOException("Reference relay requires non-empty SMPP addresses");
        }
        if (messageLength == 0 || messageLength != body.remaining() || (messageLength & 1) != 0) {
            throw new IOException("Reference relay requires one complete UCS2 short_message");
        }
        byte[] messageBytes = new byte[messageLength];
        body.get(messageBytes);
        String content;
        try {
            content = StandardCharsets.UTF_16BE.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(messageBytes))
                .toString();
        } catch (CharacterCodingException failure) {
            throw new IOException("Reference relay received malformed UCS2 content", failure);
        }
        if (content.isEmpty()) {
            throw new IOException("Reference relay requires non-empty message content");
        }
        return new ParsedDelivery(
            sourceAddress,
            destinationAddress,
            dataCoding,
            content,
            messageBytes
        );
    }

    private static void requireRequest(Pdu pdu, long commandId, int requiredBodyLength)
        throws IOException {
        if (pdu.commandId != commandId
            || pdu.commandStatus != 0
            || pdu.sequence == 0
            || (requiredBodyLength >= 0 && pdu.body.length != requiredBodyLength)) {
            throw new IOException("Unsupported SMPP request");
        }
    }

    private static Pdu readPdu(InputStream source) throws IOException {
        byte[] header = readExactly(source, 16, "SMPP header");
        ByteBuffer values = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        int length = values.getInt();
        long commandId = Integer.toUnsignedLong(values.getInt());
        long commandStatus = Integer.toUnsignedLong(values.getInt());
        long sequence = Integer.toUnsignedLong(values.getInt());
        if (length < 16 || length > MAX_SMPP_PDU_BYTES) {
            throw new IOException("Invalid SMPP command length");
        }
        return new Pdu(
            commandId,
            commandStatus,
            sequence,
            readExactly(source, length - 16, "SMPP body")
        );
    }

    private static byte[] readExactly(InputStream source, int length, String description)
        throws IOException {
        byte[] bytes = source.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("Truncated " + description);
        }
        return bytes;
    }

    private static byte[] bindRequest(long sequence, String systemId, String password) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeCOctet(body, systemId);
        writeCOctet(body, password);
        writeCOctet(body, "");
        body.write(0x34);
        body.write(0);
        body.write(0);
        writeCOctet(body, "");
        return pdu(BIND_TRANSCEIVER, 0, sequence, body.toByteArray());
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

    private static void writePdu(OutputStream output, byte[] bytes) throws IOException {
        output.write(bytes);
        output.flush();
    }

    private static void writeCOctet(OutputStream output, String value) {
        try {
            output.write(ascii(value, "SMPP C-octet string"));
            output.write(0);
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot encode SMPP bind request", failure);
        }
    }

    private static String readCOctet(ByteBuffer source, int maximum, String description)
        throws IOException {
        ByteArrayOutputStream value = new ByteArrayOutputStream();
        while (source.hasRemaining() && value.size() <= maximum) {
            int current = Byte.toUnsignedInt(source.get());
            if (current == 0) {
                return new String(ascii(value.toByteArray(), description), StandardCharsets.US_ASCII);
            }
            value.write(current);
        }
        throw new IOException("Invalid or oversized SMPP " + description);
    }

    private static int readUnsignedByte(ByteBuffer source, String description)
        throws IOException {
        if (!source.hasRemaining()) {
            throw new EOFException("Missing SMPP " + description);
        }
        return Byte.toUnsignedInt(source.get());
    }

    private static byte[] ascii(String value, String description) {
        return ascii(value.getBytes(StandardCharsets.UTF_8), description);
    }

    private static byte[] ascii(byte[] value, String description) {
        for (byte current : value) {
            if (Byte.toUnsignedInt(current) > 0x7f) {
                throw new IllegalArgumentException(description + " must be ASCII");
            }
        }
        return value;
    }

    private static String percentEncode(byte[] bytes) {
        StringBuilder encoded = new StringBuilder(bytes.length * 3);
        for (byte value : bytes) {
            int unsigned = Byte.toUnsignedInt(value);
            if ((unsigned >= 'a' && unsigned <= 'z')
                || (unsigned >= 'A' && unsigned <= 'Z')
                || (unsigned >= '0' && unsigned <= '9')
                || unsigned == '-'
                || unsigned == '_'
                || unsigned == '.') {
                encoded.append((char) unsigned);
            } else {
                encoded.append('%');
                encoded.append(Character.toUpperCase(Character.forDigit(unsigned >>> 4, 16)));
                encoded.append(Character.toUpperCase(Character.forDigit(unsigned & 0xf, 16)));
            }
        }
        return encoded.toString();
    }

    private void closeSocketAfterFailure(Throwable failure) {
        try {
            smpp.close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    @Override
    public void close() throws Exception {
        Socket http;
        synchronized (lifecycleMonitor) {
            if (closed) {
                return;
            }
            closed = true;
            http = activeHttpSocket;
        }

        Exception cleanupFailure = null;
        cleanupFailure = closeSocket(smpp, cleanupFailure);
        cleanupFailure = closeSocket(http, cleanupFailure);
        receiver.interrupt();
        boolean interrupted = false;
        try {
            receiver.join(CLOSE_TIMEOUT.toMillis());
        } catch (InterruptedException failure) {
            interrupted = true;
            cleanupFailure = appendCleanupFailure(cleanupFailure, failure);
        }
        if (receiver.isAlive()) {
            cleanupFailure = appendCleanupFailure(
                cleanupFailure,
                new IllegalStateException("Reference relay receiver did not terminate")
            );
        }
        completion.completeExceptionally(
            new IllegalStateException("Reference relay closed before completing a delivery")
        );
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    private static Exception closeSocket(Socket socket, Exception cleanupFailure) {
        if (socket == null) {
            return cleanupFailure;
        }
        try {
            socket.close();
            return cleanupFailure;
        } catch (Exception failure) {
            return appendCleanupFailure(cleanupFailure, failure);
        }
    }

    private static Exception appendCleanupFailure(
        Exception cleanupFailure,
        Exception nextFailure
    ) {
        if (cleanupFailure == null) {
            return nextFailure;
        }
        cleanupFailure.addSuppressed(nextFailure);
        return cleanupFailure;
    }

    public record Delivery(
        long sequence,
        String sourceAddress,
        String destinationAddress,
        int dataCoding,
        String content,
        String callbackId
    ) {}

    private record Pdu(
        long commandId,
        long commandStatus,
        long sequence,
        byte[] body
    ) {}

    private record ParsedDelivery(
        String sourceAddress,
        String destinationAddress,
        int dataCoding,
        String content,
        byte[] messageBytes
    ) {}

    private record HttpResponse(int status, byte[] body) {}
}
