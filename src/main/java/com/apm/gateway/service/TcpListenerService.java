package com.apm.gateway.service;

import com.apm.gateway.data.ImeiExtractor;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class TcpListenerService {

    private static final Logger LOG = Logger.getLogger(TcpListenerService.class);

    @ConfigProperty(name = "nlb-ip", defaultValue = "192.168.36.41")
    private String nlbIp;
    
    @Inject
    KafkaProducerService producer;

    @Inject
    DeviceSessionManager sessionManager;

    @Inject
    @Any
    Instance<ImeiExtractor> extractorInstance;

    private List<ImeiExtractor> extractors;

    @ConfigProperty(name = "gateway.port", defaultValue = "8225")
    int port;

    @ConfigProperty(name = "gateway.buffer-size", defaultValue = "1024")
    int bufferSize;

    @ConfigProperty(name = "kafka.send.mode", defaultValue = "Immediate")
    String mode;

    @ConfigProperty(name = "kafka.send.batch.flush-interval-ms", defaultValue = "100")
    int flushIntervalMs;

    @ConfigProperty(name = "kafka.send.batch.unknown-flush-interval-ms", defaultValue = "60000")
    int unknownFlushIntervalMs;

    @ConfigProperty(name = "kafka.send.batch.max-batch-messages", defaultValue = "500")
    int maxBatchMessages;

    @ConfigProperty(name = "kafka.input-topic", defaultValue = "gps.raw.data")
    String inputTopic;

    @ConfigProperty(name = "kafka.unknown-topic", defaultValue = "gps.raw.data.unknown")
    String unknownTopic;

    private ServerSocket serverSocket;
    private ExecutorService executor;
    private volatile boolean running = false;

    public boolean isLive() {
        return running;
    }

    public boolean isReady() {
        return running && serverSocket != null && serverSocket.isBound() && !serverSocket.isClosed();
    }
    private SimpleKafkaBatcher knownBatcher;
    private SimpleKafkaBatcher unknownBatcher;

    private final Path sessionLogPath = Paths.get("session_log.txt");
    private final Path rawAsciiPath = Paths.get("raw_ascii.txt");
    private final Path rawHexPath = Paths.get("raw_hex.txt");

    void onStart(@Observes StartupEvent ev) {
        extractors = extractorInstance.stream().toList();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        running = true;

        if ("Batch".equals(mode)) {
            knownBatcher = new SimpleKafkaBatcher(producer, inputTopic, flushIntervalMs, maxBatchMessages);
            unknownBatcher = new SimpleKafkaBatcher(producer, unknownTopic, unknownFlushIntervalMs, maxBatchMessages);
        }

        // Start TCP listener in virtual thread
        executor.submit(this::startTcpListener);

        // Start idle client sweeper
        executor.submit(this::sweepIdleClients);

        // Start active devices reporter
        executor.submit(this::reportActiveDevices);
    }

    void onStop(@Observes ShutdownEvent ev) {
        running = false;
        if (knownBatcher != null) {
            knownBatcher.flush();
        }
        if (unknownBatcher != null) {
            unknownBatcher.flush();
        }
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                LOG.error("Error closing server socket", e);
            }
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    private void startTcpListener() {
        try {
            serverSocket = new ServerSocket(port);
            LOG.infof("TCP Listener started on port %d at %s", port, Instant.now());

            while (running) {
                Socket client = serverSocket.accept();
                // Handle each client in a new virtual thread
                executor.submit(() -> handleClient(client));
            }
        } catch (IOException e) {
            if (running) {
                LOG.errorf(e, "[TCP] Error in listener at %s", Instant.now());
            }
        } finally {
            LOG.infof("[TCP] Listener stopped at %s", Instant.now());
        }
    }

    private void handleClient(Socket client) {
        String stage = "ini";
        try {
            String remoteEndpoint = client.getRemoteSocketAddress() != null
                    ? client.getRemoteSocketAddress().toString()
                    : "unknown";

            appendToFile(sessionLogPath, String.format("[%s] Client connected: %s%n", Instant.now(), remoteEndpoint));

            // Enable TCP keep-alive
            client.setKeepAlive(true);

            OutputStream outputStream = client.getOutputStream();
            byte[] buffer = new byte[bufferSize];
            AtomicInteger packetCount = new AtomicInteger(0);

            while (running && client.isConnected() && !client.isClosed()) {
                stage = "read";

                try {
                    int bytesRead = client.getInputStream().read(buffer);
                    if (bytesRead <= 0) {
                        break;
                    }

                    byte[] rawBytes = new byte[bytesRead];
                    System.arraycopy(buffer, 0, rawBytes, 0, bytesRead);

                    String asciiString = new String(rawBytes, StandardCharsets.US_ASCII);
                    String hexString = bytesToHex(rawBytes);

                    // Log rawBytes
                    appendToFile(sessionLogPath, String.format("[%s] RawBytes : %s%n", Instant.now(), hexString));
                    appendToFile(rawAsciiPath, asciiString + "\n");
                    appendToFile(rawHexPath, hexString + "\n");

                    packetCount.incrementAndGet();

                    stage = "process";
                    Optional<String> imeiOpt = sessionManager.tryGetImei(client);
                    if (!imeiOpt.isPresent() && packetCount.get() <= 3) {
                        stage = "extract";
                        if (rawBytes.length < 3) {
                            LOG.warn("[TCP] Packet too short!");
                        } else {
                            for (ImeiExtractor extractor : extractors) {
                                Optional<String> extracted = extractor.tryExtractImei(rawBytes);
                                if (extracted.isPresent()) {
                                    imeiOpt = extracted;
                                    LOG.infof("[TCP] Raw Bytes: %s", hexString);
                                    sessionManager.registerOrUpdate(client, extracted.get());
                                    LOG.infof("[TCP] IMEI extracted by %s: %s at %s",
                                            extractor.getClass().getSimpleName(), extracted.get(), Instant.now());
                                    stage = "registered";
                                    break;
                                }
                            }
                        }
                    }

                    // Track GT06 serial number for command-packet construction
                    if ((rawBytes[0] == 0x78 || rawBytes[0] == 0x79) && rawBytes.length >= 6) {
                        int sIdx = rawBytes.length - 6;
                        short incomingSerial = (short) ((rawBytes[sIdx] << 8) | (rawBytes[sIdx + 1] & 0xFF));
                        sessionManager.updateLastSeen(client, incomingSerial);
                    }

                    // Notify any pending command listener with the device's response
                    imeiOpt.ifPresent(currentImei -> {
                        if ((rawBytes[0] == 0x78 || rawBytes[0] == 0x79) && rawBytes.length >= 4
                                && (rawBytes[3] == 0x15 || rawBytes[3] == 0x21
                                || (rawBytes[3] & 0xFF) == 0x80)) {
                            sessionManager.notifyBinaryResponse(currentImei, rawBytes);
                        } else {
                            sessionManager.notifyTextResponse(currentImei, asciiString);
                        }
                    });

                    stage = "send";
                    // GT06 login ACK
                    if ((rawBytes[0] == 0x78 || rawBytes[0] == 0x79) && rawBytes.length > 13 && rawBytes[3] == 0x01) {
                        stage = "gt06_handshake";

                        byte protocol = rawBytes[3];
                        int serialIndex = rawBytes.length - 6;
                        byte serial1 = rawBytes[serialIndex];
                        byte serial2 = rawBytes[serialIndex + 1];

                        byte[] crcInput = new byte[]{0x05, protocol, serial1, serial2};
                        short crc = getCrc16(crcInput);
                        byte crcHigh = (byte) ((crc >> 8) & 0xFF);
                        byte crcLow = (byte) (crc & 0xFF);

                        byte[] handshake = new byte[]{
                            0x78, 0x78, 0x05, protocol, serial1, serial2, crcHigh, crcLow, 0x0D, 0x0A
                        };

                        outputStream.write(handshake);
                        outputStream.flush();
                        LOG.infof("[TCP] Sent GT06 login response at %s, IMEI: %s", Instant.now(), imeiOpt.orElse("unknown"));
                        stage = "gt06_handshake_sent";
                    }

                    // GT06 location ACK (0x12)
                    if ((rawBytes[0] == 0x78 || rawBytes[0] == 0x79) && rawBytes.length > 7 && rawBytes[3] == 0x12) {
                        byte protocol = 0x12;
                        int serialIndex = rawBytes.length - 6;
                        byte serial1 = rawBytes[serialIndex];
                        byte serial2 = rawBytes[serialIndex + 1];

                        byte[] crcInput = new byte[]{0x05, protocol, serial1, serial2};
                        short crc = getCrc16(crcInput);
                        byte crcHigh = (byte) ((crc >> 8) & 0xFF);
                        byte crcLow = (byte) (crc & 0xFF);

                        byte[] locationAck = new byte[]{
                            0x78, 0x78, 0x05, protocol, serial1, serial2, crcHigh, crcLow, 0x0D, 0x0A
                        };

                        outputStream.write(locationAck);
                        outputStream.flush();
                    }

                    // GT06 heartbeat ACK (0x13)
                    if ((rawBytes[0] == 0x78 || rawBytes[0] == 0x79) && rawBytes.length > 7 && rawBytes[3] == 0x13) {
                        stage = "gt06_heartbeat";
                        byte protocol = 0x13;
                        int serialIndex = rawBytes.length - 6;
                        byte serial1 = rawBytes[serialIndex];
                        byte serial2 = rawBytes[serialIndex + 1];

                        byte[] crcInput = new byte[]{0x05, protocol, serial1, serial2};
                        short crc = getCrc16(crcInput);
                        byte crcHigh = (byte) ((crc >> 8) & 0xFF);
                        byte crcLow = (byte) (crc & 0xFF);

                        byte[] heartbeatAck = new byte[]{
                            0x78, 0x78, 0x05, protocol, serial1, serial2, crcHigh, crcLow, 0x0D, 0x0A
                        };

                        outputStream.write(heartbeatAck);
                        outputStream.flush();
                        LOG.infof("[TCP] Sent GT06 heartbeat response at %s, IMEI: %s", Instant.now(), imeiOpt.orElse("unknown"));
                        stage = "gt06_heartbeat_sent";
                    }

                    stage = "send_data";
                    String imei = imeiOpt.orElse(null);
                    if (imei == null || imei.length() < 15) {
                        if ("Batch".equals(mode) && unknownBatcher != null) {
                            unknownBatcher.add("unknown", rawBytes);
                        } else {
                            producer.send(unknownTopic, "unknown", rawBytes);
                        }

                        stage = "unknown_sent";
                        String dataUnk = new String(rawBytes, StandardCharsets.US_ASCII).replace("\0", "").trim();
                        LOG.infof("[TCP] IMEI not found or invalid from %s at %s raw:%s",
                                remoteEndpoint, Instant.now(), dataUnk);
                    } else {
                        if ("Batch".equals(mode) && knownBatcher != null) {
                            knownBatcher.add(imei, rawBytes);
                        } else {
                            producer.send(inputTopic, imei, rawBytes);
                        }

                        stage = "known_sent";
                    }

                    sessionManager.updateLastSeen(client);
                    stage = "updated_session";
                } catch (IOException e) {
                    if (running) {
                        LOG.warnf("[TCP] Read error for client %s: %s", remoteEndpoint, e.getMessage());
                    }
                    break;
                }
            }
        } catch (Exception ex) {
            LOG.errorf(ex, "[TCP] Error handling client %s: %s at %s stage %s",
                    client.getRemoteSocketAddress(), ex.getMessage(), Instant.now(), stage);
        } finally {
            String remoteEndpoint = client.getRemoteSocketAddress() != null
                    ? client.getRemoteSocketAddress().toString()
                    : "unknown";
            appendToFile(sessionLogPath,
                    String.format("[%s] Client disconnected: %s%n", Instant.now(), remoteEndpoint));
            sessionManager.remove(client);
            try {
                if (!client.isClosed()) {
                    client.shutdownInput();
                    client.shutdownOutput();
                }
            } catch (IOException e) {
                // Ignore
            }
            try {
                client.close();
                if (!remoteEndpoint.contains(nlbIp)) {
                    LOG.infof("[TCP] Client %s disconnected at %s", remoteEndpoint, Instant.now());
                }
            } catch (IOException e) {
                LOG.errorf(e, "[TCP] Error closing client %s: %s at %s",
                        remoteEndpoint, e.getMessage(), Instant.now());
            }
        }
    }

    private void sweepIdleClients() {
        while (running) {
            try {
                sessionManager.sweepIdleClients(Duration.ofMinutes(10));
                Thread.sleep(Duration.ofMinutes(5).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                LOG.errorf(ex, "[TCP] Error in sweeper task at %s", Instant.now());
            }
        }
    }

    private void reportActiveDevices() {
        while (running) {
            try {
                LOG.infof("[TCP] Active devices: %d at %s", sessionManager.getCount(), Instant.now());
                Thread.sleep(Duration.ofMinutes(1).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                LOG.errorf(ex, "[TCP] Error in active devices task at %s", Instant.now());
            }
        }
    }

    private void appendToFile(Path path, String content) {
        try {
            Files.writeString(path, content, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.warnf("Failed to write to file %s: %s", path, e.getMessage());
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X-", b));
        }
        return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
    }

    private static final int[] crctab16 = {
        0x0000, 0x1189, 0x2312, 0x329B, 0x4624, 0x57AD, 0x6536, 0x74BF, 0x8C48, 0x9DC1, 0xAF5A, 0xBED3,
        0xCA6C, 0xDBE5, 0xE97E, 0xF8F7, 0x1081, 0x0108, 0x3393, 0x221A, 0x56A5, 0x472C, 0x75B7, 0x643E,
        0x9CC9, 0x8D40, 0xBFDB, 0xAE52, 0xDAED, 0xCB64, 0xF9FF, 0xE876, 0x2102, 0x308B, 0x0210, 0x1399,
        0x6726, 0x76AF, 0x4434, 0x55BD, 0xAD4A, 0xBCC3, 0x8E58, 0x9FD1, 0xEB6E, 0xFAE7, 0xC87C, 0xD9F5,
        0x3183, 0x200A, 0x1291, 0x0318, 0x77A7, 0x662E, 0x54B5, 0x453C, 0xBDCB, 0xAC42, 0x9ED9, 0x8F50,
        0xFBEF, 0xEA66, 0xD8FD, 0xC974, 0x4204, 0x538D, 0x6116, 0x709F, 0x0420, 0x15A9, 0x2732, 0x36BB,
        0xCE4C, 0xDFC5, 0xED5E, 0xFCD7, 0x8868, 0x99E1, 0xAB7A, 0xBAF3, 0x5285, 0x430C, 0x7197, 0x601E,
        0x14A1, 0x0528, 0x37B3, 0x263A, 0xDECD, 0xCF44, 0xFDDF, 0xEC56, 0x98E9, 0x8960, 0xBBFB, 0xAA72,
        0x6306, 0x728F, 0x4014, 0x519D, 0x2522, 0x34AB, 0x0630, 0x17B9, 0xEF4E, 0xFEC7, 0xCC5C, 0xDDD5,
        0xA96A, 0xB8E3, 0x8A78, 0x9BF1, 0x7387, 0x620E, 0x5095, 0x411C, 0x35A3, 0x242A, 0x16B1, 0x0738,
        0xFFCF, 0xEE46, 0xDCDD, 0xCD54, 0xB9EB, 0xA862, 0x9AF9, 0x8B70, 0x8408, 0x9581, 0xA71A, 0xB693,
        0xC22C, 0xD3A5, 0xE13E, 0xF0B7, 0x0840, 0x19C9, 0x2B52, 0x3ADB, 0x4E64, 0x5FED, 0x6D76, 0x7CFF,
        0x9489, 0x8500, 0xB79B, 0xA612, 0xD2AD, 0xC324, 0xF1BF, 0xE036, 0x18C1, 0x0948, 0x3BD3, 0x2A5A,
        0x5EE5, 0x4F6C, 0x7DF7, 0x6C7E, 0xA50A, 0xB483, 0x8618, 0x9791, 0xE32E, 0xF2A7, 0xC03C, 0xD1B5,
        0x2942, 0x38CB, 0x0A50, 0x1BD9, 0x6F66, 0x7EEF, 0x4C74, 0x5DFD, 0xB58B, 0xA402, 0x9699, 0x8710,
        0xF3AF, 0xE226, 0xD0BD, 0xC134, 0x39C3, 0x284A, 0x1AD1, 0x0B58, 0x7FE7, 0x6E6E, 0x5CF5, 0x4D7C,
        0xC60C, 0xD785, 0xE51E, 0xF497, 0x8028, 0x91A1, 0xA33A, 0xB2B3, 0x4A44, 0x5BCD, 0x6956, 0x78DF,
        0x0C60, 0x1DE9, 0x2F72, 0x3EFB, 0xD68D, 0xC704, 0xF59F, 0xE416, 0x90A9, 0x8120, 0xB3BB, 0xA232,
        0x5AC5, 0x4B4C, 0x79D7, 0x685E, 0x1CE1, 0x0D68, 0x3FF3, 0x2E7A, 0xE70E, 0xF687, 0xC41C, 0xD595,
        0xA12A, 0xB0A3, 0x8238, 0x93B1, 0x6B46, 0x7ACF, 0x4854, 0x59DD, 0x2D62, 0x3CEB, 0x0E70, 0x1FF9,
        0xF78F, 0xE606, 0xD49D, 0xC514, 0xB1AB, 0xA022, 0x92B9, 0x8330, 0x7BC7, 0x6A4E, 0x58D5, 0x495C,
        0x3DE3, 0x2C6A, 0x1EF1, 0x0F78
    };

    private static short getCrc16(byte[] pData) {
        short fcs = (short) 0xFFFF; // initialization
        int i = 0;

        while (pData.length > i) {
            fcs = (short) ((fcs >> 8) ^ crctab16[(fcs ^ pData[i]) & 0xFF]);
            i++;
        }

        fcs = (short) ~fcs;
        return fcs; // negated
    }
}
