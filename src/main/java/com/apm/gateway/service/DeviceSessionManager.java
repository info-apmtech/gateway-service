package com.apm.gateway.service;

import com.apm.gateway.data.DeviceCommand;
import com.apm.gateway.data.DeviceSession;

import jakarta.enterprise.context.ApplicationScoped;

import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class DeviceSessionManager {

    private static final Logger LOG = Logger.getLogger(DeviceSessionManager.class);
    private static final int COMMAND_TIMEOUT_SECONDS = 50;

    private final ConcurrentHashMap<Socket, DeviceSession> sessions = new ConcurrentHashMap<>();
    /**
     * Secondary index for O(1) IMEI lookup and duplicate-connection detection.
     */
    private final ConcurrentHashMap<String, Socket> imeiIndex = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, CompletableFuture<String>> pendingTextResponses = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<byte[]>> pendingBinaryResponses = new ConcurrentHashMap<>();

    public int getCount() {
        return sessions.size();
    }

    /**
     * Register or update a session for the given socket. If another socket
     * already holds the same IMEI, that old connection is closed.
     */
    public void registerOrUpdate(Socket client, String imei) {
        try {
            Socket oldClient = imeiIndex.get(imei);
            if (oldClient != null && oldClient != client) {
                sessions.remove(oldClient);
                try {
                    oldClient.close();
                } catch (Exception ignored) {
                }
                LOG.infof("[Session] Closed duplicate connection for IMEI: %s", imei);
            }

            DeviceSession session = sessions.computeIfAbsent(client, k -> new DeviceSession(client));
            session.setImei(imei);
            session.setLastSeen(Instant.now());
            imeiIndex.put(imei, client);
        } catch (Exception ex) {
            LOG.errorf(ex, "[Session] Error registering/updating session for client: %s",
                    client.getRemoteSocketAddress());
        }
    }

    public Optional<String> tryGetImei(Socket client) {
        DeviceSession session = sessions.get(client);
        if (session != null) {
            return Optional.ofNullable(session.getImei());
        }
        return Optional.empty();
    }

    public boolean trySendCommand(String imei, byte[] command) {
        DeviceSession session = findSessionByImei(imei);
        if (session != null && session.getClient() != null && session.getClient().isConnected()) {
            try {
                OutputStream stream = session.getClient().getOutputStream();
                stream.write(command);
                stream.flush();
                return true;
            } catch (IOException e) {
                LOG.errorf(e, "[Session] Error sending command to IMEI: %s", imei);
                return false;
            }
        }
        return false;
    }

    /**
     * Send a command to the device and await its response.
     *
     * <p>
     * For devicetype 3 (GT06 binary): builds a proper 0x80 command packet and
     * waits for {@link #notifyBinaryResponse} to be called from the main read
     * loop.
     *
     * <p>
     * For all other types: sends the raw commandHex bytes as UTF-8 and waits
     * for {@link #notifyTextResponse} to be called from the main read loop.
     */
    public DeviceCommand sendCommandAndReadResponse(DeviceCommand command) {
        DeviceSession session = findSessionByImei(command.getImei());

        if (session == null || session.getClient() == null || !session.getClient().isConnected()) {
            command.setResponse("Device Not connected");
            return command;
        }

        try {
            OutputStream outputStream = session.getClient().getOutputStream();

            if (command.getDevicetype() == 3) {
                CompletableFuture<byte[]> future = new CompletableFuture<>();
                pendingBinaryResponses.put(command.getImei(), future);
                try {
                    byte[] packetBytes = buildGt06CommandPacket(command, session);
                    outputStream.write(packetBytes);
                    outputStream.flush();
                    LOG.infof("[GT06] Command packet: %s", bytesToHex(packetBytes));

                    byte[] responseBytes = future.get(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    command.setResponse(parseGt06Response(responseBytes));
                } catch (TimeoutException | CancellationException e) {
                    command.setResponse("Timeout waiting for device response");
                } finally {
                    pendingBinaryResponses.remove(command.getImei());
                }
            } else {
                CompletableFuture<String> future = new CompletableFuture<>();
                pendingTextResponses.put(command.getImei(), future);
                try {
                    byte[] requestBytes = command.getCommandHex().getBytes(StandardCharsets.UTF_8);
                    outputStream.write(requestBytes);
                    outputStream.flush();
                    LOG.infof("[AIS] Command sent to %s: %s", command.getImei(), command.getCommandHex());

                    String response = future.get(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    command.setResponse(response);
                } catch (TimeoutException | CancellationException e) {
                    command.setResponse("Timeout waiting for device response");
                } finally {
                    pendingTextResponses.remove(command.getImei());
                }
            }
        } catch (Exception ex) {
            command.setResponse(ex.getMessage());
        }

        return command;
    }

    /**
     * Called by the main TCP read loop when the device sends a text/ASCII
     * packet and a command is pending for this IMEI.
     */
    public void notifyTextResponse(String imei, String response) {
        try {
            CompletableFuture<String> future = pendingTextResponses.get(imei);
            if (future != null) {
                future.complete(response);
            }
        } catch (Exception ignored) {
        }
    }

    /**
     * Called by the main TCP read loop when the device sends a binary GT06
     * command-response packet (protocol 0x15 / 0x21 / 0x80) and a command is
     * pending for this IMEI.
     */
    public void notifyBinaryResponse(String imei, byte[] response) {
        try {
            CompletableFuture<byte[]> future = pendingBinaryResponses.get(imei);
            if (future != null) {
                future.complete(response);
            }
        } catch (Exception ignored) {
        }
    }

    public void updateLastSeen(Socket client) {
        try {
            DeviceSession session = sessions.get(client);
            if (session != null) {
                session.setLastSeen(Instant.now());
            }
        } catch (Exception ex) {
            LOG.errorf(ex, "[Session] Error updating last seen for client: %s",
                    client.getRemoteSocketAddress());
        }
    }

    /**
     * Updates last-seen timestamp and also records the GT06 serial number from
     * the packet.
     */
    public void updateLastSeen(Socket client, short serial) {
        try {
            DeviceSession session = sessions.get(client);
            if (session != null) {
                session.setLastSeen(Instant.now());
                session.setLastSerial(serial);
            }
        } catch (Exception ex) {
            LOG.errorf(ex, "[Session] Error updating last seen for client: %s",
                    client.getRemoteSocketAddress());
        }
    }

    public void remove(Socket client) {
        try {
            DeviceSession session = sessions.remove(client);
            if (session != null && session.getImei() != null) {
                imeiIndex.remove(session.getImei(), client);
            }
            LOG.infof("[Session] Client disconnected and removed: %s", client.getRemoteSocketAddress());
        } catch (Exception ex) {
            LOG.errorf(ex, "[Session] Error removing client in session: %s",
                    client.getRemoteSocketAddress());
        }
    }

    public int getConnectedDeviceCount() {
        return (int) sessions.values().stream()
                .filter(s -> s.getClient() != null && s.getClient().isConnected()
                && s.getImei() != null && !s.getImei().isEmpty())
                .count();
    }

    public List<String> getConnectedImeis() {
        return sessions.values().stream()
                .filter(s -> s.getClient() != null && s.getClient().isConnected()
                && s.getImei() != null && !s.getImei().isEmpty())
                .map(DeviceSession::getImei)
                .collect(Collectors.toList());
    }

    public void sweepIdleClients(Duration maxIdle) {
        try {
            Instant now = Instant.now();
            List<Socket> toRemove = new ArrayList<>();

            for (var entry : sessions.entrySet()) {
                DeviceSession session = entry.getValue();
                if (Duration.between(session.getLastSeen(), now).compareTo(maxIdle) > 0) {
                    toRemove.add(entry.getKey());
                }
            }

            for (Socket socket : toRemove) {
                try {
                    DeviceSession session = sessions.get(socket);
                    if (session != null) {
                        if (session.getImei() != null) {
                            imeiIndex.remove(session.getImei(), socket);
                        }
                        socket.close();
                        sessions.remove(socket);
                        LOG.infof("[Sweeper] Disconnected idle client with IMEI: %s", session.getImei());
                    }
                } catch (Exception ex) {
                    LOG.errorf(ex, "[Sweeper] Error closing socket");
                }
            }
        } catch (Exception ex) {
            LOG.errorf(ex, "[Sweeper] Error during idle client cleanup");
        }
    }

    // -------------------------------------------------------------------------
    // GT06 helpers
    // -------------------------------------------------------------------------
    /**
     * Builds a GT06 protocol-0x80 (server command) packet.
     *
     * <pre>
     * Packet = 0x78 0x78
     *          PacketLength (1)
     *          Protocol     (0x80)
     *          InfoContent:
     *            LengthOfCommand (1) = 4 + M
     *            ServerFlag      (4) = 0x00 0x00 0x00 0x00
     *            CommandContent  (M) = ASCII bytes of commandHex
     *          Serial           (2)
     *          CRC16            (2) over [PacketLength..Serial]
     *          0x0D 0x0A
     * </pre>
     */
    private byte[] buildGt06CommandPacket(DeviceCommand command, DeviceSession session) {
        byte[] commandText = command.getCommandHex().getBytes(StandardCharsets.US_ASCII);
        byte[] serverFlag = {0x00, 0x00, 0x00, 0x00};
        byte lengthOfCommand = (byte) (serverFlag.length + commandText.length);

        short nextSerial = (short) (session.getLastSerial() + 1);
        session.setLastSerial(nextSerial);
        byte[] serialBytes = {(byte) ((nextSerial >> 8) & 0xFF), (byte) (nextSerial & 0xFF)};

        // Information Content: LengthOfCommand(1) + ServerFlag(4) + CommandContent(M)
        List<Byte> infoContent = new ArrayList<>();
        infoContent.add(lengthOfCommand);
        for (byte b : serverFlag) {
            infoContent.add(b);
        }
        for (byte b : commandText) {
            infoContent.add(b);
        }
        byte[] info = toByteArray(infoContent);

        byte protocolId = (byte) 0x80;
        // Packet Length: Protocol(1) + InfoContent(N) + Serial(2) + CRC(2)
        byte packetLength = (byte) (1 + info.length + 2 + 2);

        // CRC over: PacketLength + Protocol + InfoContent + Serial
        List<Byte> crcInputList = new ArrayList<>();
        crcInputList.add(packetLength);
        crcInputList.add(protocolId);
        for (byte b : info) {
            crcInputList.add(b);
        }
        for (byte b : serialBytes) {
            crcInputList.add(b);
        }
        byte[] crcInput = toByteArray(crcInputList);
        short crc = getCrc16ForCommand(crcInput);

        // Final packet: 0x78 0x78 + crcInput + crcHigh + crcLow + 0x0D 0x0A
        List<Byte> packet = new ArrayList<>();
        packet.add((byte) 0x78);
        packet.add((byte) 0x78);
        for (byte b : crcInput) {
            packet.add(b);
        }
        packet.add((byte) ((crc >> 8) & 0xFF));
        packet.add((byte) (crc & 0xFF));
        packet.add((byte) 0x0D);
        packet.add((byte) 0x0A);

        return toByteArray(packet);
    }

    /**
     * Extracts the ASCII payload from a GT06 binary command-response frame and
     * strips control characters.
     *
     * <pre>
     * Frame: 0x78 0x78 | Length | Protocol | [4-byte flag] | Payload | Serial | CRC | 0x0D 0x0A
     * payloadStart = 5 (skip start(2) + length(1) + protocol(1) + flag-byte(1))
     * payloadEnd   = frame.length - 6 (skip serial(2) + crc(2) + end(2))
     * </pre>
     */
    private String parseGt06Response(byte[] data) {
        if (data == null || data.length < 10) {
            return "Invalid packet";
        }

        int payloadStart = 5;
        int payloadLength = data.length - payloadStart - 6;

        if (payloadLength <= 0) {
            return "No payload";
        }

        byte[] payload = new byte[payloadLength];
        System.arraycopy(data, payloadStart, payload, 0, payloadLength);

        String raw = new String(payload, StandardCharsets.US_ASCII);
        return raw.chars()
                .filter(c -> !Character.isISOControl(c))
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString()
                .trim();
    }

    /**
     * CRC-16 (bit-by-bit, poly 0x8408, init 0xFFFF) — matches
     * {@code DeviceSessionManager.GetCrc16} in the C# reference (no final
     * negation).
     */
    private static short getCrc16ForCommand(byte[] data) {
        int crc = 0xFFFF;
        for (byte b : data) {
            crc ^= (b & 0xFF);
            for (int i = 0; i < 8; i++) {
                if ((crc & 0x0001) != 0) {
                    crc = (crc >>> 1) ^ 0x8408;
                } else {
                    crc >>>= 1;
                }
            }
        }
        return (short) (crc & 0xFFFF);
    }

    // -------------------------------------------------------------------------
    // Misc helpers
    // -------------------------------------------------------------------------
    private DeviceSession findSessionByImei(String imei) {
        Socket socket = imeiIndex.get(imei);
        if (socket != null) {
            DeviceSession s = sessions.get(socket);
            if (s != null) {
                return s;
            }
        }
        // Fallback linear scan (handles index/sessions out-of-sync edge cases)
        return sessions.values().stream()
                .filter(s -> imei.equals(s.getImei()))
                .findFirst()
                .orElse(null);
    }

    public static byte[] hexStringToBytesSafe(String hex) {
        try {
            if (hex == null || hex.isBlank()) {
                return new byte[0];
            }
            hex = hex.replace(" ", "");
            if (hex.length() % 2 != 0) {
                throw new IllegalArgumentException("Invalid hex string length");
            }
            byte[] bytes = new byte[hex.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            }
            return bytes;
        } catch (Exception e) {
            return new byte[0];
        }
    }

    private static byte[] toByteArray(List<Byte> list) {
        byte[] arr = new byte[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = list.get(i);
        }
        return arr;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }
}
