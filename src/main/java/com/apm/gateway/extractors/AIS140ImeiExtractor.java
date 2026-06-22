package com.apm.gateway.extractors;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class AIS140ImeiExtractor implements ImeiExtractor {

    private static final Logger LOG = Logger.getLogger(AIS140ImeiExtractor.class);

    @Override
    public Optional<String> tryExtractImei(byte[] packet) {
        String imei = null;
        try {
            String rxData = new String(packet, StandardCharsets.US_ASCII).replace("\0", "").trim();
            if (rxData.startsWith("$")) {
                String[] splitData = rxData.split(",");

                if (rxData.startsWith("$LGN")) {
                    // Login Pocket Maharashtra
                    if (splitData.length == 10 && splitData[3].length() == 15) {
                        imei = splitData[3];
                    } // Login Pocket Odisha
                    else if (splitData.length == 8 && splitData[2].length() == 15) {
                        imei = splitData[2];
                    } // Login Pocket NIC
                    else if (splitData.length == 7 && splitData[2].length() == 15) {
                        imei = splitData[2];
                    }
                } else {
                    // Login Pocket APMKT
                    String[] loginPacketData = rxData.split("\\$");
                    if (rxData.length() > 10 && loginPacketData.length == 12) {
                        imei = loginPacketData[2];
                    } else {
                        // Normal Message
                        if ("$".equals(splitData[0]) && splitData.length >= 49) {
                            imei = splitData[7];
                        } // Normal Message Maharashtra
                        else if ("$NMP".equals(splitData[0])) {
                            imei = splitData[6];
                        } // Normal Message Odisha
                        else if (splitData.length >= 48 && splitData[splitData.length - 1].length() == 9
                                && !rxData.contains("(")) {
                            imei = splitData[6];
                        } // Normal Message NIC
                        else if (splitData.length >= 48 && splitData[splitData.length - 1].length() > 1
                                && !rxData.contains("(")) {
                            imei = splitData[6];
                        } // Health Packet KT
                        else if (splitData.length == 14 && "101".equals(splitData[1])) {
                            imei = splitData[4];
                        } // Health Packet MH
                        else if (splitData.length == 11 && "$HLP".equals(splitData[0])) {
                            imei = splitData[3];
                        } // Health Packet NIC or Odisha
                        else if (splitData.length == 11 && "$HEL".equals(splitData[0])) {
                            imei = splitData[3];
                        } // iTriangle_Bh101
                        else if (splitData[0].contains("$Header")) {
                            if (splitData.length <= 10) { // login
                                imei = splitData[3];
                            } else if (splitData.length <= 12) { // health
                                imei = splitData[3];
                            } else if (splitData.length > 50) { // Normal
                                imei = splitData[6];
                            }
                        } // Emergency Packet KT
                        else if ("EPB".equals(splitData[1])) {
                            imei = splitData[3];
                        } else if ("$EPB".equals(splitData[0])) {
                            imei = splitData[2];
                        } // Sensor Packet
                        else if ("$SENS".equals(splitData[0])) {
                            imei = splitData[1];
                        } else if (rxData.contains("(")) {
                            imei = splitData[6];
                        }
                    }
                }
            }

            if (imei != null && imei.length() >= 15) {
                return Optional.of(imei.trim());
            }
        } catch (Exception ex) {
            LOG.errorf(ex, "[AIS140ImeiExtractor] Error extracting IMEI from packet %s data Str: %s",
                    bytesToHex(packet), new String(packet, StandardCharsets.US_ASCII));
        }

        return Optional.empty();
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X-", b));
        }
        return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
    }
}
