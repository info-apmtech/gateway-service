package com.apm.gateway.extractors;

import com.apm.gateway.data.ImeiExtractor;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import java.util.Optional;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class TransyncImeiExtractor implements ImeiExtractor {

    private static final Logger LOG = Logger.getLogger(TransyncImeiExtractor.class);

    @Override
    public Optional<String> tryExtractImei(byte[] packet) {
        try {
            // Check for the IMEI prefix (0x3A 0x3A) and ensure the packet is large enough
            if (packet.length >= 12 && packet[0] == 0x3A && packet[1] == 0x3A) {
                // Assuming the IMEI starts at index 5 and is 8 bytes long (BCD)
                byte[] imeiBytes = new byte[8];
                System.arraycopy(packet, 5, imeiBytes, 0, 8);

                // Convert BCD bytes to a string
                String imei = bcdToString(imeiBytes);

                // Trim leading zeroes and validate IMEI length (14 digits is the minimum required for valid IMEI)
                imei = imei.replaceFirst("^0+", "");

                // IMEI should be 15 digits long
                if (imei.length() == 15) {
                    return Optional.of(imei);
                }
            }
        } catch (Exception ex) {
            LOG.errorf(ex, "[TransyncImeiExtractor] Error extracting IMEI from packet %s",
                    bytesToHex(packet));
        }

        return Optional.empty();
    }

    // Helper method to convert BCD (Binary Coded Decimal) to string
    private String bcdToString(byte[] bcdBytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bcdBytes) {
            sb.append(String.format("%d%d", (b >> 4) & 0x0F, b & 0x0F));
        }
        return sb.toString();
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X-", b));
        }
        return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
    }
}
