package com.apm.gateway.extractors;

import com.apm.gateway.data.ImeiExtractor;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import java.util.Optional;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class GT06ImeiExtractor implements ImeiExtractor {

    private static final Logger LOG = Logger.getLogger(GT06ImeiExtractor.class);

    @Override
    public Optional<String> tryExtractImei(byte[] packet) {

        try {
            if (packet.length >= 12 && packet[3] == 0x01
                    && ((packet[0] == 0x78 && packet[1] == 0x78)
                    || (packet[0] == 0x79 && packet[1] == 0x79))) {

                byte[] imeiBytes = new byte[8];
                System.arraycopy(packet, 4, imeiBytes, 0, 8);

                StringBuilder bcd = new StringBuilder();
                for (byte b : imeiBytes) {
                    bcd.append(String.format("%02X", b));
                }

                String imei = bcd.toString().replaceAll("[^0-9]", "");
                imei = imei.replaceFirst("^0+", "");

                if (imei.length() >= 14) {
                    return Optional.of(imei);
                }
            }
        } catch (Exception ex) {
            LOG.errorf(ex, "[GT06ImeiExtractor] Error extracting IMEI from packet %s",
                    bytesToHex(packet));
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
