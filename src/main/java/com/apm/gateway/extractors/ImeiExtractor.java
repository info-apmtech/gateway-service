package com.apm.gateway.extractors;

import java.util.Optional;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
public interface ImeiExtractor {

    Optional<String> tryExtractImei(byte[] packet);
}
