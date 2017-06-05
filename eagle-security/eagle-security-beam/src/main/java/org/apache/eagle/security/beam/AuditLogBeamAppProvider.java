package org.apache.eagle.security.beam;

import org.apache.eagle.app.spi.AbstractApplicationProvider;

/**
 * @Since 4/20/17
 */
public class AuditLogBeamAppProvider extends AbstractApplicationProvider<AuditLogBeamApplication> {
    public AuditLogBeamApplication getApplication() {
        return new AuditLogBeamApplication();
    }
}
