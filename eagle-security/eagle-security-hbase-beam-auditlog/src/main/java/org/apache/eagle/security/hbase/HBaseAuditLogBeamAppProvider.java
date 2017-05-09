package org.apache.eagle.security.hbase;

import org.apache.eagle.app.spi.AbstractApplicationProvider;

/**
 * Since 4/25/17.
 */
public class HBaseAuditLogBeamAppProvider extends AbstractApplicationProvider<HBaseAuditLogBeamApplication> {
    @Override
    public HBaseAuditLogBeamApplication getApplication() {
        return new HBaseAuditLogBeamApplication();
    }
}
