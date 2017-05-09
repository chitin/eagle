package org.apache.eagle.security.hdfs;

import org.apache.eagle.app.spi.AbstractApplicationProvider;

/**
 * @Since 4/20/17
 */
public class HdfsAuditLogBeamAppProvider extends AbstractApplicationProvider<HdfsAuditLogBeamApplication> {
    public HdfsAuditLogBeamApplication getApplication() {
        return new HdfsAuditLogBeamApplication();
    }
}
