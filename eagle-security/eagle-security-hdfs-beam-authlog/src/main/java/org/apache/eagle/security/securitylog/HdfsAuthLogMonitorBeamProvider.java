package org.apache.eagle.security.securitylog;

import org.apache.eagle.app.spi.AbstractApplicationProvider;

/**
 * Since 4/25/17.
 */
public class HdfsAuthLogMonitorBeamProvider extends AbstractApplicationProvider<HdfsAuthLogMonitoringBeamMain> {

    @Override
    public HdfsAuthLogMonitoringBeamMain getApplication() {
        return new HdfsAuthLogMonitoringBeamMain();
    }
}
