package org.apache.eagle.security.securitylog;

import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by root on 4/25/17.
 */
public class HdfsAuthLogKafkaDeserializer implements SpoutKafkaMessageDeserializer {
    private static Logger LOG = LoggerFactory.getLogger(HdfsAuthLogKafkaDeserializer.class);
    @Override
    public Object deserialize(byte[] arg0) {
        String logLine = new String(arg0);

        HDFSSecurityLogParser parser = new HDFSSecurityLogParser();
        HDFSSecurityLogObject entity = null;
        try {
            entity = parser.parse(logLine);
        } catch (Exception ex) {
            LOG.error("Failing hdfs parse auth log message", ex);
        }
        if(entity == null) {
            LOG.warn("Event ignored as it can't be correctly parsed, the log is ", logLine);
            return new HashMap<>();
        }

        Map<String, String> map = new TreeMap<>();
        map.put("timestamp", String.valueOf(entity.timestamp));
        map.put("allowed", String.valueOf(entity.allowed));
        map.put("user", entity.user);
        return map;
    }
}
