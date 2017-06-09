package org.apache.eagle.security.beam.parser;

import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Since 4/25/17.
 */
public class HBaseAuditLogKafkaDeseralizer implements SpoutKafkaMessageDeserializer {
    private static Logger LOG = LoggerFactory.getLogger(HBaseAuditLogKafkaDeseralizer.class);
    @Override
    public Object deserialize(byte[] arg0) {
        String logLine = new String(arg0);
        HbaseAuditLogParser parser = new HbaseAuditLogParser();
        HbaseAuditLogObject entity = null;
        try {
            entity = parser.parse(logLine);
        } catch (Exception ex) {
            LOG.error("Failing HBase parse audit log message" + ex);
        }
        if (entity == null) {
            LOG.warn("Event ignored as it can't be correctly parsed, the log is " + logLine);
            return null;
        }
        Map<String, String> map = new TreeMap<>();
        map.put("action", entity.action);
        map.put("host", entity.host);
        map.put("status", entity.status);
        map.put("request", entity.request);
        map.put("scope", entity.scope);
        map.put("user", entity.user);
        map.put("timestamp", String.valueOf(entity.timestamp));
        return map;
    }
}
