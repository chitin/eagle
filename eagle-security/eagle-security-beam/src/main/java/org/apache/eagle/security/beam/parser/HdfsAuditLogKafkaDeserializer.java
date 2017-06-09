package org.apache.eagle.security.beam.parser;

import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer;
import org.apache.eagle.security.hdfs.HDFSAuditLogObject;
import org.apache.eagle.security.hdfs.HDFSAuditLogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Since 4/20/17
 */
public class HdfsAuditLogKafkaDeserializer implements SpoutKafkaMessageDeserializer {
    private static Logger LOG = LoggerFactory.getLogger(HdfsAuditLogKafkaDeserializer.class);
    private static final String DATASOURCE_TIMEZONE_PATH = "dataSourceConfig.timeZone";
    private Properties props;
    private Config config;
    private HDFSAuditLogParser parser;

    public HdfsAuditLogKafkaDeserializer(Config config) {
        this.config = config;
    }

    @Override
    public Object deserialize(byte[] arg0) {
        String logLine = new String(arg0);
        HDFSAuditLogObject entity = null;

        if(config.hasPath(DATASOURCE_TIMEZONE_PATH)) {
            TimeZone timeZone = TimeZone.getTimeZone(config.getString(DATASOURCE_TIMEZONE_PATH));
            parser = new HDFSAuditLogParser(timeZone);
        } else {
            parser = new HDFSAuditLogParser();
        }

        try {
            entity = parser.parse(logLine);
        } catch (Exception e) {
            LOG.error("Failing hdfs parse audit log message" + e);
        }

        if(entity == null) {
            LOG.warn("Event ignored as it can't be correctly parsed, the log is " + logLine);
            return null;
        }

        Map<String, Object> map = new TreeMap<>();
        map.put(HDFSAuditLogObject.HDFS_SRC_KEY, entity.src);
        map.put(HDFSAuditLogObject.HDFS_DST_KEY, entity.dst);
        map.put(HDFSAuditLogObject.HDFS_HOST_KEY, entity.host);
        map.put(HDFSAuditLogObject.HDFS_TIMESTAMP_KEY, entity.timestamp);
        map.put(HDFSAuditLogObject.HDFS_ALLOWED_KEY, entity.allowed);
        map.put(HDFSAuditLogObject.HDFS_USER_KEY, entity.user);
        map.put(HDFSAuditLogObject.HDFS_CMD_KEY, entity.cmd);
        return map;
    }
}
