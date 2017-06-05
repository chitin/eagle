package org.apache.eagle.security.beam;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka8.Kafka8IO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.eagle.app.BeamApplication;
import org.apache.eagle.app.environment.impl.BeamEnviroment;
import org.apache.eagle.security.beam.dataEnrich.HBaseSensitivityDataEnrichLCM;
import org.apache.eagle.security.beam.dataEnrich.IPZoneDataEnrichLCM;
import org.apache.eagle.security.beam.parser.HBaseAuditLogKafkaDeseralizer;
import org.apache.eagle.security.beam.parser.HdfsAuditLogKafkaDeserializer;
import org.apache.eagle.security.beam.dataEnrich.HdfsSensitivityDataEnrichLCM;
import org.apache.eagle.security.beam.parser.OozieAuditLogKafkaDeserializer;
import org.apache.eagle.security.enrich.DataEnrichLCM;
import org.apache.eagle.security.enrich.ExternalDataCache;
import org.apache.eagle.security.enrich.ExternalDataJoiner;
import org.apache.eagle.security.beam.util.SimplifyPath;
import org.apache.eagle.security.service.HBaseSensitivityEntity;
import org.apache.eagle.security.service.HdfsSensitivityEntity;
import org.apache.eagle.security.service.IPZoneEntity;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Since 4/20/17
 */
public class AuditLogBeamApplication extends BeamApplication {
    private static Logger LOG = LoggerFactory.getLogger(AuditLogBeamApplication.class);

    private static final String DEFAULT_CONFIG_PREFIX = "dataSourceConfig";
    private static final String TRAFFIC_MONITOR_ENABLED = "dataSinkConfig.trafficMonitorEnabled";
    private String configPrefix = DEFAULT_CONFIG_PREFIX;
    private static String DEFAULT_CONSUMER_GROUP_ID = "eagleConsumers";

    private static final String OOZIE_SYMBOL = "oozieaudit";
    private static final String HDFS_SYMBOL = "FSNamesystem.audit";
    private static final String HBASE_SYMBOL = "SecurityLogger";
    private static final String OOZIE_FLAG = "oozie";
    private static final String HDFS_FLAG = "hdfs";
    private static final String HBASE_FLAG = "hbase";

    private SparkPipelineResult res;
    public SparkPipelineResult getRes() {
        return res;
    }
    public void setRes(SparkPipelineResult res) {
        this.res = res;
    }

    @Override
    public Pipeline execute(Config config, BeamEnviroment environment) {
        Config context = config;
        if (this.configPrefix != null) {
            context = config.getConfig(configPrefix);
        }
        Map<String, String> consumerProps = ImmutableMap.<String, String>of(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("autoOffsetResetConfig"),
            "metadata.broker.list", config.getString("dataSinkConfig.brokerList"),
            "group.id", context.hasPath("consumerGroupId") ? context.getString("consumerGroupId") : DEFAULT_CONSUMER_GROUP_ID
        );
        String topics = context.getString("topicList");
        String zkConnString = context.getString("ZkConnection");
        String sinkBrokerList = config.getString("dataSinkConfig.brokerList");
        String sinkTopic = config.getString("dataSinkConfig.topic");

        Kafka8IO.Read<String, String> read = Kafka8IO.<String, String>read()
            .withBootstrapServers(zkConnString)
            .withTopics(Arrays.asList(topics.split(",")))
            .withKeyCoder(StringUtf8Coder.of())
            .withValueCoder(StringUtf8Coder.of())
            .updateKafkaClusterProperties(consumerProps);

        SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
        Duration batchIntervalDuration = Duration.standardSeconds(5);
        options.setBatchIntervalMillis(batchIntervalDuration.getMillis());
        options.setMinReadTimeMillis(batchIntervalDuration.minus(1).getMillis());
        options.setMaxRecordsPerBatch(8L);
        options.setRunner(SparkRunner.class);
        options.setAppName(config.getString("appId"));
        options.setCheckpointDir(config.getString("sparkRunner.checkpoint"));
        options.setSparkMaster(config.getString("sparkRunner.master"));
        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, Map<String, String>>> deduped =
            p.apply(read.withoutMetadata()).apply(ParDo.of(new ExtractLogFn(config)))
                .apply(ParDo.of(new SensitivityDataEnrichFn(config)));

        if (config.hasPath(TRAFFIC_MONITOR_ENABLED) && config.getBoolean(TRAFFIC_MONITOR_ENABLED)) {
            //TODO HadoopLogAccumulator
        }
        deduped.apply(ParDo.of(new IPZoneDataEnrichFn(config)))
            .apply(Kafka8IO.<String, String>write()
                .withBootstrapServers(sinkBrokerList)
                .withTopic(sinkTopic)
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(StringUtf8Coder.of()).updateProducerProperties(ImmutableMap.of("bootstrap.servers", sinkBrokerList)));
        return p;
    }

    private static class ExtractLogFn extends DoFn<KV<String, String>, KV<String, Map<String, String>>> {
        private Config config;

        public ExtractLogFn(Config config) {
            this.config = config;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {
            String log = c.element().getValue();
            Map<String, String> map;
            LOG.info("--------------------log " + log);
            if(log.contains(OOZIE_SYMBOL)) {
                map = (Map<String, String>) new OozieAuditLogKafkaDeserializer().deserialize(log.getBytes("UTF-8"));
                c.output(KV.of(OOZIE_FLAG, map));
            } else if(log.contains(HDFS_SYMBOL)) {
                map = (Map<String, String>) new HdfsAuditLogKafkaDeserializer(config).deserialize(log.getBytes("UTF-8"));
                c.output(KV.of(HDFS_FLAG, map));
            } else if(log.contains(HBASE_SYMBOL)) {
                map = (Map<String, String>) new HBaseAuditLogKafkaDeseralizer().deserialize(log.getBytes("UTF-8"));
                c.output(KV.of(HBASE_FLAG, map));
            } else {
                LOG.error("Fail to parse log: " + log);
            }
        }
    }

    private static class SensitivityDataEnrichFn extends DoFn<KV<String, Map<String, String>>, KV<String, Map<String, String>>> {
        private DataEnrichLCM lcm;
        private Config config;

        public SensitivityDataEnrichFn(Config config) {
            this.config = config;
        }
        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {
            Map<String, String> toBeCopied = c.element().getValue();
            Map<String, String> eventMap = new TreeMap<>(toBeCopied);
            if(OOZIE_FLAG.equals(c.element().getKey())) {
                c.output(KV.of(OOZIE_FLAG, toBeCopied));
            } else if(HDFS_FLAG.equals(c.element().getKey())) {
                lcm = new HdfsSensitivityDataEnrichLCM(config);
                dataRetrieval(lcm, config, c.timestamp().toString());

                HdfsSensitivityEntity entity = null;

                Map<String, HdfsSensitivityEntity> externalMap = (Map) ExternalDataCache.getInstance().getJobResult(lcm.getClass());

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Receive map: " + externalMap + "event: " + eventMap);
                }

                String src = eventMap.get("src");
                if (externalMap != null && src != null) {
                    String simplifiedPath = new SimplifyPath().build(src);
                    for (String fileDir : externalMap.keySet()) {
                        Pattern pattern = Pattern.compile(simplifiedPath, Pattern.CASE_INSENSITIVE);
                        Matcher matcher = pattern.matcher(fileDir);
                        boolean isMatched = matcher.matches();
                        if (isMatched) {
                            entity = externalMap.get(fileDir);
                            break;
                        }
                    }
                }
                eventMap.put("sensitivityType", entity == null ? "NA" : entity.getSensitivityType());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("After file sensitivity lookup: " + eventMap);
                }
                c.output(KV.of(HDFS_FLAG, eventMap));
            } else if(HBASE_FLAG.equals(c.element().getKey())) {
                lcm = new HBaseSensitivityDataEnrichLCM(config);
                dataRetrieval(lcm, config, c.timestamp().toString());
                HBaseSensitivityEntity entity = null;
                Map<String, HBaseSensitivityEntity> externalMap = (Map) ExternalDataCache.getInstance().getJobResult(lcm.getClass());

                String src = (String) eventMap.get("scope");
                if (externalMap != null && src != "") {
                    for (String key : externalMap.keySet()) {
                        Pattern pattern = Pattern.compile(key, Pattern.CASE_INSENSITIVE);
                        if (pattern.matcher(src).find()) {
                            entity = externalMap.get(key);
                            break;
                        }
                    }
                }
                eventMap.put("sensitivityType", entity == null ? "NA" : entity.getSensitivityType());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("After file sensitivity lookup: " + eventMap);
                }
                c.output(KV.of(HBASE_FLAG, eventMap));
            } else {
                LOG.error("Fail to parse log: " + toBeCopied);
            }

        }
    }

    private static class IPZoneDataEnrichFn extends DoFn<KV<String, Map<String, String>>, KV<String, String>> {
        private Config config;
        private DataEnrichLCM lcm;

        public IPZoneDataEnrichFn(Config config) {
            this.config = config;
        }
        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {
            Map<String, String> toBeCopied = c.element().getValue();
            Map<String, String> eventMap = new TreeMap<>(toBeCopied);

            if(OOZIE_FLAG.equals(c.element().getKey())) {
                String message = Arrays.asList(toBeCopied).get(0).toString();
                c.output(KV.of(eventMap.get("user"), message));
            } else if(HDFS_FLAG.equals(c.element().getKey())) {
                lcm = new IPZoneDataEnrichLCM(config);
                dataRetrieval(lcm, config, c.timestamp().toString());
                Map<String, IPZoneEntity> externalMap = (Map) ExternalDataCache.getInstance().getJobResult(lcm.getClass());
                IPZoneEntity entity = null;
                if (externalMap != null) {
                    entity = externalMap.get("host");
                }
                eventMap.put("securityZone", entity == null ? "NA" : entity.getSecurityZone());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("After IP zone lookup: " + eventMap);
                }
                String message = Arrays.asList(eventMap).get(0).toString();
                c.output(KV.of(eventMap.get("user"), message));
            } else if(HBASE_FLAG.equals(c.element().getKey())) {
                String message = Arrays.asList(toBeCopied).get(0).toString();
                c.output(KV.of(eventMap.get("user"), message));
            } else {
                LOG.error("Fail to parse log: " + toBeCopied);
            }
        }
    }

    private static void dataRetrieval(DataEnrichLCM lcm, Config config, String p) {
        try {
            ExternalDataJoiner joiner = new ExternalDataJoiner(lcm, config, p);
            joiner.start();
        } catch (Exception ex) {
            LOG.error("Fail bringing up quartz scheduler." + ex);
            throw new IllegalStateException(ex);
        }
    }
}
