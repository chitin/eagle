package org.apache.eagle.security.hdfs;

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
import org.apache.eagle.security.enrich.DataEnrichLCM;
import org.apache.eagle.security.enrich.ExternalDataCache;
import org.apache.eagle.security.enrich.ExternalDataJoiner;
import org.apache.eagle.security.hdfs.util.SimplifyPath;
import org.apache.eagle.security.service.HdfsSensitivityEntity;
import org.apache.eagle.security.service.IPZoneEntity;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Since 4/20/17
 */
public class HdfsAuditLogBeamApplication extends BeamApplication {
    private static Logger LOG = LoggerFactory.getLogger(HdfsAuditLogBeamApplication.class);

    private static final String DEFAULT_CONFIG_PREFIX = "dataSourceConfig";
    private final static String TRAFFIC_MONITOR_ENABLED = "dataSinkConfig.trafficMonitorEnabled";

    private String configPrefix = DEFAULT_CONFIG_PREFIX;
    private static String DEFAULT_CONSUMER_GROUP_ID = "eagleConsumers";
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
        String topic = context.getString("topic");
        String zkConnString = context.getString("zkConnection");
        String sinkBrokerList = config.getString("dataSinkConfig.brokerList");
        String sinkTopic = config.getString("dataSinkConfig.topic");

        Kafka8IO.Read<String, String> read = Kafka8IO.<String, String>read()
            .withBootstrapServers(zkConnString)
            .withTopics(Collections.singletonList(topic))
            .withKeyCoder(StringUtf8Coder.of())
            .withValueCoder(StringUtf8Coder.of())
            .updateKafkaClusterProperties(consumerProps);

        SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
        Duration batchIntervalDuration = Duration.standardSeconds(5);
        options.setBatchIntervalMillis(batchIntervalDuration.getMillis());
        options.setMinReadTimeMillis(batchIntervalDuration.minus(1).getMillis());
        options.setMaxRecordsPerBatch(8L);
        options.setRunner(SparkRunner.class);
        if(config.hasPath("sparkRunner.checkpoint"))
        {
            options.setCheckpointDir(config.getString("sparkRunner.checkpoint"));
        }
        if(config.hasPath("sparkRunner.master"))
        {
            options.setSparkMaster(config.getString("sparkRunner.master"));
        }
        Pipeline p = Pipeline.create(options);

        // start external data retrieval
        DataEnrichLCM lcm = new HdfsSensitivityDataEnrichLCM(config);
        dataRetrieval(lcm, config, p);

        PCollection<KV<String, Map<String, String>>> deduped =
            p.apply(read.withoutMetadata()).apply(ParDo.of(new ExtractLogFn(config)))
                .apply(ParDo.of(new SensitivityDataEnrichFn(lcm)));

        if (config.hasPath(TRAFFIC_MONITOR_ENABLED) && config.getBoolean(TRAFFIC_MONITOR_ENABLED)) {
            //TODO HadoopLogAccumulator
        }
        deduped.apply(ParDo.of(new IPZoneDataEnrichFn(lcm)))
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
            LOG.info("--------------------log " + log);
            Map<String, String> map = (Map<String, String>) new HdfsAuditLogKafkaDeserializer(config).deserialize(log.getBytes("UTF-8"));
//            String message = Arrays.asList(map).get(0).toString();
            c.output(KV.of("f1", map));
        }
    }

    private static class SensitivityDataEnrichFn extends DoFn<KV<String, Map<String, String>>, KV<String, Map<String, String>>> {
        private DataEnrichLCM lcm;

        public SensitivityDataEnrichFn(DataEnrichLCM lcm) {
            this.lcm = lcm;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {
            Map<String, String> toBeCopied = c.element().getValue();
            Map<String, String> eventMap = new TreeMap<>(toBeCopied);
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
            c.output(KV.of(eventMap.get("user"), eventMap));
        }
    }

    private static class IPZoneDataEnrichFn extends DoFn<KV<String, Map<String, String>>, KV<String, String>> {
        private DataEnrichLCM lcm;

        public IPZoneDataEnrichFn(DataEnrichLCM lcm) {
            this.lcm = lcm;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {
            Map<String, String> toBeCopied = c.element().getValue();
            Map<String, String> eventMap = new TreeMap<>(toBeCopied);
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
        }
    }

    private void dataRetrieval(DataEnrichLCM lcm, Config config, Pipeline p) {
        try {
            ExternalDataJoiner joiner = new ExternalDataJoiner(lcm, config, p.toString());
            joiner.start();
        } catch (Exception ex) {
            LOG.error("Fail bringing up quartz scheduler." + ex);
            throw new IllegalStateException(ex);
        }
    }
}
