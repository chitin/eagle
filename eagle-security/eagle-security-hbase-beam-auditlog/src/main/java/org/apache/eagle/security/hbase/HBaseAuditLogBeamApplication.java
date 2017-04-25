package org.apache.eagle.security.hbase;

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
import org.apache.eagle.security.service.HBaseSensitivityEntity;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Since 4/25/17.
 */
public class HBaseAuditLogBeamApplication extends BeamApplication {
    private static Logger LOG = LoggerFactory.getLogger(HBaseAuditLogBeamApplication.class);

    private static final String DEFAULT_CONFIG_PREFIX = "dataSourceConfig";

    private String configPrefix = DEFAULT_CONFIG_PREFIX;
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
        Map<String, String> consumerProps = ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
        String topic = context.getString("topic");
        String zkConnection = context.getString("zkConnection");

        String sinkBrokerList = config.getString("dataSinkConfig.brokerList");
        String sinkTopic = config.getString("dataSinkConfig.topic");

        Kafka8IO.Read<String, String> read = Kafka8IO.<String, String>read()
            .withBootstrapServers(zkConnection)
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
        Pipeline p = Pipeline.create(options);

        DataEnrichLCM lcm = new HBaseSensitivityDataEnrichLCM(config);
        dataRetrieval(lcm, config, p);

        PCollection<KV<String, String>> deduped =
            p.apply(read.withoutMetadata()).apply(ParDo.of(new ExtractLogFn()))
                .apply(ParDo.of(new SensitivityDataEnrichFn(lcm)));

        deduped.apply(Kafka8IO.<String, String>write()
            .withBootstrapServers(sinkBrokerList)
            .withTopic(sinkTopic)
            .withKeyCoder(StringUtf8Coder.of())
            .withValueCoder(StringUtf8Coder.of()).updateProducerProperties(ImmutableMap.of("bootstrap.servers", sinkBrokerList)));
        return p;
    }

    private static class ExtractLogFn extends DoFn<KV<String, String>, KV<String, Map<String, String>>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {
            String log = c.element().getValue();
            LOG.info("--------------------log " + log);
            Map<String, String> map = (Map<String, String>) new HBaseAuditLogKafkaDeseralizer().deserialize(log.getBytes("UTF-8"));
            c.output(KV.of("f1", map));
        }
    }

    private static class SensitivityDataEnrichFn extends DoFn<KV<String, Map<String, String>>, KV<String, String>> {
        private DataEnrichLCM lcm;

        public SensitivityDataEnrichFn(DataEnrichLCM lcm) {
            this.lcm = lcm;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {
            Map<String, String> toBeCopied = c.element().getValue();
            Map<String, String> eventMap = new TreeMap<>(toBeCopied);

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
            String message = Arrays.asList(eventMap).get(0).toString();
            c.output(KV.of((String) eventMap.get("user"), message));
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
