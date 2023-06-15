/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.StreamingReadMode;
import org.apache.paimon.annotation.Documentation.ExcludeFromDocumentation;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.options.description.TextElement;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.CoreOptions.STREAMING_READ_MODE;
import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.options.description.TextElement.text;

/** Options for flink connector. */
public class FlinkConnectorOptions {

    public static final String NONE = "none";

    public static final ConfigOption<String> LOG_SYSTEM =
            ConfigOptions.key("log.system")
                    .stringType()
                    .defaultValue(NONE)
                    .withDescription(
                            Description.builder()
                                    .text("The log system used to keep changes of the table.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Possible values:")
                                    .linebreak()
                                    .list(
                                            TextElement.text(
                                                    "\"none\": No log system, the data is written only to file store,"
                                                            + " and the streaming read will be directly read from the file store."))
                                    .list(
                                            TextElement.text(
                                                    "\"kafka\": Kafka log system, the data is double written to file"
                                                            + " store and kafka, and the streaming read will be read from kafka. If streaming read from file, configures "
                                                            + STREAMING_READ_MODE.key()
                                                            + " to "
                                                            + StreamingReadMode.FILE.getValue()
                                                            + "."))
                                    .build());

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the sink. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static final ConfigOption<Integer> SCAN_PARALLELISM =
            ConfigOptions.key("scan.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Define a custom parallelism for the scan source. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration. "
                                    + "If user enable the scan.infer-parallelism, the planner will derive the parallelism by inferred parallelism.");

    public static final ConfigOption<Boolean> INFER_SCAN_PARALLELISM =
            ConfigOptions.key("scan.infer-parallelism")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If it is false, parallelism of source are set by "
                                    + SCAN_PARALLELISM.key()
                                    + ". Otherwise, source parallelism is inferred from splits number (batch mode) or bucket number(streaming mode).");

    public static final ConfigOption<Boolean> STREAMING_READ_ATOMIC =
            ConfigOptions.key("streaming-read-atomic")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The option to enable return per iterator instead of per record in streaming read.")
                                    .text(
                                            "This can ensure that there will be no checkpoint segmentation in iterator consumption.")
                                    .linebreak()
                                    .text(
                                            "By default, streaming source checkpoint will be performed in any time,"
                                                    + " this means 'UPDATE_BEFORE' and 'UPDATE_AFTER' can be split into two checkpoint."
                                                    + " Downstream can see intermediate state.")
                                    .build());

    @Deprecated
    @ExcludeFromDocumentation("Deprecated")
    public static final ConfigOption<Duration> CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL =
            key("changelog-producer.compaction-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0))
                    .withDescription(
                            "When "
                                    + CoreOptions.CHANGELOG_PRODUCER.key()
                                    + " is set to "
                                    + CoreOptions.ChangelogProducer.FULL_COMPACTION.name()
                                    + ", full compaction will be constantly triggered after this interval.");

    public static final ConfigOption<Boolean> CHANGELOG_PRODUCER_LOOKUP_WAIT =
            key("changelog-producer.lookup-wait")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When "
                                    + CoreOptions.CHANGELOG_PRODUCER.key()
                                    + " is set to "
                                    + CoreOptions.ChangelogProducer.LOOKUP.name()
                                    + ", commit will wait for changelog generation by lookup.");

    public static final ConfigOption<WatermarkEmitStrategy> SCAN_WATERMARK_EMIT_STRATEGY =
            key("scan.watermark.emit.strategy")
                    .enumType(WatermarkEmitStrategy.class)
                    .defaultValue(WatermarkEmitStrategy.ON_EVENT)
                    .withDescription("Emit strategy for watermark generation.");

    public static final ConfigOption<String> SCAN_WATERMARK_ALIGNMENT_GROUP =
            key("scan.watermark.alignment.group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("A group of sources to align watermarks.");

    public static final ConfigOption<Duration> SCAN_WATERMARK_IDLE_TIMEOUT =
            key("scan.watermark.idle-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "If no records flow in a partition of a stream for that amount of time, then"
                                    + " that partition is considered \"idle\" and will not hold back the progress of"
                                    + " watermarks in downstream operators.");

    public static final ConfigOption<Duration> SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT =
            key("scan.watermark.alignment.max-drift")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximal drift to align watermarks, "
                                    + "before we pause consuming from the source/task/partition.");

    public static final ConfigOption<Duration> SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL =
            key("scan.watermark.alignment.update-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "How often tasks should notify coordinator about the current watermark "
                                    + "and how often the coordinator should announce the maximal aligned watermark.");

    public static final ConfigOption<Integer> SCAN_SPLIT_ENUMERATOR_BATCH_SIZE =
            key("scan.split-enumerator.batch-size")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "How many splits should assign to subtask per batch in StaticFileStoreSplitEnumerator "
                                    + "to avoid exceed `akka.framesize` limit.");

    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = FlinkConnectorOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(FlinkConnectorOptions.class));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }

    /** Watermark emit strategy for scan. */
    public enum WatermarkEmitStrategy implements DescribedEnum {
        ON_PERIODIC(
                "on-periodic",
                "Emit watermark periodically, interval is controlled by Flink 'pipeline.auto-watermark-interval'."),

        ON_EVENT("on-event", "Emit watermark per record.");

        private final String value;
        private final String description;

        WatermarkEmitStrategy(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }
}
