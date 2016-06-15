package de.sondabar;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.TextIO.Read;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import de.sondabar.model.Click;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClicksExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClicksExample.class);

    public static void main(String[] args) {
        final DataflowPipelineOptions options = PipelineOptionsFactory.create()
                                                                      .as(DataflowPipelineOptions.class);
        options.setRunner(DirectPipelineRunner.class);

        final Pipeline pipeline = Pipeline.create(options);

        final PCollection<Click> clicksTableRow = pipeline.apply(Read.from("src/main/resources/clicks.csv"))
                                                          .apply(ParDo.of(new DoFn<String, Click>() {
                                                              @Override
                                                              public void processElement(
                                                                      ProcessContext processContext) throws Exception {
                                                                  final String line = processContext.element();
                                                                  final Click click = new Click(line);
                                                                  LOGGER.info(click.toString());
                                                                  processContext.output(click);
                                                              }
                                                          }));

        clicksTableRow.apply(Count.globally()).apply(ParDo.of(new DoFn<Long, Long>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                final Long element = processContext.element();
                LOGGER.info(element.toString());
                processContext.output(element);
            }
        }));

        clicksTableRow.apply(ParDo.of(new DoFn<Click, KV<Long, Click>>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                final Click click = processContext.element();
                processContext.output(KV.of(click.getCid(), click));
            }
        })).apply(Count.perKey()).apply(ParDo.of(new DoFn<KV<Long, Long>, KV<Long, Long>>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                final KV<Long, Long> element = processContext.element();
                LOGGER.info(element.toString());
                processContext.output(element);
            }
        }));

        pipeline.run();

    }
}