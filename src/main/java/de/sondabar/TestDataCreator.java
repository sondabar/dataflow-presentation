package de.sondabar;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestDataCreator {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestDataCreator.class);

    private final static Random random = new Random();

    public static void main(String[] args) {
        final Random random = new Random();

        final DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        options.setRunner(DirectPipelineRunner.class);

        final Pipeline pipeline = Pipeline.create(options);

        List<Integer> bidIds = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());

        final int featureCount = 10;

        ArrayList<String> featureNames = IntStream.rangeClosed(1, featureCount)
                                                  .boxed()
                                                  .map(i -> "feature_" + i)
                                                  .collect(Collectors.toCollection(ArrayList<String>::new));

        ArrayList<String> featureValues = IntStream.rangeClosed(1, featureCount)
                                                   .boxed()
                                                   .map(i -> "featureValue_" + i)
                                                   .collect(Collectors.toCollection(ArrayList<String>::new));

        PCollection<KV<String, TableRow>> bids = pipeline.apply(Create.of(bidIds))
                                                         .apply(ParDo.of(new DoFn<Integer, KV<String, TableRow>>() {
                                                             @Override
                                                             public void processElement(
                                                                     ProcessContext c) throws Exception {
                                                                 String bidId = Integer.toString(c.element());
                                                                 final TableRow row = new TableRow();
                                                                 row.put("bid", bidId);
                                                                 row.put("ts", System.currentTimeMillis());

                                                                 c.output(KV.of(bidId, row));
                                                             }
                                                         }))
                                                         .apply(ParDo.of(new DoFn<KV<String, TableRow>, KV<String, TableRow>>() {
                                                             @Override
                                                             public void processElement(
                                                                     ProcessContext c) throws Exception {
                                                                 String bidId = c.element().getKey();
                                                                 final TableRow row = c.element().getValue().clone();
                                                                 final int subRowCount = random.nextInt(3) + 1;
                                                                 final List<TableRow> subRows = new ArrayList<>(
                                                                         subRowCount);
                                                                 for (int i = 1; i <= subRowCount; i++) {
                                                                     final TableRow subRow = new TableRow();
                                                                     subRow.put("fv",
                                                                                featureValues.get(random.nextInt(
                                                                                        featureCount)));
                                                                     subRow.put("fn",
                                                                                featureNames.get(random.nextInt(
                                                                                        featureCount)));
                                                                     subRows.add(subRow);
                                                                 }
                                                                 row.put("features", subRows);
                                                                 c.output(KV.of(bidId, row));
                                                             }
                                                         }))
                                                         .apply(ParDo.of(new DoFn<KV<String, TableRow>, KV<String, TableRow>>() {
                                                             @Override
                                                             public void processElement(
                                                                     ProcessContext c) throws Exception {
                                                                 String bidId = c.element().getKey();
                                                                 final TableRow row = c.element().getValue().clone();
                                                                 final int subRowCount = random.nextInt(3) + 1;
                                                                 final List<TableRow> subRows = new ArrayList<>(
                                                                         subRowCount);
                                                                 List<Integer> taken = new ArrayList<>(subRowCount);
                                                                 for (int i = 1; i <= subRowCount; i++) {
                                                                     final TableRow subRow = new TableRow();
                                                                     subRow.put("bpr",
                                                                                BigDecimal.valueOf(random.nextDouble()));
                                                                     subRow.put("cid", newRandomInt(taken));
                                                                     subRows.add(subRow);
                                                                 }
                                                                 row.put("bids", subRows);
                                                                 c.output(KV.of(bidId, row));
                                                             }
                                                         }));
        bids.apply(Values.create())
            .apply(TextIO.Write.to("src/main/resources/hugin/bids.json")
                               .withCoder(TableRowJsonCoder.of())
                               .withoutSharding());

        pipeline.run();
    }

    private static int newRandomInt(List<Integer> taken) {
        int cid = random.nextInt(9) + 1;
        if (!taken.contains(cid)) {
            taken.add(cid);
            return cid;
        } else {
            return newRandomInt(taken);
        }
    }
}
