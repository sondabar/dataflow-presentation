package de.sondabar;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import de.sondabar.fn.read.*;
import de.sondabar.model.*;

import java.util.Stack;

public class HuginExample {

    public static void main(String[] args) {
        final DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        options.setRunner(DirectPipelineRunner.class);

        final Pipeline pipeline = Pipeline.create(options);

        final PCollection<KV<String, Bid>> bidRequests = pipeline.apply(TextIO.Read.from(
                "src/main/resources/hugin/bids.json").withCoder(TableRowJsonCoder.of()))
                                                                 .apply(ParDo.of(new ToBidResponse()))
                                                                 .apply(ParDo.of(new ToCampaignResponse()));

        final PCollection<KV<String, WonBid>> wonBids = pipeline.apply(TextIO.Read.from(
                "src/main/resources/hugin/wonBids.csv")).apply(ParDo.of(new ToWonBid()));

        final PCollection<KV<String, Impression>> impressions = pipeline.apply(TextIO.Read.from(
                "src/main/resources/hugin/impressions.csv")).apply(ParDo.of(new ToImpression()));

        final PCollection<KV<String, VisibleImpression>> visibleImpressions = pipeline.apply(TextIO.Read.from(
                "src/main/resources/hugin/visibleImpressions.csv")).apply(ParDo.of(new ToVisibleImpression()));

        final PCollection<KV<String, Click>> clicks = pipeline.apply(TextIO.Read.from(
                "src/main/resources/hugin/clicks.csv")).apply(ParDo.of(new ToClick()));

        final PCollection<KV<String, Conversion>> conversions = pipeline.apply(TextIO.Read.from(
                "src/main/resources/hugin/conversions.csv")).apply(ParDo.of(new ToConversion()));

        TupleTag<Bid> campaignTuple = new TupleTag<>();
        TupleTag<WonBid> wonBidTuple = new TupleTag<>();
        TupleTag<Impression> impressionTuple = new TupleTag<>();
        TupleTag<VisibleImpression> visImpressionTuple = new TupleTag<>();
        TupleTag<Click> clickTuple = new TupleTag<>();
        TupleTag<Conversion> conversionTuple = new TupleTag<>();


        KeyedPCollectionTuple.of(campaignTuple, bidRequests)
                             .and(wonBidTuple, wonBids)
                             .and(impressionTuple, impressions)
                             .and(visImpressionTuple, visibleImpressions)
                             .and(clickTuple, clicks)
                             .and(conversionTuple, conversions)
                             .apply(CoGroupByKey.<String>create())
                             .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, Result>() {
                                 @Override
                                 public void processElement(ProcessContext c) throws Exception {
                                     CoGbkResult value = c.element().getValue();
                                     Bid bid = value.getOnly(campaignTuple);
                                     WonBid wonBid = value.getOnly(wonBidTuple, null);
                                     Impression impression = value.getOnly(impressionTuple, null);
                                     VisibleImpression visibleImpression = value.getOnly(visImpressionTuple, null);
                                     Click click = value.getOnly(clickTuple, null);
                                     Conversion conversion = value.getOnly(conversionTuple, null);

                                     Result result = new Result(bid);
                                     result.setState(getState(bid,
                                                              wonBid,
                                                              impression,
                                                              visibleImpression,
                                                              click,
                                                              conversion));
                                     c.output(result);
                                 }

                                 private Result.State getState(Event... events) {
                                     Stack<Event> stack = new Stack<>();
                                     for (Event event : events) {
                                         if (event != null) {
                                             stack.push(event);
                                         }
                                     }
                                     return Result.State.valueOf(stack.pop().getClass().getSimpleName());
                                 }
                             }));


        pipeline.run();

    }

}