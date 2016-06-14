package de.sondabar;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import de.sondabar.common.TupleTags;
import de.sondabar.fn.combine.CampaignResultFn;
import de.sondabar.fn.read.*;
import de.sondabar.model.*;

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


        PCollection<CampaignResult> resultPCollection = KeyedPCollectionTuple.of(TupleTags.CAMPAIGN_TUPLE, bidRequests)
                                                                             .and(TupleTags.WON_BID_TUPLE, wonBids)
                                                                             .and(TupleTags.IMPRESSION_TUPLE,
                                                                                  impressions)
                                                                             .and(TupleTags.VIS_IMPRESSION_TUPLE,
                                                                                  visibleImpressions)
                                                                             .and(TupleTags.CLICK_TUPLE, clicks)
                                                                             .and(TupleTags.CONVERSION_TUPLE,
                                                                                  conversions)
                                                                             .apply(CoGroupByKey.<String>create())
                                                                             .apply(ParDo.of(new CampaignResultFn()));

        resultPCollection.apply(ParDo.of(new DoFn<CampaignResult, String>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                c.output(c.element().toString());
            }
        })).apply(TextIO.Write.to("src/main/resources/hugin/campaignResult.csv"));


        pipeline.run();

    }

}