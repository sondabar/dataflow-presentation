package de.sondabar;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.TextIO.Read;
import com.google.cloud.dataflow.sdk.io.TextIO.Write;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumLongFn;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import de.sondabar.common.TupleTags;
import de.sondabar.fn.combine.CampaignResultFn;
import de.sondabar.fn.read.ToBidResponse;
import de.sondabar.fn.read.ToCampaignResponse;
import de.sondabar.fn.read.ToClick;
import de.sondabar.fn.read.ToConversion;
import de.sondabar.fn.read.ToImpression;
import de.sondabar.fn.read.ToVisibleImpression;
import de.sondabar.fn.read.ToWonBid;
import de.sondabar.fn.transform.CampaignResultToTableRows;
import de.sondabar.fn.transform.CombineColumns;
import de.sondabar.fn.transform.MapCampaignIdToTableRow;
import de.sondabar.fn.transform.MapToCampaignKey;
import de.sondabar.fn.transform.TableRowsToColumns;
import de.sondabar.fn.transform.ToStringFn;
import de.sondabar.model.Bid;
import de.sondabar.model.CampaignResult;
import de.sondabar.model.Click;
import de.sondabar.model.Conversion;
import de.sondabar.model.Impression;
import de.sondabar.model.VisibleImpression;
import de.sondabar.model.WonBid;

@SuppressWarnings("HardcodedFileSeparator")
public class HuginExample
{

   public static void main(final String[] args)
   {
      final DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
      options.setRunner(DirectPipelineRunner.class);

      final Pipeline pipeline = Pipeline.create(options);

      final PCollection<KV<String, Bid>> bidRequests =
         pipeline.apply(Read.from("src/main/resources/hugin/bids.json").withCoder(TableRowJsonCoder.of()))
            .apply(ParDo.of(new ToBidResponse())).apply(ParDo.of(new ToCampaignResponse()));

      final PCollection<KV<String, WonBid>> wonBids =
         pipeline.apply(Read.from("src/main/resources/hugin/wonBids.csv")).apply(ParDo.of(new ToWonBid()));

      final PCollection<KV<String, Impression>> impressions =
         pipeline.apply(Read.from("src/main/resources/hugin/impressions.csv")).apply(ParDo.of(new ToImpression()));

      final PCollection<KV<String, VisibleImpression>> visibleImpressions =
         pipeline.apply(Read.from("src/main/resources/hugin/visibleImpressions.csv"))
            .apply(ParDo.of(new ToVisibleImpression()));

      final PCollection<KV<String, Click>> clicks =
         pipeline.apply(Read.from("src/main/resources/hugin/clicks.csv")).apply(ParDo.of(new ToClick()));

      final PCollection<KV<String, Conversion>> conversions =
         pipeline.apply(Read.from("src/main/resources/hugin/conversions.csv")).apply(ParDo.of(new ToConversion()));

      final PCollection<CampaignResult> resultPCollection =
         KeyedPCollectionTuple.of(TupleTags.CAMPAIGN_TUPLE, bidRequests)
            .and(TupleTags.WON_BID_TUPLE, wonBids)
            .and(TupleTags.IMPRESSION_TUPLE, impressions)
            .and(TupleTags.VIS_IMPRESSION_TUPLE, visibleImpressions)
            .and(TupleTags.CLICK_TUPLE, clicks)
            .and(TupleTags.CONVERSION_TUPLE, conversions)
            .apply(CoGroupByKey.create())
            .apply(ParDo.of(new CampaignResultFn()));

      resultPCollection.apply(ParDo.of(new ToStringFn())).apply(
         Write.to("src/main/resources/hugin/campaignResult.csv").withoutSharding().withCoder(StringUtf8Coder.of()));

      resultPCollection.apply(MapElements.via(new CampaignResultToTableRows())).apply(GroupByKey.create())
         // Split every row into column with cid_columnName and groups them
         .apply(ParDo.of(new TableRowsToColumns())).apply(GroupByKey.create())
         // Sum up
         .apply(Combine.groupedValues(new SumLongFn()))
         // Map to campaign key
         .apply(MapElements.via(new MapToCampaignKey()))
         // Combine many row to one
         .apply(Combine.perKey(new CombineColumns()))
         // Map campaign id to tablerow
         .apply(MapElements.via(new MapCampaignIdToTableRow()))
         .apply(Write.to("src/main/resources/hugin/result.json").withoutSharding().withCoder(TableRowJsonCoder.of()));
      pipeline.run();

   }

}