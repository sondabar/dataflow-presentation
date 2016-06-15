package de.sondabar.model;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.avro.reflect.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class CampaignResult extends CampaignEvent
{

   @Nullable
   protected List<Feature> features;

   public enum State
   {
      Bid(new TableRow().set("Bids", 1).set("WonBids", 0).set("Impressions", 0).set("VisibleImpressions", 0)
             .set("Clicks", 0).set("Conversions", 0)), WonBid(
      new TableRow().set("Bids", 1).set("WonBids", 1).set("Impressions", 0).set("VisibleImpressions", 0)
         .set("Clicks", 0).set("Conversions", 0)), Impression(
      new TableRow().set("Bids", 1).set("WonBids", 1).set("Impressions", 1).set("VisibleImpressions", 0)
         .set("Clicks", 0).set("Conversions", 0)), VisibleImpression(
      new TableRow().set("Bids", 1).set("WonBids", 1).set("Impressions", 1).set("VisibleImpressions", 1)
         .set("Clicks", 0).set("Conversions", 0)), Click(
      new TableRow().set("Bids", 1).set("WonBids", 1).set("Impressions", 1).set("VisibleImpressions", 1)
         .set("Clicks", 1).set("Conversions", 0)),
      Conversion(new TableRow().set("Bids", 1).set("WonBids", 1).set("Impressions", 1).set("VisibleImpressions", 1)
                    .set("Clicks", 1).set("Conversions", 1));

      private final TableRow tableRow;

      State(TableRow aTableRow)
      {
         tableRow = aTableRow;
      }
   }

   private State state = State.Bid;

   @Nullable
   protected BigDecimal bidPrice;

   @Nullable
   private BigDecimal wonPrice;

   public CampaignResult()
   {
   }

   public CampaignResult(Bid bid, WonBid wonBid)
   {
      this.bid = bid.bid;
      cid = bid.cid;
      bidPrice = bid.bidPrice;
      timestamp = bid.timestamp;
      features = new ArrayList<>(bid.features);
      if(wonBid != null)
      {
         wonPrice = wonBid.getPrice();
      }
   }

   public State getState()
   {
      return state;
   }

   public void setState(State state)
   {
      this.state = state;
   }

   public KV<Long, TableRow> getTableRow()
   {
      return KV.of(cid, state.tableRow);
   }

   @Override
   public String toString()
   {
      return "CampaignResult{" +
             "bid=" + bid +
             ", ts =" + timestamp +
             ", cid=" + cid +
             ", features=" + features +
             ", state=" + state +
             ", bidPrice=" + bidPrice +
             ", wonPrice=" + wonPrice +
             '}';
   }
}
