package de.sondabar.model;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@DefaultCoder(AvroCoder.class)
public class BidResponse extends Event {

    @Nullable
    protected List<Feature> features = null;
    @Nullable
    protected List<BidLine> bidLines = null;

    public BidResponse() {
        bid = UUID.randomUUID().toString();
    }

    public BidResponse(final TableRow tableRow) {
        bid = (String)tableRow.get("bid");
        timestamp = (Long)tableRow.get("ts");

        final List<Map<String, String>> features = (List<Map<String, String>>) tableRow.get("features");
        this.features = new ArrayList<>(features.size());
        for(final Map<String, String> map: features)
        {
            this.features.add(new Feature(map));
        }
        final List<Map<String, Object>> bids = (List<Map<String, Object>>) tableRow.get("bids");
        bidLines = new ArrayList<>(bids.size());
        for(final Map<String, Object> map: bids)
        {
            bidLines.add(new BidLine(map));
        }
    }

    public List<BidLine> getBidLines() {
        return bidLines;
    }

    @Override
    public String toString() {
        return "BidRequest{" +
                "features=" + features +
                ", bids=" + bidLines +
                '}';
    }
}
