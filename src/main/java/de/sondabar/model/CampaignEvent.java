package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

@DefaultCoder(AvroCoder.class)
public class CampaignEvent extends Event {

    @Nullable
    Long cid;

    public CampaignEvent() {
    }

    public CampaignEvent(Long bid, Long timestamp, Long cid) {
        this.bid = bid;
        this.timestamp = timestamp;
        this.cid = cid;
    }

    public CampaignEvent(String bid, String timestamp, String cid) {
        this.bid = Long.parseLong(bid);
        this.timestamp = Long.parseLong(timestamp);
        this.cid = Long.parseLong(cid);
    }

    public CampaignEvent(String line)
    {
        final String[] fields = line.split(",");
        this.bid = Long.parseLong(fields[0]);
        this.timestamp = Long.parseLong(fields[1]);
        this.cid = Long.parseLong(fields[2]);
    }

    public Long getBid() {
        return bid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Long getCid() {
        return cid;
    }

    @Override
    public String toString() {
        return "CampaignEvent{" +
                "bid=" + bid +
                ", timestamp=" + timestamp +
                ", cid=" + cid +
                '}';
    }
}
