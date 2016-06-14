package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

@DefaultCoder(AvroCoder.class)
public class Event {
    @Nullable
    String bid;
    @Nullable
    Long timestamp;

    public Event() {
    }

    public Event(String bid, Long timestamp) {
        this.bid = bid;
        this.timestamp = timestamp;
    }

    public Event(String bid, String timestamp) {
        this.bid = bid;
        this.timestamp = Long.parseLong(timestamp);
    }

    public Event(String line) {
        final String[] fields = line.split(",");
        this.bid = fields[0];
        this.timestamp = Long.parseLong(fields[1]);
    }

    public String getBid() {
        return bid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "bid=" + bid +
                ", timestamp=" + timestamp +
                '}';
    }
}
