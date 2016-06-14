package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

import java.util.Map;

@DefaultCoder(AvroCoder.class)
public class Feature {

    @Nullable
    String name;
    @Nullable
    String value;

    public Feature() {
    }

    public Feature(Map<String, String> map) {
        this.name = map.get("fn");
        this.value = map.get("fv");
    }

    @Override
    public String toString() {
        return "Feature{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
