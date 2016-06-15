package de.sondabar.fn.transform;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;

import java.util.HashMap;
import java.util.Map;

public class MapToCampaignKey extends SimpleFunction<KV<String, Long>, KV<String, Map<String, String>>>
{
   @Override
   public KV<String, Map<String, String>> apply(final KV<String, Long> input)
   {
      final String[] strs = input.getKey().split("_");
      final Map<String, String> map = new HashMap<>(1);
      map.put(strs[1], input.getValue().toString());
      return KV.of(strs[0], map);
   }
}
