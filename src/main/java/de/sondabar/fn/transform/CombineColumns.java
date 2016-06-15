package de.sondabar.fn.transform;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;

import java.util.HashMap;
import java.util.Map;

public class CombineColumns extends SimpleFunction<Iterable<Map<String, String>>, Map<String, String>>
{
   @Override
   public Map<String, String> apply(final Iterable<Map<String, String>> aInput)
   {
      final Map<String, String> result = new HashMap<>(10);
      aInput.forEach(result::putAll);
      return result;
   }
}
