package de.sondabar.fn.read;

import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import de.sondabar.model.Click;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ToClickTest
{
   @Test
   public void testClickCreation()
   {
      final DoFnTester<String, KV<String, Click>> clickTest = DoFnTester.of(new ToClick());

      final List<KV<String, Click>> result = clickTest.processBatch("test,566664,2");
      Assert.assertEquals("Wrong key", "test_2", result.get(0).getKey());

      final Click click = result.get(0).getValue();
      Assert.assertEquals("Wrong bid", "test", click.getBid());
      Assert.assertEquals("Wrong timestamp", new Long(566664L), click.getTimestamp());
      Assert.assertEquals("Wrong Campaign", new Long(2L), click.getCid());
   }
}
