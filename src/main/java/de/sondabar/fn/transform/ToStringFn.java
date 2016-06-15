package de.sondabar.fn.transform;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

public class ToStringFn extends DoFn<Object, String>
{
   @Override
   public void processElement(final ProcessContext aContext) throws Exception
   {
      aContext.output(aContext.element().toString());
   }
}
