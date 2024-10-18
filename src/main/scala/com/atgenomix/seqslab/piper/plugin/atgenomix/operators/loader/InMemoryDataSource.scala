package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader

import com.atgenomix.seqslab.piper.common.operator.AbstractInMemoryDataLoader
import com.atgenomix.seqslab.piper.plugin.api.loader._
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader.InMemoryDataSource.InMemoryDataLoader


object InMemoryDataSource {
  private class InMemoryDataLoader(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends AbstractInMemoryDataLoader(pluginCtx, operatorCtx)
}

class InMemoryDataSource extends LoaderSupport {
  override def createLoader(pluginContext: PluginContext, operatorContext: OperatorContext): Loader = {
    new InMemoryDataLoader(pluginContext, operatorContext)
  }
}