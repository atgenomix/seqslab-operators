package com.atgenomix.seqslab.operators.loader

import com.atgenomix.seqslab.operators.loader.CsvInMemoryDataSource.CsvInMemoryLoader
import com.atgenomix.seqslab.piper.engine.actor.Utils.RecordPerRowLoader
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.loader.{Loader, LoaderSupport}


object CsvInMemoryDataSource {
  private class CsvInMemoryLoader(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends RecordPerRowLoader(pluginCtx, operatorCtx) {

  }
}

class CsvInMemoryDataSource extends LoaderSupport {
  override def createLoader(pluginContext: PluginContext, operatorContext: OperatorContext): Loader = {
    new CsvInMemoryLoader(pluginContext, operatorContext)
  }
}
