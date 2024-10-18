package com.atgenomix.seqslab


object TestUtil {
  def getCliArgs(
    testName: String,
    taskFqn: String,
    inputMappingPath: String,
    scriptPath: String,
    redisHost: String,
    redisPort: String
  ): Array[String] = {
    Array(
      "--workflow-req-src", "File",
      "--task-fqn", taskFqn,
      "--file-path", inputMappingPath,
      "--script-path", scriptPath,
      "--workflow-id", testName,
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/$testName/",
      "--task-root", s"hdfs://localhost:9000/plugin_test/$testName/outputs/",
      "--redis-url", redisHost,
      "--redis-port", redisPort,
      "--redis-key", testName,
      "--redis-db", "0"
    )
  }
}
