/*
 * Copyright (C) 2022, Atgenomix Incorporated.
 *
 * All Rights Reserved.
 *
 * This program is an unpublished copyrighted work which is proprietary to
 * Atgenomix Incorporated and contains confidential information that is not to
 * be reproduced or disclosed to any other person or entity without prior
 * written consent from Atgenomix, Inc. in each and every instance.
 *
 * Unauthorized reproduction of this program as well as unauthorized
 * preparation of derivative works based upon the program or distribution of
 * copies by sale, rental, lease or lending are violations of federal copyright
 * laws and state trade secret laws, punishable by civil and criminal penalties.
 */

package com.atgenomix.seqslab

import com.atgenomix.seqslab.HDFSCluster.hdfsCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.test.PathUtils

import java.io.File

object HDFSCluster {
  private var hdfsCluster: MiniDFSCluster = _

  def startHDFS(hdfsPort: Int): Unit = {
    this.synchronized {
      if (hdfsCluster == null) {
        println("Starting HDFS Cluster...")
        val baseDir = new File(PathUtils.getTestDir(getClass), "miniHDFS")
        val conf = new Configuration()
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
        conf.setBoolean("dfs.webhdfs.enabled", true)
        val builder = new MiniDFSCluster.Builder(conf)
        hdfsCluster = builder.nameNodePort(hdfsPort).manageNameDfsDirs(true).manageDataDfsDirs(true).format(true).build()
        hdfsCluster.waitClusterUp()
      }
    }
  }
}

trait HDFSCluster extends Serializable {
  def hdfsPort: Int

  def getNameNodeURI: String = "hdfs://localhost:" + hdfsCluster.getNameNodePort

  def shutdownHDFS(): Unit = {
    println("Shutting down HDFS Cluster...")
    hdfsCluster.shutdown(true)
    hdfsCluster.close()
    hdfsCluster = null
  }
}

