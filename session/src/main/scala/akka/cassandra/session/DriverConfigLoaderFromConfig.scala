/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra.session

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import com.datastax.oss.driver.api.core.config.DriverConfig
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig
import com.typesafe.config.Config

object DriverConfigLoaderFromConfig {
  def fromConfig(config: Config): DriverConfigLoader =
    new DriverConfigLoaderFromConfig(config)
}

class DriverConfigLoaderFromConfig(config: Config) extends DriverConfigLoader {

  private val driverConfig: DriverConfig = new TypesafeDriverConfig(config)

  override def getInitialConfig: DriverConfig = {
    driverConfig
  }

  override def onDriverInit(context: DriverContext): Unit = ()

  override def reload(): CompletionStage[java.lang.Boolean] =
    CompletableFuture.completedFuture(false)

  override def supportsReloading(): Boolean = false

  override def close(): Unit = ()
}
