/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra.session

import scala.collection.immutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure

import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.util.unused
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * The implementation of the `SessionProvider` is used for creating the
 * Cassandra Session. By default the [[DefaultSessionProvider]] is building
 * the Cluster from configuration properties but it is possible to
 * replace the implementation of the SessionProvider to reuse another
 * session or override the Cluster builder with other settings.
 *
 * The implementation is defined in configuration `session-provider` property.
 * It may optionally have a constructor with an ActorSystem and Config parameter.
 * The config parameter is the config section of the plugin.
 */
trait CqlSessionProvider {
  def connect()(implicit ec: ExecutionContext): Future[CqlSession]
}

class DefaultSessionProvider(@unused system: ActorSystem, config: Config) extends CqlSessionProvider {
  override def connect()(implicit ec: ExecutionContext): Future[CqlSession] = {
    val builder = CqlSession.builder()
    // FIXME PN: could we just define the session name in the `datastax-java-driver.basic.session-name`?
//    val sessionName = config.getString("session-name")
//    (if (sessionName != null && sessionName.nonEmpty) {
//       val overload: DriverConfigLoader =
//         DriverConfigLoader.programmaticBuilder().withString(DefaultDriverOption.SESSION_NAME, sessionName).build()
//       builder.withConfigLoader(overload)
//     } else {
//       builder
//     }).buildAsync().toScala

    val driverConfigPath = config.getString("datastax-java-driver-config")
    val driverConfig =
      system.settings.config
        .getConfig(driverConfigPath)
        .withFallback(if (driverConfigPath == "datastax-java-driver") ConfigFactory.empty()
        else system.settings.config.getConfig("datastax-java-driver"))

    val driverConfigLoader = DriverConfigLoaderFromConfig.fromConfig(driverConfig)

    builder.withConfigLoader(driverConfigLoader).buildAsync().toScala
  }
}

object CqlSessionProvider {

  /**
   * Create a `SessionProvider` from configuration.
   * The `session-provider` config property defines the fully qualified
   * class name of the SessionProvider implementation class. It may optionally
   * have a constructor with an `ActorSystem` and `Config` parameter.
   */
  def apply(system: ExtendedActorSystem, config: Config): CqlSessionProvider = {
    val className = config.getString("session-provider")
    val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess
    val clazz = dynamicAccess.getClassFor[CqlSessionProvider](className).get
    def instantiate(args: immutable.Seq[(Class[_], AnyRef)]) =
      dynamicAccess.createInstanceFor[CqlSessionProvider](clazz, args)

    val params = List((classOf[ActorSystem], system), (classOf[Config], config))
    instantiate(params)
      .recoverWith {
        case x: NoSuchMethodException => instantiate(params.take(1))
      }
      .recoverWith { case x: NoSuchMethodException => instantiate(Nil) }
      .recoverWith {
        case ex: Exception =>
          Failure(
            new IllegalArgumentException(
              s"Unable to create SessionProvider instance for class [$className], " +
              "tried constructor with ActorSystem, Config, and only ActorSystem, and no parameters",
              ex))
      }
      .get
  }
}
