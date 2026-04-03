/*
 * Copyright 2021-2024 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zio

import zio.test.*
import zio.test.Assertion.*

import java.util.concurrent.TimeUnit

object ZIOAppSignalHandlingSpec extends ZIOSpecDefault {

  def spec = suite("ZIOAppSignalHandlingSpec")(
    test("signal handlers are installed") {
      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          for {
            runtime <- ZIO.runtime[Any]
            _       <- installSignalHandlers(runtime)
          } yield ExitCode.success
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isSuccess)
    },
    test("finalizers run before app exit on success") {
      val finalizerFlag = Ref.make(false)

      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.scoped {
            for {
              ref <- ZIO.from(finalizerFlag)
              _ <- ZIO.acquireRelease(
                ZIO.unit
              )(
                _ => ref.set(true)
              )
            } yield ExitCode.success
          }
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isSuccess)
    },
    test("long-running finalizers respect gracefulShutdownTimeout") {
      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.scoped {
            ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.sleep(Duration(10, TimeUnit.SECONDS))
            ) *> ZIO.succeed(ExitCode.success)
          }

        override def gracefulShutdownTimeout: Duration =
          Duration(1, TimeUnit.SECONDS)
      }

      for {
        start <- Clock.instant
        exit  <- app.invoke(Chunk.empty).exit
        end   <- Clock.instant
      } yield assert(exit)(isSuccess)
    },
    test("app with scoped resource properly releases on exit") {
      val releaseCount = Ref.make(0)

      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.scoped {
            for {
              ref <- ZIO.from(releaseCount)
              _ <- ZIO.acquireRelease(
                ZIO.unit
              )(
                _ => ref.update(_ + 1)
              )
            } yield ExitCode.success
          }
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isSuccess)
    },
    test("concurrent finalizers all complete") {
      val finalizeCount = Ref.make(0)

      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.scoped {
            for {
              ref <- ZIO.from(finalizeCount)
              _ <- ZIO.foreachPar(1 to 5)(_ =>
                ZIO.acquireRelease(ZIO.unit)(_ => ref.update(_ + 1))
              )
            } yield ExitCode.success
          }
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isSuccess)
    },
    test("nested scopes are properly unwound") {
      val unwoundCount = Ref.make(0)

      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.scoped {
            for {
              ref <- ZIO.from(unwoundCount)
              _ <- ZIO.acquireRelease(ZIO.unit)(_ => ref.update(_ + 1))
              _ <- ZIO.scoped {
                ZIO.acquireRelease(ZIO.unit)(_ => ref.update(_ + 1))
              }
            } yield ExitCode.success
          }
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isSuccess)
    }
  )
}
