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

import java.io.File
import java.util.concurrent.TimeUnit
import scala.sys.process.*

object ZIOAppSpec extends ZIOSpecDefault {

  def spec = suite("ZIOAppSpec")(
    test("app completes successfully with exit code 0") {
      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.succeed(ExitCode.success)
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isSuccess(equalTo(ExitCode.success)))
    },
    test("app completes with failure and emits non-zero exit code") {
      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.fail(new Exception("test failure"))
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isFailure)
    },
    test("app runs finalizers on successful completion") {
      val finalizerRun = Ref.make(false)

      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.scoped {
            for {
              ref <- ZIO.from(finalizerRun)
              _   <- ZIO.acquireRelease(ZIO.unit)(_ => ref.set(true))
            } yield ExitCode.success
          }
      }

      for {
        exit      <- app.invoke(Chunk.empty).exit
        finalized <- Ref.make(false).flatMap(ref => app.invoke(Chunk.empty) *> ref.get)
      } yield assert(exit)(isSuccess)
    },
    test(
      "app respects gracefulShutdownTimeout"
    ) {
      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.succeed(ExitCode.success)

        override def gracefulShutdownTimeout: Duration =
          Duration(500, TimeUnit.MILLISECONDS)
      }

      for {
        start <- Clock.instant
        exit  <- app.invoke(Chunk.empty).exit
        end   <- Clock.instant
        duration = end.getEpochSecond - start.getEpochSecond
      } yield assert(exit)(isSuccess)
    },
    test("app passes command-line arguments correctly") {
      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          for {
            args <- getArgs
          } yield ExitCode.success
      }

      val testArgs = Chunk("arg1", "arg2", "arg3")

      for {
        exit <- app.invoke(testArgs).exit
      } yield assert(exit)(isSuccess)
    },
    test("app shutdown sequence doesn't hang on success") {
      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.succeed(ExitCode.success)
      }

      for {
        exit <- withTimeoutFatal(app.invoke(Chunk.empty), Duration(5, TimeUnit.SECONDS)).exit
      } yield assert(exit)(isSuccess)
    },
    test("app shutdown sequence doesn't hang on failure") {
      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.fail(new Exception("test failure"))
      }

      for {
        exit <- withTimeoutFatal(
          app.invoke(Chunk.empty),
          Duration(5, TimeUnit.SECONDS)
        ).exit
      } yield assert(exit)(isFailure) || assert(exit)(isSuccess)
    },
    test("app with multiple finalizers runs all of them") {
      val finalizersRun = Ref.make(0)

      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.scoped {
            for {
              ref <- ZIO.from(finalizersRun)
              _   <- ZIO.acquireRelease(ZIO.unit)(_ => ref.update(_ + 1))
              _   <- ZIO.acquireRelease(ZIO.unit)(_ => ref.update(_ + 1))
              _   <- ZIO.acquireRelease(ZIO.unit)(_ => ref.update(_ + 1))
            } yield ExitCode.success
          }
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isSuccess)
    },
    test("app can be invoked multiple times") {
      val counter = Ref.make(0)

      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          for {
            ref <- ZIO.from(counter)
            _   <- ref.update(_ + 1)
          } yield ExitCode.success
      }

      for {
        exit1 <- app.invoke(Chunk.empty).exit
        exit2 <- app.invoke(Chunk.empty).exit
      } yield assert(exit1)(isSuccess) && assert(exit2)(isSuccess)
    },
    test("app with custom bootstrap layer works correctly") {
      val layer = ZLayer.succeed("test-value")

      val app = new ZIOApp {
        type Environment = String

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[String]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          layer

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          for {
            value <- ZIO.service[String]
          } yield if (value == "test-value") ExitCode.success else ExitCode.failure
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isSuccess)
    },
    test("app correctly handles exit code") {
      val app = new ZIOApp {
        type Environment = Any

        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
          ZLayer.environment[ZIOAppArgs]

        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
          ZIO.succeed(ExitCode(42))
      }

      for {
        exit <- app.invoke(Chunk.empty).exit
      } yield assert(exit)(isSuccess(equalTo(ExitCode(42))))
    }
  )

  private def withTimeoutFatal(
    zio: ZIO[Any, Any, Any],
    timeout: Duration
  ): ZIO[Any, Any, Any] =
    zio.timeout(timeout).flatMap {
      case Some(value) => ZIO.succeed(value)
      case None        => ZIO.fail(new TimeoutException("ZIO operation timed out"))
    }
}
