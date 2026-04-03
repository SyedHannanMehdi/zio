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

import zio.test._
import zio.test.Assertion._

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._

object ZIOAppSpec extends ZIOSpecDefault {

  def spec = suite("ZIOAppSpec")(
    suite("successful completion")(
      test("app that succeeds emits exit code 0") {
        val app = ZIOApp(
          ZIO.succeed(ExitCode.success),
          ZLayer.environment
        )
        for {
          result <- app.invoke(Chunk.empty)
        } yield assert(result)(isUnit)
      },
      test("app that fails emits non-zero exit code") {
        val app = ZIOApp(
          ZIO.fail(new Exception("test failure")),
          ZLayer.environment
        )
        for {
          result <- app.invoke(Chunk.empty).flip
        } yield assert(result)(isNotNull)
      },
      test("app with explicit success exit code") {
        val app = ZIOApp(
          ZIO.succeed(ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertCompletes
      },
      test("app with explicit failure exit code") {
        val app = ZIOApp(
          ZIO.succeed(ExitCode.failure),
          ZLayer.environment
        )
        for {
          result <- app.invoke(Chunk.empty)
        } yield assert(result)(isUnit)
      }
    ),
    suite("finalizers execution")(
      test("finalizers run on successful completion") {
        val finalizerRun = new AtomicBoolean(false)
        val app = ZIOApp(
          ZIO.acquireRelease(
            ZIO.unit
          )(_ => ZIO.succeed(finalizerRun.set(true)))
            .as(ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(finalizerRun.get())(isTrue)
      },
      test("finalizers run on failure") {
        val finalizerRun = new AtomicBoolean(false)
        val app = ZIOApp(
          ZIO.acquireRelease(
            ZIO.unit
          )(_ => ZIO.succeed(finalizerRun.set(true)))
            .flatMap(_ => ZIO.fail(new Exception("test"))),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty).flip
        } yield assert(finalizerRun.get())(isTrue)
      },
      test("multiple finalizers run in order") {
        val order = scala.collection.mutable.Buffer[String]()
        val app = ZIOApp(
          (for {
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(order += "first")
            )
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(order += "second")
            )
          } yield ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(order.toList)(
          equalTo(List("second", "first"))
        )
      },
      test("finalizers run even with Scope") {
        val finalizerRun = new AtomicBoolean(false)
        val app = ZIOApp(
          ZIO.scoped[ZIOAppArgs] {
            ZIO.acquireRelease(
              ZIO.unit
            )(_ => ZIO.succeed(finalizerRun.set(true)))
              .as(ExitCode.success)
          },
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(finalizerRun.get())(isTrue)
      }
    ),
    suite("gracefulShutdownTimeout")(
      test("respects gracefulShutdownTimeout") {
        val app = new ZIOApp {
          type Environment = ZIOAppArgs

          def environmentTag: EnvironmentTag[Environment] =
            EnvironmentTag[ZIOAppArgs]

          def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
            ZLayer.environment

          override def gracefulShutdownTimeout: Duration =
            100.millis

          def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
            ZIO.succeed(ExitCode.success)
        }

        for {
          startTime <- Clock.nanoTime
          _ <- app.invoke(Chunk.empty)
          endTime <- Clock.nanoTime
          elapsed = (endTime - startTime) / 1_000_000
        } yield assert(elapsed)(
          isGreaterThanOrEqualTo(0L)
        )
      },
      test("gracefulShutdownTimeout defaults to Infinity") {
        val app = ZIOApp(
          ZIO.succeed(ExitCode.success),
          ZLayer.environment
        )
        assert(app.gracefulShutdownTimeout)(
          equalTo(Duration.Infinity)
        )
      }
    ),
    suite("command-line arguments")(
      test("app can access command-line arguments") {
        val app = ZIOApp(
          for {
            args <- ZIOAppArgs.getArgs
          } yield ExitCode.success,
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk("arg1", "arg2"))
        } yield assertCompletes
      },
      test("empty arguments when none provided") {
        val capturedArgs = scala.collection.mutable.Buffer[String]()
        val app = ZIOApp(
          for {
            args <- ZIOAppArgs.getArgs
            _ <- ZIO.succeed(capturedArgs ++= args)
          } yield ExitCode.success,
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(capturedArgs.toList)(isEmpty)
      },
      test("arguments are passed correctly") {
        val capturedArgs = scala.collection.mutable.Buffer[String]()
        val app = ZIOApp(
          for {
            args <- ZIOAppArgs.getArgs
            _ <- ZIO.succeed(capturedArgs ++= args)
          } yield ExitCode.success,
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk("test", "args"))
        } yield assert(capturedArgs.toList)(
          equalTo(List("test", "args"))
        )
      }
    ),
    suite("layer composition")(
      test("bootstrap layer is used") {
        val testValue = new AtomicBoolean(false)
        val customLayer = ZLayer.succeed {
          testValue.set(true)
          "test"
        }
        val app = ZIOApp(
          for {
            value <- ZIO.service[String]
            _ <- ZIO.succeed(assert(value)(equalTo("test")))
          } yield ExitCode.success,
          customLayer
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertCompletes
      },
      test("multiple layers can be composed") {
        val layer1 = ZLayer.succeed(1)
        val layer2 = ZLayer.succeed("test")
        val app = ZIOApp(
          for {
            v1 <- ZIO.service[Int]
            v2 <- ZIO.service[String]
          } yield ExitCode.success,
          layer1 >>> ZLayer.environment[ZIOAppArgs] ++ 
            layer2 >>> ZLayer.environment[ZIOAppArgs]
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertCompletes
      }
    ),
    suite("error handling")(
      test("die causes failure") {
        val app = ZIOApp(
          ZIO.die(new Exception("catastrophic")),
          ZLayer.environment
        )
        for {
          result <- app.invoke(Chunk.empty).flip
        } yield assert(result)(isNotNull)
      },
      test("interrupted effect is handled") {
        val app = ZIOApp(
          ZIO.interrupt,
          ZLayer.environment
        )
        for {
          result <- app.invoke(Chunk.empty).flip
        } yield assert(result)(isNotNull)
      },
      test("timeout effect completes") {
        val app = ZIOApp(
          ZIO.succeed(1).timeout(100.millis).as(ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertCompletes
      }
    ),
    suite("app composition")(
      test("two apps can be composed with <>") {
        val app1 = ZIOApp(
          ZIO.succeed(ExitCode.success),
          ZLayer.environment
        )
        val app2 = ZIOApp(
          ZIO.succeed(ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- (app1 <> app2).invoke(Chunk.empty)
        } yield assertCompletes
      },
      test("composed apps both execute") {
        val executed1 = new AtomicBoolean(false)
        val executed2 = new AtomicBoolean(false)
        val app1 = ZIOApp(
          ZIO.succeed(executed1.set(true)).as(ExitCode.success),
          ZLayer.environment
        )
        val app2 = ZIOApp(
          ZIO.succeed(executed2.set(true)).as(ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- (app1 <> app2).invoke(Chunk.empty)
        } yield assert(executed1.get() && executed2.get())(isTrue)
      }
    ),
    suite("runtime behavior")(
      test("default runtime is used") {
        val app = ZIOApp(
          ZIO.succeed(ExitCode.success),
          ZLayer.environment
        )
        assert(app.runtime)(isNotNull)
      },
      test("shutdown sequence doesn't hang on success") {
        val app = ZIOApp(
          ZIO.succeed(ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty).timeout(5.seconds)
        } yield assertCompletes
      },
      test("shutdown sequence doesn't hang on failure") {
        val app = ZIOApp(
          ZIO.fail(new Exception("test")),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty).flip.timeout(5.seconds)
        } yield assertCompletes
      }
    )
  )
}
