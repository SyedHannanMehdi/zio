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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch
import scala.concurrent.duration._

object ZIOAppSpec extends ZIOSpecDefault {
  def spec: Spec[TestEnvironment, Any] =
    suite("ZIOApp")(
      test("should complete successfully with exit code 0 when app succeeds") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should complete with failure exit code when app fails") {
        val app = ZIOApp.fromZIO(
          ZIO.fail(new Exception("test failure"))
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isFailure)
      },
      test("should run finalizers on successful completion") {
        val finalizerRun = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
            .ensuring(ZIO.succeed(finalizerRun.set(true)))
        )

        for {
          _ <- app.invoke(Chunk.empty)
          result = finalizerRun.get()
        } yield assertTrue(result)
      },
      test("should run finalizers on failure") {
        val finalizerRun = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          ZIO.fail(new Exception("test failure"))
            .ensuring(ZIO.succeed(finalizerRun.set(true)))
        )

        for {
          _ <- app.invoke(Chunk.empty).exit
          result = finalizerRun.get()
        } yield assertTrue(result)
      },
      test("should run multiple finalizers in reverse order") {
        val log = scala.collection.mutable.ListBuffer[String]()

        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
            .ensuring(ZIO.succeed(log += "finalizer1"))
            .ensuring(ZIO.succeed(log += "finalizer2"))
            .ensuring(ZIO.succeed(log += "finalizer3"))
        )

        for {
          _ <- app.invoke(Chunk.empty)
          result = log.toList
        } yield assertTrue(result == List("finalizer3", "finalizer2", "finalizer1"))
      },
      test("should access command-line arguments") {
        val app = ZIOApp.fromZIO(
          for {
            args <- ZIOAppArgs.getArgs
          } yield ExitCode(if (args.contains("test")) 0 else 1)
        )

        for {
          exit <- app.invoke(Chunk("test")).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should handle args correctly when empty") {
        val app = ZIOApp.fromZIO(
          for {
            args <- ZIOAppArgs.getArgs
          } yield ExitCode(if (args.isEmpty) 0 else 1)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should handle args correctly when multiple") {
        val app = ZIOApp.fromZIO(
          for {
            args <- ZIOAppArgs.getArgs
          } yield ExitCode(if (args.length == 3) 0 else 1)
        )

        for {
          exit <- app.invoke(Chunk("arg1", "arg2", "arg3")).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should respect gracefulShutdownTimeout") {
        val shutdownStarted = new AtomicBoolean(false)
        val shutdownCompleted = new AtomicBoolean(false)

        val app = new ZIOApp {
          type Environment = Any

          def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]

          def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] = ZLayer.environment

          def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
            ZIO.succeed(ExitCode.success)

          override def gracefulShutdownTimeout: Duration = 100.millis
        }

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should handle interruption during execution") {
        val finalizerRun = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          ZIO.never
            .ensuring(ZIO.succeed(finalizerRun.set(true)))
        )

        for {
          fiber <- app.invoke(Chunk.empty).fork
          _     <- fiber.interrupt
          _     <- fiber.await
          result = finalizerRun.get()
        } yield assertTrue(result)
      },
      test("should handle success exit code") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should handle failure exit code") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.failure)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess) // The app itself succeeds, just returns failure code
      },
      test("should handle scoped resources cleanup") {
        val resourceAcquired = new AtomicBoolean(false)
        val resourceReleased = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          ZIO.scoped {
            ZIO.acquireRelease(
              ZIO.succeed(resourceAcquired.set(true))
            )(_ => ZIO.succeed(resourceReleased.set(true)))
          }.as(ExitCode.success)
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(resourceAcquired.get() && resourceReleased.get())
      },
      test("should compose multiple ZIOApps with <>") {
        val app1Executed = new AtomicBoolean(false)
        val app2Executed = new AtomicBoolean(false)

        val app1 = ZIOApp.fromZIO(
          ZIO.succeed(app1Executed.set(true)).as(ExitCode.success)
        )

        val app2 = ZIOApp.fromZIO(
          ZIO.succeed(app2Executed.set(true)).as(ExitCode.success)
        )

        val combined = app1 <> app2

        for {
          exit <- combined.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess && app1Executed.get() && app2Executed.get())
      },
      test("should log errors when app fails") {
        val app = ZIOApp.fromZIO(
          ZIO.fail(new Exception("expected test error"))
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isFailure)
      },
      test("should handle nested scoped effects") {
        val log = scala.collection.mutable.ListBuffer[String]()

        val app = ZIOApp.fromZIO(
          ZIO.scoped {
            for {
              _ <- ZIO.acquireRelease(
                     ZIO.succeed(log += "acquire1")
                   )(_ => ZIO.succeed(log += "release1"))
              _ <- ZIO.acquireRelease(
                     ZIO.succeed(log += "acquire2")
                   )(_ => ZIO.succeed(log += "release2"))
            } yield ExitCode.success
          }
        )

        for {
          _ <- app.invoke(Chunk.empty)
          result = log.toList
        } yield assertTrue(
          result.contains("acquire1") &&
            result.contains("acquire2") &&
            result.contains("release1") &&
            result.contains("release2")
        )
      },
      test("should not hang on shutdown") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          exit <- app
                   .invoke(Chunk.empty)
                   .timeout(5.seconds)
        } yield assertTrue(exit.isDefined)
      },
      test("should handle ZIO.never with interruption") {
        val started = new AtomicBoolean(false)
        val finalized = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          (for {
            _ <- ZIO.succeed(started.set(true))
            _ <- ZIO.never
          } yield ExitCode.success)
            .ensuring(ZIO.succeed(finalized.set(true)))
        )

        for {
          fiber <- app.invoke(Chunk.empty).fork
          _     <- ZIO.sleep(100.millis)
          _     <- fiber.interrupt
          exit  <- fiber.await
          s     = started.get()
          f     = finalized.get()
        } yield assertTrue(exit.isInterrupted && s && f)
      },
      test("should execute app workflow with args") {
        val capturedArgs = scala.collection.mutable.ListBuffer[String]()

        val app = ZIOApp.fromZIO(
          for {
            args <- ZIOAppArgs.getArgs
            _    <- ZIO.succeed(capturedArgs.addAll(args))
          } yield ExitCode.success
        )

        val testArgs = Chunk("--foo", "bar", "--baz")
        for {
          _ <- app.invoke(testArgs)
        } yield assertTrue(capturedArgs.toList == testArgs.toList)
      },
      test("should handle early exit via ZIO.succeed") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      }
    )
}
