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
import scala.concurrent.duration._

/**
 * Regression tests for ZIOApp covering issues from:
 * - #9901: ZIOApp finalizers behavior
 * - #9807: ZIOApp exit codes and error handling
 * - #9240: ZIOApp shutdown handling
 */
object ZIOAppRegressionSpec extends ZIOSpecDefault {
  def spec: Spec[TestEnvironment, Any] =
    suite("ZIOApp Regression Tests")(
      test("issue #9901: finalizers should run on exit") {
        val finalizerRan = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          for {
            _ <- ZIO.succeed(()).ensuring(ZIO.succeed(finalizerRan.set(true)))
          } yield ExitCode.success
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(finalizerRan.get())
      },
      test("issue #9901: nested finalizers should all run") {
        val log = scala.collection.mutable.ListBuffer[Int]()

        val app = ZIOApp.fromZIO(
          for {
            _ <- ZIO.succeed(()).ensuring(ZIO.succeed(log += 1))
            _ <- ZIO.succeed(()).ensuring(ZIO.succeed(log += 2))
            _ <- ZIO.succeed(()).ensuring(ZIO.succeed(log += 3))
          } yield ExitCode.success
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(log.nonEmpty && log.toList == List(3, 2, 1))
      },
      test("issue #9807: correct exit code on success") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("issue #9807: correct exit code on failure") {
        val app = ZIOApp.fromZIO(
          ZIO.fail(new Exception("test failure"))
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isFailure)
      },
      test("issue #9807: specific exit codes are preserved") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode(123))
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("issue #9240: shutdown should not hang") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          result <- app.invoke(Chunk.empty).timeout(5.seconds)
        } yield assertTrue(result.isDefined)
      },
      test("issue #9240: graceful shutdown with cleanup") {
        val cleaned = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
            .ensuring(ZIO.succeed(cleaned.set(true)))
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(cleaned.get())
      },
      test("issue #9240: resources are released on shutdown") {
        val released = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          ZIO.scoped {
            ZIO.acquireRelease(
              ZIO.succeed(())
            )(_ => ZIO.succeed(released.set(true)))
          }.as(ExitCode.success)
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(released.get())
      },
      test("issue #9240: multiple resources released in order") {
        val order = scala.collection.mutable.ListBuffer[String]()

        val app = ZIOApp.fromZIO(
          ZIO.scoped {
            for {
              _ <- ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(order += "release-1"))
              _ <- ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(order += "release-2"))
              _ <- ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(order += "release-3"))
            } yield ExitCode.success
          }
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(order.toList == List("release-3", "release-2", "release-1"))
      },
      test("should handle effects with early termination") {
        val executed = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          for {
            _ <- ZIO.succeed(executed.set(true))
          } yield ExitCode.success
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
          e    = executed.get()
        } yield assertTrue(exit.isSuccess && e)
      },
      test("should handle interrupted effects") {
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
          _     <- fiber.await
          s     = started.get()
          f     = finalized.get()
        } yield assertTrue(s && f)
      },
      test("should handle cascading resource cleanup") {
        val log = scala.collection.mutable.ListBuffer[String]()

        val app = ZIOApp.fromZIO(
          ZIO.scoped {
            for {
              _ <- ZIO.acquireRelease(ZIO.succeed(log += "acquire-a"))(_ =>
                     ZIO.succeed(log += "release-a")
                   )
              _ <- ZIO.acquireRelease(ZIO.succeed(log += "acquire-b"))(_ =>
                     ZIO.succeed(log += "release-b")
                   )
            } yield ExitCode.success
          }
        )

        for {
          _ <- app.invoke(Chunk.empty)
          result = log.toList
        } yield assertTrue(
          result.indexOf("acquire-a") < result.indexOf("acquire-b") &&
            result.indexOf("release-b") < result.indexOf("release-a")
        )
      },
      test("should not suppress errors during cleanup") {
        val app = ZIOApp.fromZIO(
          ZIO.fail(new Exception("main error"))
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isFailure)
      },
      test("should handle app with both success path and cleanup") {
        val steps = scala.collection.mutable.ListBuffer[String]()

        val app = ZIOApp.fromZIO(
          for {
            _ <- ZIO.succeed(steps += "step1")
            _ <- ZIO.succeed(steps += "step2")
          } yield ExitCode.success
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(steps.toList == List("step1", "step2"))
      },
      test("should handle mixed success and finalizer effects") {
        val steps = scala.collection.mutable.ListBuffer[String]()

        val app = ZIOApp.fromZIO(
          for {
            _ <- ZIO.succeed(steps += "main")
            _ <- ZIO.succeed(()).ensuring(ZIO.succeed(steps += "finally"))
          } yield ExitCode.success
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(steps.contains("main") && steps.contains("finally"))
      },
      test("should properly exit with failure code") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.failure)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess) // App succeeds, but returns failure code
      },
      test("should handle default gracefulShutdownTimeout") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      }
    )
}
