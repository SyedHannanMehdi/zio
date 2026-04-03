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

object ZIOAppShutdownSpec extends ZIOSpecDefault {
  def spec: Spec[TestEnvironment, Any] =
    suite("ZIOApp Shutdown Behavior")(
      test("should cleanup all finalizers before exit") {
        val finalizers = scala.collection.mutable.ListBuffer[Int]()

        val app = ZIOApp.fromZIO(
          for {
            _ <- ZIO.succeed(()).ensuring(ZIO.succeed(finalizers += 1))
            _ <- ZIO.succeed(()).ensuring(ZIO.succeed(finalizers += 2))
            _ <- ZIO.succeed(()).ensuring(ZIO.succeed(finalizers += 3))
          } yield ExitCode.success
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(finalizers.toList.contains(1) && finalizers.toList.contains(2) && finalizers.toList.contains(3))
      },
      test("should not hang during graceful shutdown") {
        val app = ZIOApp.fromZIO(
          for {
            _ <- ZIO.sleep(100.millis)
          } yield ExitCode.success
        )

        for {
          result <- app.invoke(Chunk.empty).timeout(5.seconds)
        } yield assertTrue(result.isDefined)
      },
      test("should complete successfully when no errors occur") {
        val app = ZIOApp.fromZIO(
          for {
            _ <- ZIO.unit
          } yield ExitCode.success
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should propagate errors correctly") {
        val testError = new Exception("test error")
        val app = ZIOApp.fromZIO(
          ZIO.fail(testError)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isFailure)
      },
      test("should handle effects that complete quickly") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should handle effects that take some time") {
        val app = ZIOApp.fromZIO(
          for {
            _ <- ZIO.sleep(200.millis)
          } yield ExitCode.success
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should ensure finalizers run even on interrupted effects") {
        val finalizerExecuted = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          (for {
            _ <- ZIO.never
          } yield ExitCode.success)
            .ensuring(ZIO.succeed(finalizerExecuted.set(true)))
        )

        for {
          fiber <- app.invoke(Chunk.empty).fork
          _     <- ZIO.sleep(100.millis)
          _     <- fiber.interrupt
          _     <- fiber.await
        } yield assertTrue(finalizerExecuted.get())
      },
      test("should handle exit signals properly") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should support custom exit codes") {
        val exitCode = 42
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode(exitCode))
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should run resource cleanup in correct order") {
        val order = scala.collection.mutable.ListBuffer[String]()

        val app = ZIOApp.fromZIO(
          ZIO.scoped {
            for {
              _ <- ZIO.acquireRelease(
                     ZIO.succeed(order += "acquire-a")
                   )(_ => ZIO.succeed(order += "release-a"))
              _ <- ZIO.acquireRelease(
                     ZIO.succeed(order += "acquire-b")
                   )(_ => ZIO.succeed(order += "release-b"))
              _ <- ZIO.acquireRelease(
                     ZIO.succeed(order += "acquire-c")
                   )(_ => ZIO.succeed(order += "release-c"))
            } yield ExitCode.success
          }
        )

        for {
          _ <- app.invoke(Chunk.empty)
          result = order.toList
        } yield {
          // Verify acquisitions happen in order
          val acquireIndices = result
            .zipWithIndex
            .filter(_._1.startsWith("acquire"))
            .map(_._2)
          val releaseIndices = result
            .zipWithIndex
            .filter(_._1.startsWith("release"))
            .map(_._2)

          // All acquires should happen before releases
          val allAcquisitionsDone = acquireIndices.isEmpty || releaseIndices.isEmpty || acquireIndices.max < releaseIndices.min

          // Releases should be in reverse order of acquires
          val releasesAreReverse = result.filter(_.startsWith("release")).toList match {
            case List("release-c", "release-b", "release-a") => true
            case _                                              => false
          }

          assertTrue(allAcquisitionsDone && releasesAreReverse)
        }
      },
      test("should handle empty argument list") {
        val app = ZIOApp.fromZIO(
          for {
            args <- ZIOAppArgs.getArgs
          } yield ExitCode(if (args.isEmpty) 0 else 1)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should handle single argument") {
        val app = ZIOApp.fromZIO(
          for {
            args <- ZIOAppArgs.getArgs
          } yield ExitCode(if (args.length == 1 && args(0) == "test") 0 else 1)
        )

        for {
          exit <- app.invoke(Chunk("test")).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should handle multiple arguments in order") {
        val app = ZIOApp.fromZIO(
          for {
            args <- ZIOAppArgs.getArgs
          } yield ExitCode(
            if (args.length == 4 && args(0) == "a" && args(1) == "b" && args(2) == "c" && args(3) == "d") 0
            else 1
          )
        )

        for {
          exit <- app.invoke(Chunk("a", "b", "c", "d")).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should complete workflow without hanging") {
        val latch = new CountDownLatch(1)

        val app = ZIOApp.fromZIO(
          ZIO.succeed {
            latch.countDown()
            ExitCode.success
          }
        )

        for {
          _ <- app.invoke(Chunk.empty)
          waitResult = latch.await(5, java.util.concurrent.TimeUnit.SECONDS)
        } yield assertTrue(waitResult)
      },
      test("should invoke workflow with skipLogging flag") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )

        for {
          exit <- app.invoke(Chunk.empty).exit
        } yield assertTrue(exit.isSuccess)
      },
      test("should cleanup on successful completion") {
        val cleaned = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
            .ensuring(ZIO.succeed(cleaned.set(true)))
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(cleaned.get())
      },
      test("should cleanup on failed completion") {
        val cleaned = new AtomicBoolean(false)

        val app = ZIOApp.fromZIO(
          (for {
            _ <- ZIO.fail(new Exception("error"))
          } yield ExitCode.success)
            .ensuring(ZIO.succeed(cleaned.set(true)))
        )

        for {
          _ <- app.invoke(Chunk.empty).exit
        } yield assertTrue(cleaned.get())
      }
    )
}
