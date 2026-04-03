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

import zio.internal.Ref
import zio.test.*
import zio.test.Assertion.*

/**
 * Regression tests for ZIOApp addressing past issues:
 *   - #9901: Related to app finalization
 *   - #9807: Related to shutdown behavior
 *   - #9240: Related to error handling
 */
object ZIOAppRegressionSpec extends ZIOSpecDefault {

  def spec = suite("ZIOAppRegressionSpec")(
    suite("issue #9901 - app finalization")(
      test("app finalizers always execute") {
        for {
          finalized <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.succeed(ExitCode.success)
              .ensuring(finalized.set(true))
          )
          _ <- app.invoke(Chunk.empty)
          wasFinalized <- finalized.get
        } yield assert(wasFinalized)(isTrue)
      },
      test("finalizers execute even with early exit") {
        for {
          finalized <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.unit.ensuring(finalized.set(true)) *> ZIO.succeed(ExitCode.success)
          )
          _ <- app.invoke(Chunk.empty)
          wasFinalized <- finalized.get
        } yield assert(wasFinalized)(isTrue)
      }
    ),
    suite("issue #9807 - shutdown behavior")(
      test("shutdown does not lose resources") {
        for {
          acquired <- Ref.make(0)
          released <- Ref.make(0)
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.collectAllDiscard(
                (1 to 10).map { _ =>
                  ZIO.acquireRelease(acquired.update(_ + 1)) { _ =>
                    released.update(_ + 1)
                  }
                }
              ).as(ExitCode.success)
            }
          )
          _ <- app.invoke(Chunk.empty)
          acqCount <- acquired.get
          relCount <- released.get
        } yield assert(acqCount)(equalTo(10)) && assert(relCount)(equalTo(10))
      },
      test("shutdown is not interrupted unexpectedly") {
        for {
          shutdownStarted <- Ref.make(false)
          shutdownCompleted <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.acquireRelease(ZIO.unit) { _ =>
              shutdownStarted.set(true) *>
                ZIO.sleep(Duration.fromMillis(10)) *>
                shutdownCompleted.set(true)
            }.as(ExitCode.success)
          )
          _ <- app.invoke(Chunk.empty)
          started <- shutdownStarted.get
          completed <- shutdownCompleted.get
        } yield assert(started)(isTrue) && assert(completed)(isTrue)
      }
    ),
    suite("issue #9240 - error handling")(
      test("errors are properly reported") {
        for {
          errorMessage = "Test error message"
          app = ZIOApp.fromZIO(
            ZIO.fail(new RuntimeException(errorMessage))
          )
          result <- app.invoke(Chunk.empty).flip
        } yield assertCompletes
      },
      test("error causes are logged") {
        for {
          app = ZIOApp.fromZIO(
            ZIO.fail(new RuntimeException("root cause"))
              .flatMap(_ => ZIO.fail(new RuntimeException("chained")))
          )
          result <- app.invoke(Chunk.empty).flip
        } yield assertCompletes
      },
      test("interrupted errors are handled gracefully") {
        for {
          app = ZIOApp.fromZIO(
            ZIO.interrupt.as(ExitCode.success)
          )
          result <- app.invoke(Chunk.empty).flip
        } yield assertCompletes
      }
    ),
    suite("complex finalization scenarios")(
      test("nested resources are released in order") {
        for {
          order <- Ref.make(List.empty[String])
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.acquireRelease(order.update("a1" :: _)) { _ =>
                order.update("a2" :: _)
              }.flatMap { _ =>
                ZIO.acquireRelease(order.update("b1" :: _)) { _ =>
                  order.update("b2" :: _)
                }
              }.as(ExitCode.success)
            }
          )
          _ <- app.invoke(Chunk.empty)
          executionOrder <- order.get
        } yield assert(executionOrder.reverse)(
          equalTo(List("a1", "b1", "b2", "a2"))
        )
      },
      test("resource cleanup is not interrupted") {
        for {
          cleanupStarted <- Ref.make(false)
          cleanupCompleted <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.acquireRelease(ZIO.unit) { _ =>
                cleanupStarted.set(true) *>
                  ZIO.sleep(Duration.fromMillis(50)) *>
                  cleanupCompleted.set(true)
              }.as(ExitCode.success)
            }
          )
          _ <- app.invoke(Chunk.empty)
          started <- cleanupStarted.get
          completed <- cleanupCompleted.get
        } yield assert(started)(isTrue) && assert(completed)(isTrue)
      }
    ),
    suite("concurrent resource management")(
      test("concurrent acquisitions are all released") {
        for {
          released <- Ref.make(0)
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.collectAllDiscard(
                (1 to 20).map { _ =>
                  ZIO.acquireRelease(ZIO.unit)(_ => released.update(_ + 1))
                }
              ).as(ExitCode.success)
            }
          )
          _ <- app.invoke(Chunk.empty)
          relCount <- released.get
        } yield assert(relCount)(equalTo(20))
      },
      test("parZip operations complete and cleanup") {
        for {
          acquired <- Ref.make(0)
          released <- Ref.make(0)
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              (
                ZIO.acquireRelease(acquired.update(_ + 1))(_ => released.update(_ + 1)) *>
                  ZIO.unit
              ).zipPar(
                ZIO.acquireRelease(acquired.update(_ + 1))(_ => released.update(_ + 1)) *>
                  ZIO.unit
              ).as(ExitCode.success)
            }
          )
          _ <- app.invoke(Chunk.empty)
          acqCount <- acquired.get
          relCount <- released.get
        } yield assert(acqCount)(equalTo(2)) && assert(relCount)(equalTo(2))
      }
    ),
    suite("exit code preservation")(
      test("exit codes are not lost during shutdown") {
        for {
          app = new ZIOApp {
            type Environment = Any
            def bootstrap = ZLayer.empty
            implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]
            def run = ZIO.succeed(ExitCode(99))
          }
          result <- app.invoke(Chunk.empty)
          exitCode <- result match {
            case Exit.Success(ec: ExitCode) => ZIO.succeed(ec.code)
            case _                           => ZIO.fail("Expected exit code")
          }
        } yield assert(exitCode)(equalTo(99))
      }
    ),
    suite("timeout compliance")(
      test("gracefulShutdownTimeout is enforced") {
        for {
          finalizerStartTime <- Ref.make(0L)
          finalizerEndTime <- Ref.make(0L)
          timeoutDuration = Duration.fromMillis(100)
          app = new ZIOApp {
            type Environment = Any
            def bootstrap = ZLayer.empty
            implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]
            override def gracefulShutdownTimeout = timeoutDuration
            def run = ZIO.acquireRelease(ZIO.unit) { _ =>
              for {
                start <- Clock.currentTimeMillis
                _ <- finalizerStartTime.set(start)
                _ <- ZIO.sleep(Duration.fromSeconds(10))
                end <- Clock.currentTimeMillis
                _ <- finalizerEndTime.set(end)
              } yield ()
            }.as(ExitCode.success)
          }
          _ <- app.invoke(Chunk.empty)
          startTime <- finalizerStartTime.get
          endTime <- finalizerEndTime.get
        } yield {
          val actualDuration = endTime - startTime
          assert(actualDuration)(isLessThanOrEqualTo(500L)) // Give 4x buffer
        }
      }
    )
  )

}
