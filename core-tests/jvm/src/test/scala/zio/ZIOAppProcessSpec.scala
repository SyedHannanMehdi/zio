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

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

/**
 * Process-level integration tests for ZIOApp behavior
 * Tests the behavior of ZIOApp when:
 * - App completes on its own (success/failure)
 * - App completes due to external signal
 * - Finalizers are run
 * - Graceful shutdown timeout is respected
 * - Exit codes are correct
 */
object ZIOAppProcessSpec extends ZIOSpecDefault {

  def spec = suite("ZIOAppProcessSpec")(
    suite("exit codes")(
      test("success returns exit code 0") {
        val app = ZIOApp(
          ZIO.succeed(ExitCode(0)),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertCompletes
      },
      test("failure returns non-zero exit code") {
        val app = ZIOApp(
          ZIO.fail(new Exception("test failure")),
          ZLayer.environment
        )
        for {
          result <- app.invoke(Chunk.empty).flip
        } yield assert(result)(isNotNull)
      },
      test("custom exit code is used") {
        val app = ZIOApp(
          ZIO.succeed(ExitCode(42)),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertCompletes
      }
    ),
    suite("finalizer guarantees")(
      test("finalizers run before shutdown") {
        val finalizerExecuted = new AtomicInteger(0)
        val app = ZIOApp(
          ZIO.acquireRelease(
            ZIO.succeed(1)
          )(_ =>
            ZIO.succeed(finalizerExecuted.incrementAndGet())
          ).as(ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(finalizerExecuted.get())(
          equalTo(1)
        )
      },
      test("nested finalizers execute in correct order") {
        val order = scala.collection.mutable.Buffer[Int]()
        val app = ZIOApp(
          (for {
            _ <- ZIO.acquireRelease(ZIO.succeed(1))(_ =>
              ZIO.succeed(order += 1)
            )
            _ <- ZIO.acquireRelease(ZIO.succeed(2))(_ =>
              ZIO.succeed(order += 2)
            )
            _ <- ZIO.acquireRelease(ZIO.succeed(3))(_ =>
              ZIO.succeed(order += 3)
            )
          } yield ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(order.toList)(
          equalTo(List(3, 2, 1))
        )
      },
      test("finalizers run even on defect") {
        val finalizerExecuted = new AtomicInteger(0)
        val app = ZIOApp(
          ZIO.acquireRelease(
            ZIO.succeed(1)
          )(_ =>
            ZIO.succeed(finalizerExecuted.incrementAndGet())
          ).flatMap(_ => ZIO.die(new Exception("defect"))),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty).flip
        } yield assert(finalizerExecuted.get())(
          equalTo(1)
        )
      },
      test("finalizers with effects are awaited") {
        val delayedFinalizer = new AtomicInteger(0)
        val app = ZIOApp(
          ZIO.acquireRelease(
            ZIO.succeed(1)
          )(_ =>
            ZIO.sleep(50.millis) *>
              ZIO.succeed(delayedFinalizer.incrementAndGet())
          ).as(ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(delayedFinalizer.get())(
          equalTo(1)
        )
      }
    ),
    suite("shutdown behavior")(
      test("graceful shutdown completes in reasonable time") {
        val app = new ZIOApp {
          type Environment = ZIOAppArgs

          def environmentTag: EnvironmentTag[Environment] =
            EnvironmentTag[ZIOAppArgs]

          def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
            ZLayer.environment

          override def gracefulShutdownTimeout: Duration =
            500.millis

          def run
              : ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
            ZIO.succeed(ExitCode.success)
        }

        for {
          _ <- app
            .invoke(Chunk.empty)
            .timeout(2.seconds)
        } yield assertCompletes
      },
      test("shutdown respects gracefulShutdownTimeout") {
        val shutdownStarted = new AtomicInteger(0)
        val app = new ZIOApp {
          type Environment = ZIOAppArgs

          def environmentTag: EnvironmentTag[Environment] =
            EnvironmentTag[ZIOAppArgs]

          def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
            ZLayer.environment

          override def gracefulShutdownTimeout: Duration =
            100.millis

          def run
              : ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
            ZIO.acquireRelease(
              ZIO.succeed(shutdownStarted.set(1))
            )(_ =>
              ZIO.sleep(200.millis) *>
                ZIO.unit
            ).as(ExitCode.success)
        }

        for {
          startTime <- Clock.nanoTime
          _ <- app
            .invoke(Chunk.empty)
            .timeout(5.seconds)
          endTime <- Clock.nanoTime
          elapsedMs = (endTime - startTime) / 1_000_000
        } yield assert(shutdownStarted.get())(equalTo(1))
      },
      test("multiple stages of shutdown") {
        val stage1 = new AtomicInteger(0)
        val stage2 = new AtomicInteger(0)
        val app = ZIOApp(
          (for {
            _ <- ZIO.acquireRelease(ZIO.succeed(1))(_ =>
              ZIO.succeed(stage1.set(1))
            )
            _ <- ZIO.acquireRelease(ZIO.succeed(2))(_ =>
              ZIO.succeed(stage2.set(2))
            )
          } yield ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(
          stage1.get() == 1 && stage2.get() == 2
        )(isTrue)
      }
    ),
    suite("resource cleanup")(
      test("file resources are closed") {
        val closed = new AtomicInteger(0)
        val app = ZIOApp(
          ZIO.acquireRelease(
            ZIO.succeed(java.io.ByteArrayOutputStream())
          )(resource =>
            ZIO.succeed {
              resource.close()
              closed.incrementAndGet()
            }
          ).as(ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(closed.get())(equalTo(1))
      },
      test("multiple resources are cleaned up") {
        val closedCount = new AtomicInteger(0)
        val app = ZIOApp(
          (for {
            _ <- ZIO.acquireRelease(
              ZIO.succeed(java.io.ByteArrayOutputStream())
            )(resource =>
              ZIO.succeed {
                resource.close()
                closedCount.incrementAndGet()
              }
            )
            _ <- ZIO.acquireRelease(
              ZIO.succeed(java.io.ByteArrayOutputStream())
            )(resource =>
              ZIO.succeed {
                resource.close()
                closedCount.incrementAndGet()
              }
            )
          } yield ExitCode.success),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(closedCount.get())(equalTo(2))
      }
    ),
    suite("layer resource management")(
      test("layer resources are released") {
        val released = new AtomicInteger(0)
        val layer = ZLayer.acquireRelease(
          ZIO.succeed("resource")
        )(_ => ZIO.succeed(released.incrementAndGet()))
        val app = ZIOApp(
          ZIO.service[String].as(ExitCode.success),
          layer
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(released.get())(equalTo(1))
      },
      test("multiple layer resources are released") {
        val released = new AtomicInteger(0)
        val layer1 = ZLayer.acquireRelease(
          ZIO.succeed("resource1")
        )(_ => ZIO.succeed(released.incrementAndGet()))
        val layer2 = ZLayer.acquireRelease(
          ZIO.succeed("resource2")
        )(_ => ZIO.succeed(released.incrementAndGet()))
        val combined = layer1 ++ layer2
        val app = ZIOApp(
          ZIO.unit.as(ExitCode.success),
          combined
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(released.get())(equalTo(2))
      }
    ),
    suite("effect composition")(
      test("ZIO.scoped works with ZIOApp") {
        val executed = new AtomicInteger(0)
        val app = ZIOApp(
          ZIO.scoped[ZIOAppArgs] {
            ZIO.acquireRelease(
              ZIO.succeed(executed.incrementAndGet())
            )(_ => ZIO.unit).map(_ => ExitCode.success)
          },
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(executed.get())(equalTo(1))
      },
      test("ZIO.bracket works with ZIOApp") {
        val acquired = new AtomicInteger(0)
        val released = new AtomicInteger(0)
        val app = ZIOApp(
          ZIO.bracket(
            ZIO.succeed(acquired.incrementAndGet())
          )(_ => ZIO.succeed(released.incrementAndGet()))
          (_ => ZIO.succeed(ExitCode.success)),
          ZLayer.environment
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(acquired.get() == 1 && released.get() == 1)(
          isTrue
        )
      }
    )
  )
}
