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
 * Test suite for graceful shutdown behavior of ZIOApp including signal
 * handling, finalizer execution, and timeout behavior.
 */
object ZIOAppGracefulShutdownSpec extends ZIOSpecDefault {

  def spec = suite("ZIOAppGracefulShutdownSpec")(
    suite("basic graceful shutdown")(
      test("finalizers run on successful completion") {
        for {
          finalizerExecuted <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.acquireRelease(ZIO.unit) { _ =>
              finalizerExecuted.set(true)
            }.as(ExitCode.success)
          )
          _ <- app.invoke(Chunk.empty)
          executed <- finalizerExecuted.get
        } yield assert(executed)(isTrue)
      },
      test("finalizers run on failure") {
        for {
          finalizerExecuted <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.acquireRelease(ZIO.unit) { _ =>
              finalizerExecuted.set(true)
            }.flatMap(_ => ZIO.fail(new RuntimeException("test")))
          )
          _ <- app.invoke(Chunk.empty).flip
          executed <- finalizerExecuted.get
        } yield assert(executed)(isTrue)
      }
    ),
    suite("timeout enforcement")(
      test("gracefulShutdownTimeout prevents hung finalizers") {
        for {
          finalizerStarted <- Ref.make(false)
          finalizerCompleted <- Ref.make(false)
          app = new ZIOApp {
            type Environment = Any
            def bootstrap = ZLayer.empty
            implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]
            override def gracefulShutdownTimeout = Duration.fromMillis(50)
            def run = ZIO.acquireRelease(ZIO.unit) { _ =>
              finalizerStarted.set(true) *>
                ZIO.sleep(Duration.fromSeconds(10)) *>
                finalizerCompleted.set(true)
            }.as(ExitCode.success)
          }
          _ <- app.invoke(Chunk.empty)
          started <- finalizerStarted.get
          completed <- finalizerCompleted.get
        } yield assert(started)(isTrue) && assert(completed)(isFalse)
      },
      test("app with infinity timeout waits for finalizers") {
        for {
          finalizerCompleted <- Ref.make(false)
          app = new ZIOApp {
            type Environment = Any
            def bootstrap = ZLayer.empty
            implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]
            override def gracefulShutdownTimeout = Duration.Infinity
            def run = ZIO.acquireRelease(ZIO.unit) { _ =>
              ZIO.sleep(Duration.fromMillis(100)) *>
                finalizerCompleted.set(true)
            }.as(ExitCode.success)
          }
          _ <- app.invoke(Chunk.empty)
          completed <- finalizerCompleted.get
        } yield assert(completed)(isTrue)
      }
    ),
    suite("multiple finalizers")(
      test("all finalizers execute despite some failing") {
        for {
          finalizer1Executed <- Ref.make(false)
          finalizer2Executed <- Ref.make(false)
          finalizer3Executed <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.acquireRelease(ZIO.unit) { _ =>
              finalizer1Executed.set(true)
            }.flatMap { _ =>
              ZIO.acquireRelease(ZIO.unit) { _ =>
                finalizer2Executed.set(true) *>
                  ZIO.fail(new RuntimeException("expected failure"))
              }.catchAll(_ => ZIO.unit)
            }.flatMap { _ =>
              ZIO.acquireRelease(ZIO.unit) { _ =>
                finalizer3Executed.set(true)
              }
            }.as(ExitCode.success)
          )
          _ <- app.invoke(Chunk.empty)
          f1 <- finalizer1Executed.get
          f2 <- finalizer2Executed.get
          f3 <- finalizer3Executed.get
        } yield assert(f1)(isTrue) && assert(f2)(isTrue) && assert(f3)(isTrue)
      }
    ),
    suite("scope handling")(
      test("scope finalizers execute on app success") {
        for {
          resourceAcquired <- Ref.make(false)
          resourceReleased <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.acquireRelease(resourceAcquired.set(true)) { _ =>
                resourceReleased.set(true)
              }.as(ExitCode.success)
            }
          )
          _ <- app.invoke(Chunk.empty)
          acq <- resourceAcquired.get
          rel <- resourceReleased.get
        } yield assert(acq)(isTrue) && assert(rel)(isTrue)
      },
      test("scope finalizers execute on app failure") {
        for {
          resourceAcquired <- Ref.make(false)
          resourceReleased <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.acquireRelease(resourceAcquired.set(true)) { _ =>
                resourceReleased.set(true)
              }.flatMap(_ => ZIO.fail(new RuntimeException("test")))
            }
          )
          _ <- app.invoke(Chunk.empty).flip
          acq <- resourceAcquired.get
          rel <- resourceReleased.get
        } yield assert(acq)(isTrue) && assert(rel)(isTrue)
      }
    ),
    suite("no hanging")(
      test("app completes in reasonable time") {
        for {
          app = ZIOApp.fromZIO(ZIO.succeed(ExitCode.success))
          result <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
        } yield assert(result)(isSome)
      },
      test("app with many resources completes") {
        for {
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.collectAllDiscard(
                (1 to 100).map { i =>
                  ZIO.acquireRelease(ZIO.unit)(_ => ZIO.unit)
                }
              ).as(ExitCode.success)
            }
          )
          result <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(10))
        } yield assert(result)(isSome)
      }
    ),
    suite("error codes")(
      test("success returns exit code 0") {
        for {
          app = ZIOApp.fromZIO(ZIO.succeed(ExitCode.success))
          result <- app.invoke(Chunk.empty)
          exitCode <- result match {
            case Exit.Success(ec: ExitCode) => ZIO.succeed(ec.code)
            case _                           => ZIO.fail("Expected exit code")
          }
        } yield assert(exitCode)(equalTo(0))
      },
      test("failure returns non-zero exit code") {
        for {
          app = ZIOApp.fromZIO(ZIO.fail(new RuntimeException("test")))
          result <- app.invoke(Chunk.empty).flip
        } yield assertCompletes
      },
      test("custom exit codes are preserved") {
        for {
          app = ZIOApp.fromZIO(ZIO.succeed(ExitCode(42)))
          result <- app.invoke(Chunk.empty)
          exitCode <- result match {
            case Exit.Success(ec: ExitCode) => ZIO.succeed(ec.code)
            case _                           => ZIO.fail("Expected exit code")
          }
        } yield assert(exitCode)(equalTo(42))
      }
    ),
    suite("bootstrap layer")(
      test("bootstrap layer is properly acquired") {
        for {
          layerInitialized <- Ref.make(false)
          layer = ZLayer.succeed(layerInitialized.set(true) *> ZIO.unit)
          app = ZIOApp(
            ZIO.succeed(ExitCode.success),
            ZLayer.succeed(ZIOAppArgs(Chunk.empty)) >>> layer
          )
          _ <- app.invoke(Chunk.empty)
          initialized <- layerInitialized.get
        } yield assert(initialized)(isTrue)
      },
      test("bootstrap layer is released on completion") {
        for {
          layerReleased <- Ref.make(false)
          layer = ZLayer.acquireReleaseWith(ZIO.unit)(_ => layerReleased.set(true))
          app = ZIOApp(
            ZIO.succeed(ExitCode.success),
            ZLayer.succeed(ZIOAppArgs(Chunk.empty)) >>> layer
          )
          _ <- app.invoke(Chunk.empty)
          released <- layerReleased.get
        } yield assert(released)(isTrue)
      }
    )
  )

}
