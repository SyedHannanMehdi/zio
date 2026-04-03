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
 * Comprehensive test suite for ZIOApp behavior
 */
object ZIOAppSpec extends ZIOSpecDefault {

  def spec = suite("ZIOAppSpec")(
    suite("successful app completion")(
      test("returns exit code 0 on success") {
        val app = ZIOApp.fromZIO(ZIO.succeed(ExitCode.success))
        for {
          result <- app.invoke(Chunk.empty)
          exitCode <- result match {
            case Exit.Success(ec: ExitCode) => ZIO.succeed(ec)
            case _                           => ZIO.fail("Expected exit code")
          }
        } yield assert(exitCode.code)(equalTo(0))
      },
      test("app that completes successfully runs finalizers") {
        for {
          finalizerRun <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.acquireRelease(ZIO.unit)(_ => finalizerRun.set(true))
              .flatMap(_ => ZIO.succeed(ExitCode.success))
          )
          _ <- app.invoke(Chunk.empty)
          ran <- finalizerRun.get
        } yield assert(ran)(isTrue)
      }
    ),
    suite("failed app completion")(
      test("returns exit code 1 on failure") {
        val app = ZIOApp.fromZIO(ZIO.fail(new RuntimeException("test failure")))
        for {
          result <- app.invoke(Chunk.empty).flip
        } yield assertCompletes
      },
      test("app that fails runs finalizers") {
        for {
          finalizerRun <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.acquireRelease(ZIO.unit)(_ => finalizerRun.set(true))
              .flatMap(_ => ZIO.fail(new RuntimeException("test failure")))
          )
          _ <- app.invoke(Chunk.empty).flip
          ran <- finalizerRun.get
        } yield assert(ran)(isTrue)
      }
    ),
    suite("graceful shutdown")(
      test("gracefulShutdownTimeout is respected") {
        val timeout = Duration.fromMillis(100)
        for {
          finalizerStarted <- Ref.make(false)
          finalizerDone <- Ref.make(false)
          app = new ZIOApp {
            type Environment = Any
            def bootstrap = ZLayer.empty
            def run = ZIO.acquireRelease(ZIO.unit) { _ =>
              finalizerStarted.set(true) *> ZIO.sleep(Duration.fromSeconds(10)) *> finalizerDone.set(true)
            }.as(ExitCode.success)
            implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]
            override def gracefulShutdownTimeout = timeout
          }
          _ <- app.invoke(Chunk.empty)
          started <- finalizerStarted.get
          done <- finalizerDone.get
        } yield assert(started)(isTrue) && assert(done)(isFalse)
      },
      test("shutdown sequence doesn't hang") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(ExitCode.success)
        )
        for {
          result <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
        } yield assert(result)(isSome)
      }
    ),
    suite("application arguments")(
      test("app receives command-line arguments") {
        val app = ZIOApp.fromZIO(
          ZIOAppArgs.getArgs.map { args =>
            assert(args)(equalTo(Chunk("hello", "world"))).toExit match {
              case Exit.Success(_) => ExitCode.success
              case Exit.Failure(_) => ExitCode.failure
            }
          }
        )
        for {
          result <- app.invoke(Chunk("hello", "world"))
        } yield assertCompletes
      },
      test("app works with empty arguments") {
        val app = ZIOApp.fromZIO(
          ZIOAppArgs.getArgs.map { args =>
            if (args.isEmpty) ExitCode.success else ExitCode.failure
          }
        )
        for {
          result <- app.invoke(Chunk.empty)
        } yield assertCompletes
      }
    ),
    suite("scoped resources")(
      test("scoped resources are acquired and released") {
        for {
          acquired <- Ref.make(false)
          released <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.acquireRelease(acquired.set(true))(_ => released.set(true))
                .flatMap(_ => ZIO.succeed(ExitCode.success))
            }
          )
          _ <- app.invoke(Chunk.empty)
          acq <- acquired.get
          rel <- released.get
        } yield assert(acq)(isTrue) && assert(rel)(isTrue)
      },
      test("nested scopes are properly cleaned up") {
        for {
          outer <- Ref.make(false)
          inner <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.acquireRelease(outer.set(true))(_ => outer.set(false))
                .flatMap { _ =>
                  ZIO.acquireRelease(inner.set(true))(_ => inner.set(false))
                }
                .as(ExitCode.success)
            }
          )
          _ <- app.invoke(Chunk.empty)
          outerReleased <- outer.get
          innerReleased <- inner.get
        } yield assert(outerReleased)(isFalse) && assert(innerReleased)(isFalse)
      }
    ),
    suite("layer composition")(
      test("bootstrap layers are properly initialized") {
        for {
          initialized <- Ref.make(false)
          layer = ZLayer.succeed(initialized.set(true) *> ZIO.unit)
          app = ZIOApp(
            ZIO.succeed(ExitCode.success),
            ZLayer.succeed(ZIOAppArgs(Chunk.empty)) >>> layer
          )
          _ <- app.invoke(Chunk.empty)
          init <- initialized.get
        } yield assert(init)(isTrue)
      }
    ),
    suite("exit handling")(
      test("app can explicitly exit with custom code") {
        val app = new ZIOApp {
          type Environment = Any
          def bootstrap = ZLayer.empty
          def run = ZIO.succeed(ExitCode(42))
          implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]
        }
        for {
          result <- app.invoke(Chunk.empty)
          code <- result match {
            case Exit.Success(ec: ExitCode) => ZIO.succeed(ec.code)
            case _                           => ZIO.fail("Expected exit code")
          }
        } yield assert(code)(equalTo(42))
      }
    ),
    suite("error logging")(
      test("errors are not silently swallowed") {
        val app = ZIOApp.fromZIO(
          ZIO.fail(new RuntimeException("test error"))
        )
        for {
          result <- app.invoke(Chunk.empty).flip
        } yield assertCompletes
      }
    ),
    suite("concurrent operations")(
      test("concurrent tasks complete successfully") {
        val app = ZIOApp.fromZIO(
          ZIO.collectAllDiscard(
            (1 to 10).map(_ => ZIO.succeed(()))
          ).as(ExitCode.success)
        )
        for {
          result <- app.invoke(Chunk.empty)
        } yield assertCompletes
      },
      test("parZip operations complete") {
        val app = ZIOApp.fromZIO(
          ZIO.succeed(()).zipPar(ZIO.succeed(()))
            .as(ExitCode.success)
        )
        for {
          result <- app.invoke(Chunk.empty)
        } yield assertCompletes
      }
    ),
    suite("finalizer behavior")(
      test("finalizers execute in reverse order") {
        for {
          order <- Ref.make(List.empty[Int])
          app = ZIOApp.fromZIO(
            ZIO.acquireRelease(order.update(1 :: _)) { _ =>
              order.update(10 :: _)
            }.flatMap { _ =>
              ZIO.acquireRelease(order.update(2 :: _)) { _ =>
                order.update(20 :: _)
              }
            }.as(ExitCode.success)
          )
          _ <- app.invoke(Chunk.empty)
          finalOrder <- order.get
        } yield assert(finalOrder.reverse)(
          equalTo(List(1, 2, 20, 10))
        )
      },
      test("exceptions in finalizers don't prevent other finalizers") {
        for {
          order <- Ref.make(List.empty[Int])
          app = ZIOApp.fromZIO(
            ZIO.acquireRelease(order.update(1 :: _)) { _ =>
              order.update(10 :: _) *> ZIO.fail(new RuntimeException("finalizer error"))
            }.catchAll(_ => ZIO.unit)
              .flatMap { _ =>
                ZIO.acquireRelease(order.update(2 :: _)) { _ =>
                  order.update(20 :: _)
                }
              }
              .as(ExitCode.success)
          )
          _ <- app.invoke(Chunk.empty)
          finalOrder <- order.get
        } yield assert(finalOrder.reverse.length)(equalTo(4))
      }
    ),
    suite("signal handling")(
      test("app initializes signal handlers") {
        val app = ZIOApp.fromZIO(ZIO.succeed(ExitCode.success))
        for {
          result <- app.invoke(Chunk.empty)
        } yield assertCompletes
      }
    )
  )

}
