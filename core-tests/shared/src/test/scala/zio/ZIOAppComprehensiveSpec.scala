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
import scala.annotation.nowarn
import scala.concurrent.duration._

object ZIOAppComprehensiveSpec extends ZIOBaseSpec {
  def spec = suite("ZIOAppComprehensiveSpec")(
    suite("App completion behavior")(
      test("App completes successfully with correct exit code") {
        for {
          code <- ZIOApp.fromZIO(ZIO.succeed("success")).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.success)
      },
      test("App fails with correct exit code") {
        for {
          code <- ZIOApp.fromZIO(ZIO.fail("error")).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.failure)
      },
      test("App with explicit exit code returns that code") {
        for {
          code <- ZIOApp.fromZIO(ZIO.succeed(ExitCode(42))).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode(42))
      },
      test("Successful completion returns exit code zero") {
        for {
          code <- ZIOApp.fromZIO(ZIO.succeed("Done")).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.success)
      },
      test("Failure completion returns non-zero exit code") {
        for {
          code <- ZIOApp.fromZIO(ZIO.fail(new Exception("Test error"))).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.failure)
      }
    ),
    suite("Finalizer behavior")(
      test("Finalizers run on successful completion") {
        for {
          ref <- Ref.make(false)
          app = ZIOApp.fromZIO(ZIO.succeed("ok").ensuring(ref.set(true)))
          _   <- app.invoke(Chunk.empty)
          ran <- ref.get
        } yield assertTrue(ran)
      },
      test("Finalizers run on failure") {
        for {
          ref <- Ref.make(false)
          app = ZIOApp.fromZIO(ZIO.fail("error").ensuring(ref.set(true)))
          _   <- app.invoke(Chunk.empty)
          ran <- ref.get
        } yield assertTrue(ran)
      },
      test("Multiple finalizers all execute") {
        for {
          ref1 <- Ref.make(false)
          ref2 <- Ref.make(false)
          ref3 <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.succeed("ok").ensuring(ref1.set(true)).ensuring(ref2.set(true)).ensuring(ref3.set(true))
          )
          _   <- app.invoke(Chunk.empty)
          r1  <- ref1.get
          r2  <- ref2.get
          r3  <- ref3.get
        } yield assertTrue(r1 && r2 && r3)
      },
      test("Finalizers run even on interrupt") {
        for {
          running   <- Promise.make[Nothing, Unit]
          ref       <- Ref.make(false)
          effect     = (running.succeed(()) *> ZIO.never).ensuring(ref.set(true))
          app        = ZIOAppDefault.fromZIO(effect)
          fiber     <- app.invoke(Chunk.empty).fork
          _         <- running.await
          _         <- fiber.interrupt
          finalized <- ref.get
        } yield assertTrue(finalized)
      },
      test("Finalizers have access to released resources") {
        for {
          ref1 <- Ref.make(false)
          ref2 <- Ref.make(false)
          app = new ZIOAppDefault {
                  override val bootstrap = ZLayer.scoped(ZIO.acquireRelease(ref1.set(true))(_ => ref1.set(false)))
                  val run                = ZIO.acquireRelease(ZIO.unit)(_ => ref1.get.flatMap(ref2.set))
                }
          _     <- app.invoke(Chunk.empty)
          value <- ref2.get
        } yield assertTrue(value)
      },
      test("Nested resource acquisitions cleanup in correct order") {
        for {
          order <- Ref.make(List.empty[Int])
          app = ZIOApp.fromZIO(
            ZIO.acquireRelease(order.update(_ :+ 1))(_ => order.update(_ :+ 4)).flatMap { _ =>
              ZIO.acquireRelease(order.update(_ :+ 2))(_ => order.update(_ :+ 3))
            }
          )
          _     <- app.invoke(Chunk.empty)
          trace <- order.get
        } yield assertTrue(trace == List(1, 2, 3, 4))
      }
    ),
    suite("Shutdown and interrupt behavior")(
      test("App shutdown does not hang on success") {
        for {
          startTime <- Clock.instant
          _         <- ZIOApp.fromZIO(ZIO.succeed("instant")).invoke(Chunk.empty).timeout(5.seconds)
          endTime   <- Clock.instant
          duration   = endTime.toEpochMilli - startTime.toEpochMilli
        } yield assertTrue(duration < 5000)
      },
      test("App shutdown does not hang on failure") {
        for {
          startTime <- Clock.instant
          _         <- ZIOApp.fromZIO(ZIO.fail("error")).invoke(Chunk.empty).timeout(5.seconds)
          endTime   <- Clock.instant
          duration   = endTime.toEpochMilli - startTime.toEpochMilli
        } yield assertTrue(duration < 5000)
      },
      test("Interrupted app terminates") {
        for {
          promise <- Promise.make[Nothing, Unit]
          app      = ZIOApp.fromZIO(promise.await *> ZIO.succeed("done"))
          fiber   <- app.invoke(Chunk.empty).fork
          _       <- fiber.interrupt
          result  <- fiber.await
        } yield assertTrue(result.isInterrupted)
      },
      test("Root fibers are interrupted on shutdown") {
        for {
          childStarted <- Promise.make[Nothing, Unit]
          childExited  <- Promise.make[Nothing, Unit]
          ref          <- Ref.make(false)
          app = ZIOApp.fromZIO(
            ZIO.scoped {
              ZIO.forkDaemon(
                (childStarted.succeed(()) *> ZIO.never).ensuring(ref.set(true)).ensuring(childExited.succeed(()))
              ) *> childStarted.await
            }
          )
          _       <- app.invoke(Chunk.empty).fork
          timeout <- childExited.await.timeout(5.seconds)
        } yield assertTrue(timeout.isDefined)
      }
    ),
    suite("Graceful shutdown timeout")(
      test("gracefulShutdownTimeout is respected on success") {
        val timeout = 1.second
        for {
          startTime <- Clock.instant
          app = new ZIOAppDefault {
                  override def gracefulShutdownTimeout: Duration = timeout
                  val run = ZIO.succeed("done")
                }
          _       <- app.invoke(Chunk.empty)
          endTime <- Clock.instant
          duration = endTime.toEpochMilli - startTime.toEpochMilli
        } yield assertTrue(duration < 5000)
      },
      test("gracefulShutdownTimeout is respected on failure") {
        val timeout = 1.second
        for {
          startTime <- Clock.instant
          app = new ZIOAppDefault {
                  override def gracefulShutdownTimeout: Duration = timeout
                  val run = ZIO.fail("error")
                }
          _       <- app.invoke(Chunk.empty)
          endTime <- Clock.instant
          duration = endTime.toEpochMilli - startTime.toEpochMilli
        } yield assertTrue(duration < 5000)
      },
      test("App waits for finalizers up to gracefulShutdownTimeout") {
        for {
          ref <- Ref.make(false)
          finalizedIn <- Ref.make(0L)
          app = new ZIOAppDefault {
                  override def gracefulShutdownTimeout: Duration = 2.seconds
                  val run = ZIO.succeed("ok").ensuring {
                    Clock.instant.flatMap { instant =>
                      finalizedIn.set(instant.toEpochMilli) *> ref.set(true)
                    }
                  }
                }
          startTime <- Clock.instant
          _         <- app.invoke(Chunk.empty)
          finalized <- ref.get
        } yield assertTrue(finalized)
      }
    ),
    suite("Environment and bootstrap")(
      test("Bootstrap layer is properly initialized") {
        for {
          ref <- Ref.make(0)
          app = ZIOApp(
            ZIO.succeed(42),
            ZLayer.succeed(ZIOAppArgs(Chunk.empty)) >>> ZLayer.fromZIO(ref.update(_ + 1).as(42))
          )(EnvironmentTag[Int])
          _   <- app.invoke(Chunk.empty)
          val <- ref.get
        } yield assertTrue(val == 1)
      },
      test("Bootstrap layer cleanup runs after app") {
        for {
          ref1 <- Ref.make(false)
          ref2 <- Ref.make(false)
          app = new ZIOAppDefault {
                  override val bootstrap = ZLayer.scoped(ZIO.acquireRelease(ref1.set(true))(_ => ref1.set(false)))
                  val run                = ZIO.acquireRelease(ZIO.unit)(_ => ref1.get.flatMap(ref2.set))
                }
          _     <- app.invoke(Chunk.empty)
          value <- ref2.get
        } yield assertTrue(value)
      },
      test("Multiple bootstrap layers compose correctly") {
        for {
          ref1 <- Ref.make(0)
          ref2 <- Ref.make(0)
          app = ZIOApp(
            ZIO.succeed("done"),
            ZLayer.succeed(ZIOAppArgs(Chunk.empty)) >>>
              (ZLayer.fromZIO(ref1.update(_ + 1).as(1)) +!+ ZLayer.fromZIO(ref2.update(_ + 2).as(2)))
          )(EnvironmentTag[Int with Int])
          _   <- app.invoke(Chunk.empty)
          v1  <- ref1.get
          v2  <- ref2.get
        } yield assertTrue(v1 == 1 && v2 == 1)
      }
    ),
    suite("Error handling")(
      test("Error is logged on failure") {
        for {
          logged <- Ref.make(false)
          logger = new ZLogger[Any, Unit] {
            def apply(
              trace: Trace,
              fiberId: FiberId,
              logLevel: LogLevel,
              message: () => Any,
              cause: Cause[Any],
              context: FiberRefs,
              spans: List[LogSpan],
              annotations: Map[String, String]
            ): Unit = {
              if (logLevel == LogLevel.Error) logged.set(true).unsafeRunSync()(Unsafe.unsafe)
            }
          }
          app = ZIOApp(ZIO.fail("Uh oh!"), Runtime.addLogger(logger))
          _   <- app.invoke(Chunk.empty)
          was <- logged.get
        } yield assertTrue(was)
      },
      test("Cause information is preserved in failure") {
        for {
          ref <- Ref.make(Cause.empty)
          app = ZIOApp.fromZIO(
            ZIO.fail(new Exception("Test error")).tapErrorCause(c => Ref.update(ref, _ <> c))
          )
          _ <- app.invoke(Chunk.empty).catchAll(_ => ZIO.unit)
        } yield assertTrue(ref.unsafeRunSync()(Unsafe.unsafe).failureOption.isDefined)
      }
    ),
    suite("App composition")(
      test("Composed apps both execute") {
        for {
          ref1 <- Ref.make(0)
          ref2 <- Ref.make(0)
          app1  = ZIOApp.fromZIO(ref1.set(1))
          app2  = ZIOApp.fromZIO(ref2.set(2))
          _    <- (app1 <> app2).invoke(Chunk.empty)
          v1   <- ref1.get
          v2   <- ref2.get
        } yield assertTrue(v1 == 1 && v2 == 2)
      },
      test("Composed app fails if either component fails") {
        for {
          ref1 <- Ref.make(0)
          app1  = ZIOApp.fromZIO(ref1.set(1))
          app2  = ZIOApp.fromZIO(ZIO.fail("error"))
          code <- (app1 <> app2).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.failure)
      },
      test("Composed apps can share layer") {
        for {
          ref <- Ref.make(0)
          sharedLayer = ZLayer.fromZIO(ref.update(_ + 1))
          app1 = ZIOApp(ZIO.unit, sharedLayer)(EnvironmentTag[Unit])
          app2 = ZIOApp(ZIO.unit, sharedLayer)(EnvironmentTag[Unit])
          _   <- (app1 <> app2).invoke(Chunk.empty)
          val <- ref.get
        } yield assertTrue(val > 0)
      }
    ),
    suite("Command-line arguments")(
      test("Arguments are passed to app") {
        for {
          ref  <- Ref.make(Chunk.empty[String])
          app  = ZIOApp.fromZIO(ZIOAppArgs.getArgs.flatMap(ref.set))
          _    <- app.invoke(Chunk("arg1", "arg2", "arg3"))
          args <- ref.get
        } yield assertTrue(args == Chunk("arg1", "arg2", "arg3"))
      },
      test("Empty arguments work") {
        for {
          ref  <- Ref.make(Chunk.empty[String])
          app  = ZIOApp.fromZIO(ZIOAppArgs.getArgs.flatMap(ref.set))
          _    <- app.invoke(Chunk.empty)
          args <- ref.get
        } yield assertTrue(args.isEmpty)
      }
    )
  )
}
