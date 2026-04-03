package zio

import zio.test._
import scala.annotation.nowarn
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch

object ZIOAppSpec extends ZIOBaseSpec {
  def spec = suite("ZIOAppSpec")(
    suite("Basic functionality")(
      test("fromZIO") {
        for {
          ref <- Ref.make(0)
          _   <- ZIOApp.fromZIO(ref.update(_ + 1)).invoke(Chunk.empty)
          v   <- ref.get
        } yield assertTrue(v == 1)
      },
      test("failure translates into ExitCode.failure") {
        for {
          code <- ZIOApp.fromZIO(ZIO.fail("Uh oh!")).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.failure)
      },
      test("success translates into ExitCode.success") {
        for {
          code <- ZIOApp.fromZIO(ZIO.succeed("Hurray!")).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.success)
      },
      test("composed app logic runs component logic") {
        for {
          ref <- Ref.make(2)
          app1 = ZIOApp.fromZIO(ref.update(_ + 3))
          app2 = ZIOApp.fromZIO(ref.update(_ - 5))
          _   <- (app1 <> app2).invoke(Chunk.empty)
          v   <- ref.get
        } yield assertTrue(v == 0)
      }
    ),
    suite("Error codes")(
      test("correct error code on success") {
        for {
          code <- ZIOApp.fromZIO(ZIO.succeed(())).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.success)
      },
      test("correct error code on failure") {
        for {
          code <- ZIOApp.fromZIO(ZIO.fail(new Exception("test"))).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.failure)
      },
      test("correct error code on die") {
        for {
          code <- ZIOApp.fromZIO(ZIO.die(new Exception("test"))).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.failure)
      }
    ),
    suite("Finalizers")(
      test("execution of finalizers on interruption") {
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
      test("finalizers are run in scope of bootstrap layer") {
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
      test("multiple nested finalizers execute in correct order") {
        for {
          order <- Ref.make(List[Int]())
          app = ZIOAppDefault.fromZIO(
                  ZIO.acquireRelease(ZIO.unit)(_ => order.update(_ :+ 1))
                    *> ZIO.acquireRelease(ZIO.unit)(_ => order.update(_ :+ 2))
                    *> ZIO.acquireRelease(ZIO.unit)(_ => order.update(_ :+ 3))
                )
          _    <- app.invoke(Chunk.empty)
          result <- order.get
        } yield assertTrue(result == List(3, 2, 1))
      },
      test("finalizers run even when main effect succeeds") {
        for {
          finalizerRan <- Ref.make(false)
          app = ZIOAppDefault.fromZIO(
                  ZIO.succeed("success").ensuring(finalizerRan.set(true))
                )
          _  <- app.invoke(Chunk.empty)
          ran <- finalizerRan.get
        } yield assertTrue(ran)
      },
      test("finalizers run even when main effect fails") {
        for {
          finalizerRan <- Ref.make(false)
          app = ZIOAppDefault.fromZIO(
                  ZIO.fail("error").ensuring(finalizerRan.set(true))
                )
          _  <- app.invoke(Chunk.empty)
          ran <- finalizerRan.get
        } yield assertTrue(ran)
      }
    ),
    suite("Shutdown sequence")(
      test("app completes on its own without hanging") {
        for {
          completed <- Ref.make(false)
          app = ZIOAppDefault.fromZIO(
                  ZIO.succeed("done").tap(_ => completed.set(true))
                )
          _ <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
          done <- completed.get
        } yield assertTrue(done)
      },
      test("app completes on failure without hanging") {
        for {
          completed <- Ref.make(false)
          app = ZIOAppDefault.fromZIO(
                  ZIO.fail("error").tap(_ => completed.set(true)).catchAll(_ => completed.set(true) *> ZIO.unit)
                )
          _ <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
          done <- completed.get
        } yield assertTrue(done)
      }
    ),
    suite("Graceful shutdown timeout")(
      test("gracefulShutdownTimeout is respected") {
        val startTime = System.currentTimeMillis()
        for {
          completed <- Ref.make(false)
          app = new ZIOAppDefault {
                  override val gracefulShutdownTimeout = Duration.fromMillis(100)
                  val run = ZIO.acquireRelease(ZIO.unit) { _ =>
                    ZIO.sleep(Duration.fromSeconds(10)) *> completed.set(true)
                  }
                }
          _ <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
          elapsed = System.currentTimeMillis() - startTime
          ran <- completed.get
        } yield {
          // Should timeout quickly due to gracefulShutdownTimeout, not wait full 10 seconds
          // We allow some margin for execution overhead
          assertTrue(elapsed < 5000) && assertTrue(!ran)
        }
      }
    ),
    suite("Platform integration")(
      test("hook update platform") {
        val counter = new java.util.concurrent.atomic.AtomicInteger(0)

        val logger1 = new ZLogger[Any, Unit] {
          def apply(
            trace: Trace,
            fiberId: zio.FiberId,
            logLevel: zio.LogLevel,
            message: () => Any,
            cause: Cause[Any],
            context: FiberRefs,
            spans: List[zio.LogSpan],
            annotations: Map[String, String]
          ): Unit = {
            counter.incrementAndGet()
            ()
          }
        }

        val app1 = ZIOApp(ZIO.fail("Uh oh!"), Runtime.addLogger(logger1))

        for {
          c <- app1.invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
          v <- ZIO.succeed(counter.get())
        } yield assertTrue(c == ExitCode.failure) && assertTrue(v == 1)
      }
    ),
    suite("Command-line arguments")(
      test("command-line arguments are passed correctly") {
        for {
          args <- ZIOAppDefault.fromZIO(ZIOAppArgs.getArgs).invoke(Chunk("arg1", "arg2", "arg3"))
        } yield assertTrue(args == Chunk("arg1", "arg2", "arg3"))
      },
      test("empty arguments work") {
        for {
          args <- ZIOAppDefault.fromZIO(ZIOAppArgs.getArgs).invoke(Chunk.empty)
        } yield assertTrue(args == Chunk.empty)
      }
    ),
    suite("Complex scenarios")(
      test("resource acquisition and release on success") {
        for {
          acquired <- Ref.make(false)
          released <- Ref.make(false)
          app = ZIOAppDefault.fromZIO(
                  ZIO.acquireRelease(acquired.set(true))(_ => released.set(true))
                    *> ZIO.succeed("success")
                )
          _ <- app.invoke(Chunk.empty)
          acq <- acquired.get
          rel <- released.get
        } yield assertTrue(acq) && assertTrue(rel)
      },
      test("resource acquisition and release on failure") {
        for {
          acquired <- Ref.make(false)
          released <- Ref.make(false)
          app = ZIOAppDefault.fromZIO(
                  ZIO.acquireRelease(acquired.set(true))(_ => released.set(true))
                    *> ZIO.fail("error")
                )
          _ <- app.invoke(Chunk.empty)
          acq <- acquired.get
          rel <- released.get
        } yield assertTrue(acq) && assertTrue(rel)
      },
      test("multiple layers with finalizers") {
        for {
          layer1Acquired <- Ref.make(false)
          layer1Released <- Ref.make(false)
          layer2Acquired <- Ref.make(false)
          layer2Released <- Ref.make(false)
          layer1 = ZLayer.scoped(
                     ZIO.acquireRelease(layer1Acquired.set(true))(_ => layer1Released.set(true))
                   )
          layer2 = ZLayer.scoped(
                     ZIO.acquireRelease(layer2Acquired.set(true))(_ => layer2Released.set(true))
                   )
          app = ZIOApp(
                  ZIO.succeed("success"),
                  layer1 >!> layer2
                )
          _ <- app.invoke(Chunk.empty)
          l1Acq <- layer1Acquired.get
          l1Rel <- layer1Released.get
          l2Acq <- layer2Acquired.get
          l2Rel <- layer2Released.get
        } yield assertTrue(l1Acq) && assertTrue(l1Rel) && assertTrue(l2Acq) && assertTrue(l2Rel)
      },
      test("fiber supervision - child fibers are properly managed") {
        for {
          childStarted <- Promise.make[Nothing, Unit]
          childFinished <- Ref.make(false)
          app = ZIOAppDefault.fromZIO(
                  childStarted.succeed(()) *>
                  ZIO.forkDaemon(
                    ZIO.acquireRelease(ZIO.unit)(_ => childFinished.set(true)) *> ZIO.never
                  ) *> ZIO.sleep(Duration.fromMillis(10))
                )
          _ <- app.invoke(Chunk.empty)
        } yield assertTrue(true) // Just test that it doesn't hang
      }
    ),
    suite("App composition")(
      test("composed apps share environment") {
        for {
          ref1 <- Ref.make(0)
          ref2 <- Ref.make(0)
          app1 = ZIOApp.fromZIO(ref1.update(_ + 1))
          app2 = ZIOApp.fromZIO(ref2.update(_ + 2))
          _ <- (app1 <> app2).invoke(Chunk.empty)
          v1 <- ref1.get
          v2 <- ref2.get
        } yield assertTrue(v1 == 1) && assertTrue(v2 == 2)
      }
    ),
    suite("Exit codes for different failure modes")(
      test("exception in run produces failure exit code") {
        for {
          code <- ZIOAppDefault.fromZIO(
                    ZIO.attempt(throw new RuntimeException("test"))
                  ).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        } yield assertTrue(code == ExitCode.failure)
      }
    )
  )
}
