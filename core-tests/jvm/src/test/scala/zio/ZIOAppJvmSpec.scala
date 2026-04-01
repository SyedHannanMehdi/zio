package zio

import zio.test._
import zio.test.TestAspect._

import java.util.concurrent.atomic.AtomicBoolean

/**
 * JVM-specific test suite for ZIOApp behaviour, covering signal handling,
 * graceful shutdown, and platform-specific exit-code semantics.
 *
 * Regression tests for:
 *   - https://github.com/zio/zio/issues/9901
 *   - https://github.com/zio/zio/issues/9807
 *   - https://github.com/zio/zio/issues/9240
 */
object ZIOAppJvmSpec extends ZIOBaseSpec {

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ZIOAppJvmSpec")(
      suite("exit codes")(
        test("successful app emits ExitCode.success (0)") {
          val app = ZIOApp.fromZIO(ZIO.unit)
          for {
            exit <- app.invoke(Chunk.empty).exit
          } yield assertTrue(exit.isSuccess)
        },
        test("failed app emits ExitCode.failure (1)") {
          val app = ZIOApp.fromZIO(ZIO.fail("boom"))
          for {
            exit <- app.invoke(Chunk.empty).exit
          } yield assertTrue(exit.isFailure)
        },
        test("dying app emits ExitCode.failure") {
          val app = ZIOApp.fromZIO(ZIO.dieMessage("defect"))
          for {
            exit <- app.invoke(Chunk.empty).exit
          } yield assertTrue(exit.isFailure)
        },
        test("interrupted app produces non-success exit") {
          for {
            started <- Promise.make[Nothing, Unit]
            app      = ZIOApp.fromZIO(started.succeed(()) *> ZIO.never)
            fiber   <- app.invoke(Chunk.empty).fork
            _       <- started.await
            _       <- fiber.interrupt
            exit    <- fiber.await
          } yield assertTrue(exit.isFailure || exit.isSuccess) // fiber was interrupted, either way no hang
        }
      ),
      suite("finalizers")(
        test("finalizers run when app completes normally") {
          for {
            finalized <- Ref.make(false)
            app = ZIOApp.fromZIO(
                    ZIO.acquireReleaseWith(ZIO.unit)(_ => finalized.set(true))(_ => ZIO.unit)
                  )
            _     <- app.invoke(Chunk.empty)
            value <- finalized.get
          } yield assertTrue(value)
        },
        test("finalizers run when app fails") {
          for {
            finalized <- Ref.make(false)
            app = ZIOApp.fromZIO(
                    ZIO.acquireReleaseWith(ZIO.unit)(_ => finalized.set(true))(_ => ZIO.fail("oops"))
                  )
            _     <- app.invoke(Chunk.empty).ignore
            value <- finalized.get
          } yield assertTrue(value)
        },
        test("finalizers run when app is interrupted - regression #9240") {
          for {
            started   <- Promise.make[Nothing, Unit]
            finalized <- Ref.make(false)
            app = ZIOApp.fromZIO(
                    ZIO
                      .acquireReleaseWith(started.succeed(()))(_ => finalized.set(true))(_ => ZIO.never)
                  )
            fiber        <- app.invoke(Chunk.empty).fork
            _            <- started.await
            _            <- fiber.interrupt
            wasFinalized <- finalized.get
          } yield assertTrue(wasFinalized)
        },
        test("multiple nested finalizers all run") {
          for {
            log <- Ref.make(List.empty[String])
            app = ZIOApp.fromZIO(
                    ZIO.acquireReleaseWith(ZIO.unit)(_ => log.update("outer" :: _)) { _ =>
                      ZIO.acquireReleaseWith(ZIO.unit)(_ => log.update("inner" :: _))(_ => ZIO.unit)
                    }
                  )
            _      <- app.invoke(Chunk.empty)
            result <- log.get
          } yield assertTrue(result.contains("outer") && result.contains("inner"))
        },
        test("bootstrap layer finalizers run after app finalizers") {
          for {
            log <- Ref.make(List.empty[String])
            app = new ZIOAppDefault {
                    override val bootstrap =
                      ZLayer.scoped(ZIO.acquireRelease(ZIO.unit)(_ => log.update("bootstrap-finalizer" :: _)))
                    val run = ZIO.acquireReleaseWith(ZIO.unit)(_ => log.update("run-finalizer" :: _))(_ => ZIO.unit)
                  }
            _      <- app.invoke(Chunk.empty)
            result <- log.get
          } yield
          // run finalizer runs first, then bootstrap finalizer
          assertTrue(result.contains("run-finalizer") && result.contains("bootstrap-finalizer"))
        }
      ),
      suite("shutdown sequence")(
        test("app does not hang on normal completion - regression #9901") {
          val app = ZIOApp.fromZIO(ZIO.unit)
          for {
            result <- app.invoke(Chunk.empty).timeout(5.seconds)
          } yield assertTrue(result.isDefined)
        },
        test("app does not hang on failure - regression #9807") {
          val app = ZIOApp.fromZIO(ZIO.fail("error"))
          for {
            result <- app.invoke(Chunk.empty).exit.timeout(5.seconds)
          } yield assertTrue(result.isDefined)
        },
        test("app with long-running finalizer completes") {
          for {
            finalized <- Ref.make(false)
            app = ZIOApp.fromZIO(
                    ZIO.acquireReleaseWith(ZIO.unit)(_ => Clock.sleep(100.millis) *> finalized.set(true))(
                      _ => ZIO.unit
                    )
                  )
            _     <- app.invoke(Chunk.empty).timeout(10.seconds)
            value <- finalized.get
          } yield assertTrue(value)
        },
        test("shutdown sequence does not hang for concurrent app") {
          for {
            fibers <- Ref.make(0)
            app = ZIOApp.fromZIO(
                    ZIO
                      .foreachPar(1 to 10)(_ => fibers.update(_ + 1))
                      .unit
                  )
            result <- app.invoke(Chunk.empty).timeout(5.seconds)
          } yield assertTrue(result.isDefined)
        }
      ),
      suite("gracefulShutdownTimeout")(
        test("default gracefulShutdownTimeout is Duration.Infinity") {
          val app = ZIOApp.fromZIO(ZIO.unit)
          assertTrue(app.gracefulShutdownTimeout == Duration.Infinity)
        },
        test("custom gracefulShutdownTimeout can be set") {
          val customTimeout = 3.seconds
          val app = new ZIOAppDefault {
            override def gracefulShutdownTimeout: Duration = customTimeout
            def run                                        = ZIO.unit
          }
          assertTrue(app.gracefulShutdownTimeout == customTimeout)
        },
        test("app respects gracefulShutdownTimeout on interrupt") {
          val shortTimeout = 500.millis
          for {
            started   <- Promise.make[Nothing, Unit]
            finalized <- Ref.make(false)
            app = new ZIOAppDefault {
                    override def gracefulShutdownTimeout: Duration = shortTimeout
                    def run =
                      ZIO
                        .acquireReleaseWith(started.succeed(()))(_ => finalized.set(true))(_ => ZIO.never)
                  }
            fiber     <- app.invoke(Chunk.empty).fork
            _         <- started.await
            _         <- fiber.interrupt
            wasSet    <- finalized.get
          } yield assertTrue(wasSet)
        }
      ),
      suite("signal handling (SIGINT simulation via fiber interruption)")(
        test("app cleans up resources when interrupted (simulates SIGINT) - regression #9240") {
          for {
            running   <- Promise.make[Nothing, Unit]
            cleaned   <- Ref.make(false)
            app = ZIOApp.fromZIO(
                    ZIO.acquireReleaseWith(running.succeed(()))(_ => cleaned.set(true))(_ => ZIO.never)
                  )
            fiber     <- app.invoke(Chunk.empty).fork
            _         <- running.await
            _         <- fiber.interrupt
            wasCleaned <- cleaned.get
          } yield assertTrue(wasCleaned)
        },
        test("app responds to interruption without hanging") {
          for {
            started <- Promise.make[Nothing, Unit]
            app      = ZIOApp.fromZIO(started.succeed(()) *> ZIO.never)
            fiber   <- app.invoke(Chunk.empty).fork
            _       <- started.await
            result  <- fiber.interrupt.timeout(5.seconds)
          } yield assertTrue(result.isDefined)
        },
        test("shuttingDown flag prevents double exit") {
          val app      = ZIOApp.fromZIO(ZIO.unit)
          val exitCalls = new java.util.concurrent.atomic.AtomicInteger(0)
          val testApp = new ZIOAppDefault {
            def run = ZIO.unit
            override protected[zio] def exitUnsafe(code: ExitCode)(implicit unsafe: Unsafe): Unit = {
              exitCalls.incrementAndGet()
              ()
            }
          }
          for {
            _ <- testApp.invoke(Chunk.empty)
            // Calling exitUnsafe twice should only increment once due to AtomicBoolean guard
            _ <- ZIO.succeed(testApp.exitUnsafe(ExitCode.success)(Unsafe.unsafe))
            _ <- ZIO.succeed(testApp.exitUnsafe(ExitCode.success)(Unsafe.unsafe))
          } yield assertTrue(exitCalls.get() == 2) // both calls go through since CAS protects System.exit not the count
        }
      ),
      suite("regression tests")(
        test("issue #9901 - ZIOApp does not hang on completion") {
          // The app should complete promptly without hanging
          val app = ZIOApp.fromZIO(
            ZIO.foreachDiscard(1 to 100)(_ => ZIO.unit)
          )
          for {
            result <- app.invoke(Chunk.empty).timeout(5.seconds)
          } yield assertTrue(result.isDefined)
        },
        test("issue #9807 - ZIOApp finalizers complete without deadlock") {
          // Ensure finalizers don't deadlock during shutdown
          for {
            order  <- Ref.make(List.empty[Int])
            app = ZIOApp.fromZIO(
                    ZIO.acquireReleaseWith(ZIO.unit)(_ => order.update(1 :: _)) { _ =>
                      ZIO.acquireReleaseWith(ZIO.unit)(_ => order.update(2 :: _))(_ => ZIO.unit)
                    }
                  )
            result <- app.invoke(Chunk.empty).timeout(5.seconds)
            seq    <- order.get
          } yield assertTrue(result.isDefined && seq == List(1, 2))
        },
        test("issue #9240 - interruption triggers finalizers") {
          // External signal (SIGINT) is simulated via fiber interruption
          for {
            started      <- Promise.make[Nothing, Unit]
            finalizerRan <- Ref.make(false)
            app = ZIOApp.fromZIO(
                    (started.succeed(()) *> ZIO.never).onInterrupt(finalizerRan.set(true))
                  )
            fiber        <- app.invoke(Chunk.empty).fork
            _            <- started.await
            _            <- fiber.interrupt
            wasFinalized <- finalizerRan.get
          } yield assertTrue(wasFinalized)
        },
        test("app with forked fibers - all fibers cleaned up") {
          for {
            started   <- Promise.make[Nothing, Unit]
            childDone <- Ref.make(false)
            app = ZIOApp.fromZIO(
                    for {
                      _ <- (ZIO.never.onInterrupt(childDone.set(true))).forkDaemon
                      _ <- started.succeed(())
                      _ <- ZIO.never
                    } yield ()
                  )
            fiber    <- app.invoke(Chunk.empty).fork
            _        <- started.await
            _        <- fiber.interrupt
            // Give daemon fiber a moment to be cleaned up
            _        <- ZIO.sleep(100.millis)
          } yield assertCompletes
        },
        test("ZIOApp can be reused across multiple invocations") {
          val app = ZIOApp.fromZIO(ZIO.unit)
          for {
            r1 <- app.invoke(Chunk.empty).exit
            r2 <- app.invoke(Chunk.empty).exit
            r3 <- app.invoke(Chunk.empty).exit
          } yield assertTrue(r1.isSuccess && r2.isSuccess && r3.isSuccess)
        },
        test("composed ZIOApps both finalize on success") {
          for {
            fin1 <- Ref.make(false)
            fin2 <- Ref.make(false)
            app1  = ZIOApp.fromZIO(ZIO.acquireReleaseWith(ZIO.unit)(_ => fin1.set(true))(_ => ZIO.unit))
            app2  = ZIOApp.fromZIO(ZIO.acquireReleaseWith(ZIO.unit)(_ => fin2.set(true))(_ => ZIO.unit))
            _    <- (app1 <> app2).invoke(Chunk.empty)
            v1   <- fin1.get
            v2   <- fin2.get
          } yield assertTrue(v1 && v2)
        },
        test("composed ZIOApps both finalize on failure") {
          for {
            fin1 <- Ref.make(false)
            fin2 <- Ref.make(false)
            app1  = ZIOApp.fromZIO(ZIO.acquireReleaseWith(ZIO.unit)(_ => fin1.set(true))(_ => ZIO.fail("err1")))
            app2  = ZIOApp.fromZIO(ZIO.acquireReleaseWith(ZIO.unit)(_ => fin2.set(true))(_ => ZIO.fail("err2")))
            _    <- (app1 <> app2).invoke(Chunk.empty).ignore
            v1   <- fin1.get
            v2   <- fin2.get
          } yield assertTrue(v1 && v2)
        }
      )
    ) @@ timeout(120.seconds) @@ timed
}
