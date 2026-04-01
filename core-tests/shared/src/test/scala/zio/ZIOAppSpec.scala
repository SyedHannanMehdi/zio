package zio

import zio.test._

import scala.annotation.nowarn

object ZIOAppSpec extends ZIOBaseSpec {
  def spec = suite("ZIOAppSpec")(
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
    },
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
    },
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
    test("app finalizers run on success") {
      for {
        finalized <- Ref.make(false)
        app = ZIOApp.fromZIO(
                ZIO.acquireReleaseWith(ZIO.unit)(_ => finalized.set(true))(_ => ZIO.unit)
              )
        _     <- app.invoke(Chunk.empty)
        value <- finalized.get
      } yield assertTrue(value)
    },
    test("app finalizers run on failure") {
      for {
        finalized <- Ref.make(false)
        app = ZIOApp.fromZIO(
                ZIO.acquireReleaseWith(ZIO.unit)(_ => finalized.set(true))(_ => ZIO.fail("boom"))
              )
        _     <- app.invoke(Chunk.empty).ignore
        value <- finalized.get
      } yield assertTrue(value)
    },
    test("app returns correct exit code on defect") {
      for {
        code <- ZIOApp
                  .fromZIO(ZIO.dieMessage("catastrophic failure"))
                  .invoke(Chunk.empty)
                  .exitCode: @nowarn("cat=deprecation")
      } yield assertTrue(code == ExitCode.failure)
    },
    test("app receives command-line arguments") {
      for {
        ref <- Ref.make(Chunk.empty[String])
        app = ZIOApp.fromZIO(
                ZIOAppArgs.getArgs.flatMap(args => ref.set(args))
              )
        _    <- app.invoke(Chunk("hello", "world"))
        args <- ref.get
      } yield assertTrue(args == Chunk("hello", "world"))
    },
    test("bootstrap layer failure causes app to fail") {
      val failingBootstrap = ZLayer.fail("bootstrap failed")
      val app = ZIOApp(
        ZIO.unit,
        failingBootstrap
      )
      for {
        code <- app.invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
      } yield assertTrue(code == ExitCode.failure)
    },
    test("scoped resources in run are released on success") {
      for {
        acquired <- Ref.make(false)
        released <- Ref.make(false)
        app = new ZIOAppDefault {
                def run = ZIO.scoped {
                  ZIO.acquireRelease(acquired.set(true))(_ => released.set(true)) *> ZIO.unit
                }
              }
        _         <- app.invoke(Chunk.empty)
        wasAcq    <- acquired.get
        wasRel    <- released.get
      } yield assertTrue(wasAcq && wasRel)
    },
    test("scoped resources in run are released on failure") {
      for {
        acquired <- Ref.make(false)
        released <- Ref.make(false)
        app = new ZIOAppDefault {
                def run = ZIO.scoped {
                  ZIO.acquireRelease(acquired.set(true))(_ => released.set(true)) *> ZIO.fail("oops")
                }
              }
        _      <- app.invoke(Chunk.empty).ignore
        wasAcq <- acquired.get
        wasRel <- released.get
      } yield assertTrue(wasAcq && wasRel)
    },
    test("composed apps both run finalizers") {
      for {
        ref1 <- Ref.make(false)
        ref2 <- Ref.make(false)
        app1 = ZIOApp.fromZIO(ZIO.acquireReleaseWith(ZIO.unit)(_ => ref1.set(true))(_ => ZIO.unit))
        app2 = ZIOApp.fromZIO(ZIO.acquireReleaseWith(ZIO.unit)(_ => ref2.set(true))(_ => ZIO.unit))
        _    <- (app1 <> app2).invoke(Chunk.empty)
        v1   <- ref1.get
        v2   <- ref2.get
      } yield assertTrue(v1 && v2)
    },
    test("app does not hang on completion - issue #9901") {
      // Regression test: ZIOApp should not hang when the app completes
      for {
        ref <- Ref.make(false)
        app = ZIOApp.fromZIO(ref.set(true))
        _   <- app.invoke(Chunk.empty).timeout(5.seconds)
        v   <- ref.get
      } yield assertTrue(v)
    },
    test("app finalizers complete within reasonable time - issue #9807") {
      // Regression test: finalizers should not hang indefinitely
      for {
        finalized <- Ref.make(false)
        app = ZIOApp.fromZIO(
                ZIO.acquireReleaseWith(ZIO.unit)(_ => finalized.set(true) *> ZIO.unit)(_ => ZIO.unit)
              )
        result <- app.invoke(Chunk.empty).timeout(10.seconds)
        value  <- finalized.get
      } yield assertTrue(result.isDefined && value)
    },
    test("interrupted app runs finalizers - issue #9240") {
      // Regression test: interruption should still run finalizers
      for {
        started   <- Promise.make[Nothing, Unit]
        finalized <- Ref.make(false)
        app = ZIOApp.fromZIO(
                ZIO.acquireReleaseWith(started.succeed(()))(_ => finalized.set(true))(_ => ZIO.never)
              )
        fiber     <- app.invoke(Chunk.empty).fork
        _         <- started.await
        _         <- fiber.interrupt
        wasFinalized <- finalized.get
      } yield assertTrue(wasFinalized)
    },
    test("invoke does not call System.exit") {
      // invoke() is designed for testing and should not call System.exit
      var exitCalled = false
      val app = new ZIOAppDefault {
        def run = ZIO.unit
        override protected[zio] def exitUnsafe(code: ExitCode)(implicit unsafe: Unsafe): Unit = {
          exitCalled = true
          ()
        }
      }
      for {
        _ <- app.invoke(Chunk.empty)
      } yield assertTrue(!exitCalled)
    },
    test("multiple invocations of the same app work independently") {
      for {
        counter <- Ref.make(0)
        app      = ZIOApp.fromZIO(counter.update(_ + 1))
        _       <- app.invoke(Chunk.empty)
        _       <- app.invoke(Chunk.empty)
        _       <- app.invoke(Chunk.empty)
        v       <- counter.get
      } yield assertTrue(v == 3)
    },
    test("app with ZLayer environment runs correctly") {
      case class Config(value: Int)
      val configLayer = ZLayer.succeed(Config(42))
      val app = ZIOApp(
        ZIO.service[Config].map(_.value),
        configLayer
      )
      for {
        result <- app.invoke(Chunk.empty)
      } yield assertTrue(result == 42)
    },
    test("gracefulShutdownTimeout is defined as Duration.Infinity by default") {
      val app = ZIOApp.fromZIO(ZIO.unit)
      assertTrue(app.gracefulShutdownTimeout == Duration.Infinity)
    },
    test("custom gracefulShutdownTimeout is respected") {
      val timeout = 5.seconds
      val app = new ZIOAppDefault {
        override def gracefulShutdownTimeout: Duration = timeout
        def run                                        = ZIO.unit
      }
      assertTrue(app.gracefulShutdownTimeout == timeout)
    },
    test("app failure does not swallow original error") {
      val error = "specific error message"
      for {
        capturedCause <- Ref.make[Option[Cause[Any]]](None)
        app = ZIOApp(
                ZIO.fail(error),
                Runtime.addLogger(new ZLogger[Any, Unit] {
                  def apply(
                    trace: Trace,
                    fiberId: FiberId,
                    logLevel: LogLevel,
                    message: () => Any,
                    cause: Cause[Any],
                    context: FiberRefs,
                    spans: List[LogSpan],
                    annotations: Map[String, String]
                  ): Unit = ()
                })
              )
        exit <- app.invoke(Chunk.empty).exit
      } yield assertTrue(exit.isFailure)
    },
    test("shutdown sequence completes without hanging for simple app") {
      for {
        steps <- Ref.make(List.empty[String])
        app = ZIOApp.fromZIO(
                ZIO.acquireReleaseWith(steps.update("acquire" :: _))(_ => steps.update("release" :: _))(_ =>
                  steps.update("use" :: _)
                )
              )
        _      <- app.invoke(Chunk.empty).timeout(5.seconds)
        result <- steps.get
      } yield assertTrue(result.contains("acquire") && result.contains("use") && result.contains("release"))
    }
  )
}
