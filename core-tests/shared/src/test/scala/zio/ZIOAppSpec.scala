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

    // ------------------------------------------------------------------
    // Additional tests added for issue #9909
    // ------------------------------------------------------------------

    test("die (defect) propagates as a failed effect from invoke") {
      for {
        result <- ZIOApp.fromZIO(ZIO.die(new RuntimeException("boom"))).invoke(Chunk.empty).exit
      } yield assert(result)(Assertion.fails(Assertion.anything))
    },
    test("ZIOAppArgs are forwarded to run") {
      val args = Chunk("arg1", "arg2")
      for {
        captured <- Ref.make(Chunk.empty[String])
        app = ZIOApp.fromZIO(ZIOAppArgs.getArgs.flatMap(a => captured.set(a)))
        _   <- app.invoke(args)
        v   <- captured.get
      } yield assertTrue(v == args)
    },
    test("scoped resource is released after successful app run") {
      for {
        released <- Ref.make(false)
        app = ZIOApp.fromZIO(
                ZIO.acquireRelease(ZIO.unit)(_ => released.set(true)) *> ZIO.unit
              )
        _   <- app.invoke(Chunk.empty)
        v   <- released.get
      } yield assertTrue(v)
    },
    test("scoped resource is released after failed app run") {
      for {
        released <- Ref.make(false)
        app = ZIOApp.fromZIO(
                ZIO.acquireRelease(ZIO.unit)(_ => released.set(true)) *> ZIO.fail("oops")
              )
        _   <- app.invoke(Chunk.empty).ignore
        v   <- released.get
      } yield assertTrue(v)
    },
    test("default gracefulShutdownTimeout is Duration.Infinity") {
      val app = ZIOApp.fromZIO(ZIO.unit)
      assertTrue(app.gracefulShutdownTimeout == Duration.Infinity)
    },
    test("custom gracefulShutdownTimeout is preserved on ZIOAppDefault") {
      val timeout = 10.seconds
      val app = new ZIOAppDefault {
        override val gracefulShutdownTimeout = timeout
        val run                              = ZIO.unit
      }
      assertTrue(app.gracefulShutdownTimeout == timeout)
    },
    test("composed apps run in parallel and both effects are observed") {
      for {
        ref  <- Ref.make(Set.empty[String])
        app1  = ZIOApp.fromZIO(ref.update(_ + "a"))
        app2  = ZIOApp.fromZIO(ref.update(_ + "b"))
        _    <- (app1 <> app2).invoke(Chunk.empty)
        v    <- ref.get
      } yield assertTrue(v == Set("a", "b"))
    },
    test("bootstrap layer failure is reported as failed invoke") {
      val app = ZIOApp(
        run0 = ZIO.unit,
        bootstrap0 = ZLayer.fail("bootstrap failed")
      )
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(Assertion.fails(Assertion.anything))
    },
    test("finalizer in ZIOAppDefault run is within scope of bootstrap") {
      for {
        log <- Ref.make(List.empty[String])
        app = new ZIOAppDefault {
                override val bootstrap =
                  ZLayer.scoped(
                    ZIO.acquireRelease(log.update("bootstrap-acquire" :: _))(_ =>
                      log.update("bootstrap-release" :: _)
                    )
                  )
                val run =
                  ZIO.acquireRelease(log.update("run-acquire" :: _))(_ => log.update("run-release" :: _))
              }
        _      <- app.invoke(Chunk.empty)
        events <- log.get
        // run-release must come before bootstrap-release
        runReleaseIdx       = events.indexOf("run-release")
        bootstrapReleaseIdx = events.indexOf("bootstrap-release")
      } yield assertTrue(runReleaseIdx < bootstrapReleaseIdx)
    },
    test("multiple sequential invocations of the same app are independent") {
      for {
        counter <- Ref.make(0)
        app      = ZIOApp.fromZIO(counter.update(_ + 1))
        _       <- app.invoke(Chunk.empty)
        _       <- app.invoke(Chunk.empty)
        v       <- counter.get
      } yield assertTrue(v == 2)
    },
    test("invoke result is failed when run uses ZIO.fail with a typed error") {
      for {
        result <- ZIOApp.fromZIO(ZIO.fail(42)).invoke(Chunk.empty).exit
      } yield assert(result)(Assertion.fails(Assertion.equalTo(42)))
    }
  )
}
