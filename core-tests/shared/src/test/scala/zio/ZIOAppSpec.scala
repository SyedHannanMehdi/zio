package zio

import zio.test._
import scala.annotation.nowarn
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

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
      val counter = new AtomicInteger(0)

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
    test("application completes successfully on success") {
      for {
        result <- ZIOApp.fromZIO(ZIO.succeed("success")).invoke(Chunk.empty)
      } yield assertTrue(result == "success")
    },
    test("application completes with failure when app fails") {
      for {
        exit <- ZIOApp.fromZIO(ZIO.fail("error")).invoke(Chunk.empty).exit
      } yield assertTrue(exit.isFailure)
    },
    test("correct error code is emitted on failure") {
      for {
        code <- ZIOApp.fromZIO(ZIO.fail("test error")).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
      } yield assertTrue(code == ExitCode.failure)
    },
    test("correct error code is emitted on success") {
      for {
        code <- ZIOApp.fromZIO(ZIO.succeed("ok")).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
      } yield assertTrue(code == ExitCode.success)
    },
    test("application finalizers are run on normal completion") {
      for {
        finalizerRan <- Ref.make(false)
        effect       = ZIO.unit.ensuring(finalizerRan.set(true))
        app          = ZIOAppDefault.fromZIO(effect)
        _            <- app.invoke(Chunk.empty)
        ran          <- finalizerRan.get
      } yield assertTrue(ran)
    },
    test("application finalizers are run on failure") {
      for {
        finalizerRan <- Ref.make(false)
        effect       = ZIO.fail("error").ensuring(finalizerRan.set(true))
        app          = ZIOAppDefault.fromZIO(effect)
        _            <- app.invoke(Chunk.empty).ignore
        ran          <- finalizerRan.get
      } yield assertTrue(ran)
    },
    test("shutdown doesn't hang for simple app") {
      for {
        result <- ZIOApp.fromZIO(ZIO.succeed(42)).invoke(Chunk.empty).timeout(5.seconds)
      } yield assertTrue(result.isDefined)
    },
    test("multiple finalizers are all executed") {
      for {
        ref1   <- Ref.make(false)
        ref2   <- Ref.make(false)
        ref3   <- Ref.make(false)
        effect = ZIO.unit
          .ensuring(ref1.set(true))
          .ensuring(ref2.set(true))
          .ensuring(ref3.set(true))
        app <- ZIO.succeed(ZIOAppDefault.fromZIO(effect))
        _   <- app.invoke(Chunk.empty)
        r1  <- ref1.get
        r2  <- ref2.get
        r3  <- ref3.get
      } yield assertTrue(r1 && r2 && r3)
    },
    test("bootstrap layer is properly initialized") {
      for {
        initialized <- Ref.make(false)
        app = new ZIOAppDefault {
                override val bootstrap = ZLayer.scoped(
                  ZIO.acquireRelease(initialized.set(true))(_ => ZIO.unit)
                )
                val run = initialized.get.map(identity)
              }
        result <- app.invoke(Chunk.empty)
      } yield assertTrue(result)
    },
    test("application receives command-line arguments") {
      for {
        app = ZIOAppDefault.fromZIO(ZIOAppArgs.getArgs.map(_.length))
        result <- app.invoke(Chunk("arg1", "arg2", "arg3"))
      } yield assertTrue(result == 3)
    },
    test("scoped resources are released after app completes") {
      for {
        resourceReleased <- Ref.make(false)
        effect = ZIO.scoped(
          ZIO.acquireRelease(ZIO.unit)(_ => resourceReleased.set(true))
        )
        app <- ZIO.succeed(ZIOAppDefault.fromZIO(effect))
        _   <- app.invoke(Chunk.empty)
        released <- resourceReleased.get
      } yield assertTrue(released)
    },
    test("nested scopes are properly released") {
      for {
        outer <- Ref.make(false)
        inner <- Ref.make(false)
        effect = ZIO.scoped {
          for {
            _ <- ZIO.acquireRelease(ZIO.unit)(_ => outer.set(true))
            _ <- ZIO.scoped(
              ZIO.acquireRelease(ZIO.unit)(_ => inner.set(true))
            )
          } yield ()
        }
        app <- ZIO.succeed(ZIOAppDefault.fromZIO(effect))
        _   <- app.invoke(Chunk.empty)
        o   <- outer.get
        i   <- inner.get
      } yield assertTrue(o && i)
    },
    test("app with multiple operations runs all operations") {
      for {
        counter <- Ref.make(0)
        effect = for {
          _ <- counter.update(_ + 1)
          _ <- counter.update(_ + 1)
          _ <- counter.update(_ + 1)
        } yield ()
        app <- ZIO.succeed(ZIOAppDefault.fromZIO(effect))
        _   <- app.invoke(Chunk.empty)
        v   <- counter.get
      } yield assertTrue(v == 3)
    },
    test("error in finalizer doesn't prevent app completion") {
      for {
        exit <- ZIOApp
          .fromZIO(
            ZIO.unit.ensuring(ZIO.fail("finalizer error"))
          )
          .invoke(Chunk.empty)
          .exit
      } yield assertTrue(exit.isFailure)
    },
    test("composed apps both execute") {
      for {
        ref1 <- Ref.make(false)
        ref2 <- Ref.make(false)
        app1 = ZIOAppDefault.fromZIO(ref1.set(true))
        app2 = ZIOAppDefault.fromZIO(ref2.set(true))
        _    <- (app1 <> app2).invoke(Chunk.empty)
        v1   <- ref1.get
        v2   <- ref2.get
      } yield assertTrue(v1 && v2)
    }
  )
}
