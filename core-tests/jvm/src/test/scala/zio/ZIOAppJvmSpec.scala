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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * JVM-specific tests for ZIOApp behaviour, covering:
 *   - Correct exit codes on success/failure
 *   - Finalizers are run on normal completion and interruption
 *   - Shutdown sequence doesn't hang
 *   - gracefulShutdownTimeout is respected
 *   - Regression tests for past issues (#9901, #9807, #9240)
 */
object ZIOAppJvmSpec extends ZIOBaseSpec {

  def spec: Spec[TestEnvironment with Scope, Any] = suite("ZIOAppJvmSpec")(
    // -------------------------------------------------------------------------
    // Exit-code tests
    // -------------------------------------------------------------------------
    suite("exit codes")(
      test("successful app produces ExitCode.success result") {
        for {
          result <- ZIOApp.fromZIO(ZIO.succeed(42)).invoke(Chunk.empty).exit
        } yield assert(result)(succeeds(anything))
      },
      test("failed app invoke returns a failed effect") {
        for {
          result <- ZIOApp.fromZIO(ZIO.fail("boom")).invoke(Chunk.empty).exit
        } yield assert(result)(fails(anything))
      },
      test("ZIOAppDefault success runs without error") {
        for {
          ref <- Ref.make(0)
          _   <- ZIOAppDefault.fromZIO(ref.set(1)).invoke(Chunk.empty)
          v   <- ref.get
        } yield assertTrue(v == 1)
      },
      test("ZIOAppDefault failure propagates as failed effect") {
        for {
          result <- ZIOAppDefault.fromZIO(ZIO.fail("error")).invoke(Chunk.empty).exit
        } yield assert(result)(fails(anything))
      },
      test("die (defect) in run propagates as failed effect") {
        for {
          result <- ZIOApp.fromZIO(ZIO.die(new RuntimeException("defect"))).invoke(Chunk.empty).exit
        } yield assert(result)(fails(anything))
      }
    ),

    // -------------------------------------------------------------------------
    // Finalizer tests
    // -------------------------------------------------------------------------
    suite("finalizers")(
      test("finalizer runs on successful completion") {
        val finalized = new AtomicBoolean(false)
        val app       = ZIOApp.fromZIO(ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(finalized.set(true))))
        for {
          _ <- app.invoke(Chunk.empty)
          v  = finalized.get()
        } yield assertTrue(v)
      },
      test("finalizer runs on failure") {
        val finalized = new AtomicBoolean(false)
        val app = ZIOApp.fromZIO(
          ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(finalized.set(true))) *> ZIO.fail("boom")
        )
        for {
          _ <- app.invoke(Chunk.empty).ignore
          v  = finalized.get()
        } yield assertTrue(v)
      },
      test("finalizer runs on interruption (issue #9807)") {
        for {
          started   <- Promise.make[Nothing, Unit]
          finalized <- Ref.make(false)
          app = ZIOApp.fromZIO(
                  ZIO.acquireRelease(started.succeed(()) *> ZIO.never)(_ => finalized.set(true))
                )
          fiber <- app.invoke(Chunk.empty).fork
          _     <- started.await
          _     <- fiber.interrupt
          v     <- finalized.get
        } yield assertTrue(v)
      },
      test("multiple finalizers all run on completion") {
        val counter = new AtomicInteger(0)
        val app = ZIOApp.fromZIO(
          ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(counter.incrementAndGet())) *>
            ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(counter.incrementAndGet()))
        )
        for {
          _ <- app.invoke(Chunk.empty)
          v  = counter.get()
        } yield assertTrue(v == 2)
      },
      test("finalizers in bootstrap layer run after run finalizers") {
        for {
          order <- Ref.make(List.empty[String])
          app = new ZIOAppDefault {
                  override val bootstrap =
                    ZLayer.scoped(
                      ZIO.acquireRelease(ZIO.unit)(_ => order.update("bootstrap" :: _))
                    )
                  val run = ZIO.acquireRelease(ZIO.unit)(_ => order.update("run" :: _))
                }
          _      <- app.invoke(Chunk.empty)
          result <- order.get
        } yield assertTrue(result == List("bootstrap", "run"))
      },
      test("ZIO.ensuring finalizer runs on interruption") {
        for {
          running   <- Promise.make[Nothing, Unit]
          finalized <- Ref.make(false)
          app        = ZIOApp.fromZIO((running.succeed(()) *> ZIO.never).ensuring(finalized.set(true)))
          fiber     <- app.invoke(Chunk.empty).fork
          _         <- running.await
          _         <- fiber.interrupt
          v         <- finalized.get
        } yield assertTrue(v)
      }
    ),

    // -------------------------------------------------------------------------
    // Shutdown / hang tests
    // -------------------------------------------------------------------------
    suite("shutdown does not hang")(
      test("app that succeeds immediately completes") {
        for {
          _ <- ZIOApp.fromZIO(ZIO.unit).invoke(Chunk.empty)
        } yield assertCompletes
      },
      test("app that fails immediately completes") {
        for {
          _ <- ZIOApp.fromZIO(ZIO.fail("boom")).invoke(Chunk.empty).ignore
        } yield assertCompletes
      },
      test("composed apps both complete without hanging") {
        for {
          ref  <- Ref.make(0)
          app1  = ZIOApp.fromZIO(ref.update(_ + 1))
          app2  = ZIOApp.fromZIO(ref.update(_ + 2))
          _    <- (app1 <> app2).invoke(Chunk.empty)
          v    <- ref.get
        } yield assertTrue(v == 3)
      },
      test("app forked and interrupted completes without hanging") {
        for {
          started <- Promise.make[Nothing, Unit]
          app      = ZIOApp.fromZIO(started.succeed(()) *> ZIO.never)
          fiber   <- app.invoke(Chunk.empty).fork
          _       <- started.await
          _       <- fiber.interrupt
        } yield assertCompletes
      }
    ),

    // -------------------------------------------------------------------------
    // gracefulShutdownTimeout tests
    // -------------------------------------------------------------------------
    suite("gracefulShutdownTimeout")(
      test("default gracefulShutdownTimeout is Duration.Infinity") {
        val app = ZIOApp.fromZIO(ZIO.unit)
        assertTrue(app.gracefulShutdownTimeout == Duration.Infinity)
      },
      test("custom gracefulShutdownTimeout is preserved") {
        val customTimeout = 5.seconds
        val app = new ZIOAppDefault {
          override val gracefulShutdownTimeout = customTimeout
          val run                              = ZIO.unit
        }
        assertTrue(app.gracefulShutdownTimeout == customTimeout)
      },
      test("app with short timeout still runs finalizers that complete quickly") {
        val finalized = new AtomicBoolean(false)
        val app = new ZIOAppDefault {
          override val gracefulShutdownTimeout = 5.seconds
          val run = ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(finalized.set(true)))
        }
        for {
          _ <- app.invoke(Chunk.empty)
          v  = finalized.get()
        } yield assertTrue(v)
      }
    ),

    // -------------------------------------------------------------------------
    // Regression tests for past issues
    // -------------------------------------------------------------------------
    suite("regression tests")(
      // #9901 – ZIOApp should handle interruption of the main fiber correctly
      test("regression #9901 – interrupted app doesn't swallow finalizers") {
        for {
          started   <- Promise.make[Nothing, Unit]
          finalized <- Ref.make(false)
          app = ZIOApp.fromZIO(
                  ZIO.acquireRelease(started.succeed(()))(_ => finalized.set(true)) *> ZIO.never
                )
          fiber <- app.invoke(Chunk.empty).fork
          _     <- started.await
          _     <- fiber.interrupt
          v     <- finalized.get
        } yield assertTrue(v)
      },

      // #9807 – Finalizers must run when the app is interrupted externally
      test("regression #9807 – finalizers run on external interruption") {
        for {
          latch     <- Promise.make[Nothing, Unit]
          finalized <- Ref.make(false)
          app = ZIOAppDefault.fromZIO(
                  ZIO.acquireRelease(latch.succeed(()) *> ZIO.never)(_ => finalized.set(true))
                )
          fiber <- app.invoke(Chunk.empty).fork
          _     <- latch.await
          _     <- fiber.interrupt
          v     <- finalized.get
        } yield assertTrue(v)
      },

      // #9240 – ZIOApp should not deadlock when the run effect itself forks daemon fibers
      test("regression #9240 – app with daemon fibers completes without deadlock") {
        for {
          ref <- Ref.make(0)
          app = ZIOApp.fromZIO(
                  ZIO.forkDaemon(ref.update(_ + 1)).flatMap(_.join) *> ref.update(_ + 1)
                )
          _   <- app.invoke(Chunk.empty)
          v   <- ref.get
        } yield assertTrue(v == 2)
      },

      // Composed apps share bootstrap/environment correctly
      test("composed app with shared layer runs both components") {
        for {
          ref  <- Ref.make(List.empty[String])
          app1  = ZIOApp.fromZIO(ref.update("a" :: _))
          app2  = ZIOApp.fromZIO(ref.update("b" :: _))
          _    <- (app1 <> app2).invoke(Chunk.empty)
          v    <- ref.get
        } yield assertTrue(v.toSet == Set("a", "b"))
      },

      // bootstrap layer errors should propagate
      test("bootstrap layer failure propagates as failed invoke") {
        val app = ZIOApp(
          run0 = ZIO.unit,
          bootstrap0 = ZLayer.fail("bootstrap error")
        )
        for {
          result <- app.invoke(Chunk.empty).exit
        } yield assert(result)(fails(anything))
      },

      // invoke should respect ZIOAppArgs
      test("ZIOAppArgs are accessible in run") {
        val args    = Chunk("hello", "world")
        val captured = new java.util.concurrent.atomic.AtomicReference[Chunk[String]](Chunk.empty)
        val app = ZIOApp.fromZIO(
          ZIOAppArgs.getArgs.flatMap(a => ZIO.succeed(captured.set(a)))
        )
        for {
          _ <- app.invoke(args)
          v  = captured.get()
        } yield assertTrue(v == args)
      },

      // App that uses Scope and runs acquireRelease correctly finalizes
      test("scoped resource is released after app completes") {
        for {
          released <- Ref.make(false)
          app = ZIOApp.fromZIO(
                  ZIO.acquireRelease(ZIO.unit)(_ => released.set(true)) *> ZIO.unit
                )
          _   <- app.invoke(Chunk.empty)
          v   <- released.get
        } yield assertTrue(v)
      },

      // Ensure that errors inside finalizers do not prevent other finalizers from running
      test("error in one finalizer does not prevent other finalizers from running") {
        val secondFinalized = new AtomicBoolean(false)
        val app = ZIOApp.fromZIO(
          ZIO
            .acquireRelease(ZIO.unit)(_ => ZIO.die(new RuntimeException("finalizer error")))
            .flatMap(_ =>
              ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(secondFinalized.set(true)))
            )
        )
        for {
          _ <- app.invoke(Chunk.empty).exit
          v  = secondFinalized.get()
        } yield assertTrue(v)
      },

      // Layered bootstrap provides correct environment to run
      test("bootstrap layer provides services to run") {
        case class MyService(value: Int)
        val layer = ZLayer.succeed(MyService(42))
        val app = ZIOApp(
          run0 = ZIO.service[MyService].flatMap(s => ZIO.succeed(s.value)),
          bootstrap0 = ZLayer.empty >>> layer
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertCompletes
      }
    )
  )
}
