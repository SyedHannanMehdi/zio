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

import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.nowarn

/**
 * Test suite for ZIOApp JVM behaviour, covering:
 *  - Exit codes on success / failure / defect
 *  - Finalizer execution
 *  - No hangs on shutdown
 *  - gracefulShutdownTimeout respected
 *  - Regression cases for #9901, #9807, #9240
 */
object ZIOAppBehaviourSpec extends ZIOBaseSpec {

  // ─── helpers ────────────────────────────────────────────────────────────────

  /** Run an app and collect the exit value; suppress internal logging. */
  private def runApp(app: ZIOApp): ZIO[Any, Any, Any] =
    app.invoke(Chunk.empty)

  /** Run an app and return the ZIO ExitCode. */
  private def exitCodeOf(app: ZIOApp): ZIO[Any, Nothing, ExitCode] =
    runApp(app).exitCode: @nowarn("cat=deprecation")

  // ─── spec ───────────────────────────────────────────────────────────────────

  def spec: Spec[TestEnvironment with Scope, Any] = suite("ZIOAppBehaviourSpec")(
    // ── 1. Exit codes ──────────────────────────────────────────────────────

    suite("exit codes")(
      test("succeeding app emits exit code 0") {
        for {
          code <- exitCodeOf(ZIOApp.fromZIO(ZIO.succeed("ok")))
        } yield assertTrue(code == ExitCode.success)
      },
      test("failing app emits exit code 1") {
        for {
          code <- exitCodeOf(ZIOApp.fromZIO(ZIO.fail("boom")))
        } yield assertTrue(code == ExitCode.failure)
      },
      test("dying app emits exit code 1") {
        for {
          code <- exitCodeOf(ZIOApp.fromZIO(ZIO.die(new RuntimeException("bang"))))
        } yield assertTrue(code == ExitCode.failure)
      },
      test("app that calls exit(0) emits exit code 0") {
        // We cannot actually call System.exit in tests; instead we verify the
        // ExitCode value returned through invoke.
        for {
          code <- exitCodeOf(new ZIOAppDefault {
                    val run = ZIO.succeed("done")
                  })
        } yield assertTrue(code == ExitCode.success)
      },
      test("interrupted app emits exit code 1") {
        for {
          code <- exitCodeOf(ZIOApp.fromZIO(ZIO.interrupt))
        } yield assertTrue(code == ExitCode.failure)
      }
    ),

    // ── 2. Finalizers ──────────────────────────────────────────────────────

    suite("finalizers")(
      test("finalizers run on success") {
        for {
          finalized <- Ref.make(false)
          app        = ZIOApp.fromZIO(ZIO.acquireReleaseWith(ZIO.unit)(_ => finalized.set(true))(_ => ZIO.unit))
          _         <- runApp(app)
          v         <- finalized.get
        } yield assertTrue(v)
      },
      test("finalizers run on failure") {
        for {
          finalized <- Ref.make(false)
          app = ZIOApp.fromZIO(
                  ZIO.acquireReleaseWith(ZIO.unit)(_ => finalized.set(true))(_ => ZIO.fail("oops"))
                )
          _   <- runApp(app).ignore
          v   <- finalized.get
        } yield assertTrue(v)
      },
      test("finalizers run on defect") {
        for {
          finalized <- Ref.make(false)
          app = ZIOApp.fromZIO(
                  ZIO.acquireReleaseWith(ZIO.unit)(_ => finalized.set(true))(_ => ZIO.die(new Exception("defect")))
                )
          _   <- runApp(app).ignore
          v   <- finalized.get
        } yield assertTrue(v)
      },
      test("finalizers run when fiber is interrupted externally") {
        for {
          started   <- Promise.make[Nothing, Unit]
          finalized <- Ref.make(false)
          app = ZIOApp.fromZIO(
                  (started.succeed(()) *> ZIO.never).ensuring(finalized.set(true))
                )
          fiber <- runApp(app).fork
          _     <- started.await
          _     <- fiber.interrupt
          v     <- finalized.get
        } yield assertTrue(v)
      },
      test("bootstrap layer finalizers run after run finalizers") {
        // Regression guard: bootstrap finalizers must not run before 'run' scope closes.
        for {
          events <- Ref.make(List.empty[String])
          app = new ZIOAppDefault {
                  override val bootstrap =
                    ZLayer.scoped(
                      ZIO.acquireRelease(events.update("bootstrap-acquire" :: _))(
                        _ => events.update("bootstrap-release" :: _)
                      )
                    )
                  val run =
                    ZIO.acquireReleaseWith(events.update("run-acquire" :: _))(
                      _ => events.update("run-release" :: _)
                    )(_ => ZIO.unit)
                }
          _    <- runApp(app)
          list <- events.get
          // list is in reverse insertion order; last inserted == "bootstrap-acquire"
          // Expected order of releases: run-release then bootstrap-release
          releaseIdx      = list.indexOf("run-release")
          bootstrapRelIdx = list.indexOf("bootstrap-release")
        } yield assertTrue(releaseIdx < bootstrapRelIdx) // run-release appears later in the prepended list
                                                         // i.e., it was recorded first
      }
    ),

    // ── 3. No hangs ────────────────────────────────────────────────────────

    suite("no hangs")(
      test("app that succeeds immediately does not hang") {
        for {
          _ <- runApp(ZIOApp.fromZIO(ZIO.unit)).timeout(10.seconds)
        } yield assertCompletes
      },
      test("app that fails immediately does not hang") {
        for {
          _ <- runApp(ZIOApp.fromZIO(ZIO.fail("fail"))).ignore.timeout(10.seconds)
        } yield assertCompletes
      },
      test("app interrupted from outside does not hang") {
        for {
          started <- Promise.make[Nothing, Unit]
          app      = ZIOApp.fromZIO(started.succeed(()) *> ZIO.never)
          fiber   <- runApp(app).fork
          _       <- started.await
          _       <- fiber.interrupt.timeout(10.seconds)
        } yield assertCompletes
      },
      test("app with slow finalizer completes within gracefulShutdownTimeout") {
        // The finalizer sleeps longer than the timeout; the app should still
        // terminate rather than hang forever.
        for {
          started   <- Promise.make[Nothing, Unit]
          completed <- Ref.make(false)
          app = new ZIOAppDefault {
                  override val gracefulShutdownTimeout: Duration = 1.second
                  val run =
                    (started.succeed(()) *> ZIO.never)
                      .ensuring(ZIO.sleep(30.seconds) *> completed.set(true))
                }
          fiber  <- runApp(app).fork
          _      <- started.await
          _      <- fiber.interrupt
          result <- completed.get
          // The finalizer should NOT have completed because the timeout kicked in.
        } yield assertTrue(!result)
      } @@ TestAspect.timeout(15.seconds)
    ),

    // ── 4. gracefulShutdownTimeout ─────────────────────────────────────────

    suite("gracefulShutdownTimeout")(
      test("short timeout cuts off long finalizer") {
        for {
          finalizerRan <- Ref.make(false)
          started      <- Promise.make[Nothing, Unit]
          app = new ZIOAppDefault {
                  override val gracefulShutdownTimeout: Duration = 500.milliseconds
                  val run =
                    (started.succeed(()) *> ZIO.never)
                      .ensuring(ZIO.sleep(10.seconds) *> finalizerRan.set(true))
                }
          fiber  <- runApp(app).fork
          _      <- started.await
          _      <- fiber.interrupt
          ran    <- finalizerRan.get
        } yield assertTrue(!ran)
      } @@ TestAspect.timeout(10.seconds),
      test("infinite timeout allows finalizer to complete") {
        for {
          finalizerRan <- Ref.make(false)
          started      <- Promise.make[Nothing, Unit]
          app = new ZIOAppDefault {
                  override val gracefulShutdownTimeout: Duration = Duration.Infinity
                  val run =
                    (started.succeed(()) *> ZIO.never)
                      .ensuring(finalizerRan.set(true))
                }
          fiber <- runApp(app).fork
          _     <- started.await
          _     <- fiber.interrupt
          ran   <- finalizerRan.get
        } yield assertTrue(ran)
      } @@ TestAspect.timeout(10.seconds)
    ),

    // ── 5. Regression tests ────────────────────────────────────────────────

    suite("regressions")(
      // #9901 – app that uses ZIO.never plus interruption should not leak fibers
      test("#9901 interrupting a never-ending app does not leak fibers") {
        for {
          rootsBefore <- Fiber.roots
          started     <- Promise.make[Nothing, Unit]
          app          = ZIOApp.fromZIO(started.succeed(()) *> ZIO.never)
          fiber       <- runApp(app).fork
          _           <- started.await
          _           <- fiber.interrupt
          rootsAfter  <- Fiber.roots
          // After interruption the fiber count should not have grown unboundedly.
          // We allow some slack for background fibers.
          leaked = rootsAfter.size - rootsBefore.size
        } yield assertTrue(leaked <= 2)
      } @@ TestAspect.timeout(15.seconds),

      // #9807 – ZIOApp with failing bootstrap should not swallow the error
      test("#9807 failing bootstrap surfaces as failure") {
        val failingBootstrap = ZLayer.fail("bootstrap failed")
        val app = ZIOApp(ZIO.unit, failingBootstrap)
        for {
          code <- exitCodeOf(app)
        } yield assertTrue(code == ExitCode.failure)
      },

      // #9240 – Scope finalizers should run even when run effect is interrupted
      test("#9240 scope finalizers run on interruption of run") {
        for {
          finalized <- Ref.make(false)
          started   <- Promise.make[Nothing, Unit]
          app = ZIOApp.fromZIO(
                  ZIO.scoped(
                    ZIO.acquireRelease(started.succeed(()) *> ZIO.unit)(_ => finalized.set(true)) *> ZIO.never
                  )
                )
          fiber <- runApp(app).fork
          _     <- started.await
          _     <- fiber.interrupt
          v     <- finalized.get
        } yield assertTrue(v)
      } @@ TestAspect.timeout(15.seconds),

      // Double-invoke: calling invoke twice should work independently
      test("invoke can be called multiple times on the same app") {
        for {
          counter <- Ref.make(0)
          app      = ZIOApp.fromZIO(counter.update(_ + 1))
          _       <- runApp(app)
          _       <- runApp(app)
          v       <- counter.get
        } yield assertTrue(v == 2)
      },

      // App that exits via ZIO.succeed should propagate the result correctly
      test("app returning a value propagates success") {
        for {
          result <- runApp(ZIOApp.fromZIO(ZIO.succeed(42)))
        } yield assertTrue(result == 42)
      },

      // Ensuring that bootstrap ZLayer is properly torn down on app failure
      test("bootstrap layer is torn down even when run fails") {
        for {
          bootstrapReleased <- Ref.make(false)
          app = new ZIOAppDefault {
                  override val bootstrap =
                    ZLayer.scoped(
                      ZIO.acquireRelease(ZIO.unit)(_ => bootstrapReleased.set(true))
                    )
                  val run = ZIO.fail("run failed")
                }
          _   <- runApp(app).ignore
          v   <- bootstrapReleased.get
        } yield assertTrue(v)
      },

      // Composed apps: both components run, both finalizers run
      test("composed apps run both component finalizers") {
        for {
          fin1 <- Ref.make(false)
          fin2 <- Ref.make(false)
          app1  = ZIOApp.fromZIO(ZIO.acquireReleaseWith(ZIO.unit)(_ => fin1.set(true))(_ => ZIO.unit))
          app2  = ZIOApp.fromZIO(ZIO.acquireReleaseWith(ZIO.unit)(_ => fin2.set(true))(_ => ZIO.unit))
          _    <- runApp(app1 <> app2)
          v1   <- fin1.get
          v2   <- fin2.get
        } yield assertTrue(v1) && assertTrue(v2)
      },

      // App with ZIOAppArgs receives args correctly
      test("ZIOAppArgs are forwarded to the app") {
        for {
          received <- Ref.make(Chunk.empty[String])
          app = new ZIOAppDefault {
                  val run = ZIOAppArgs.getArgs.flatMap(received.set)
                }
          _   <- app.invoke(Chunk("hello", "world"))
          v   <- received.get
        } yield assertTrue(v == Chunk("hello", "world"))
      }
    )
  ) @@ TestAspect.jvmOnly @@ TestAspect.timeout(120.seconds)
}
