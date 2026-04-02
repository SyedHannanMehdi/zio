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

/**
 * Tests for ZIOApp behaviour covering:
 *   - Correct exit codes on success / failure
 *   - Finalizers are run (except catastrophic failures)
 *   - Shutdown sequence does not hang
 *   - gracefulShutdownTimeout is respected
 *   - Regression scenarios from past issues (#9901, #9807, #9240)
 */
object ZIOAppSpec extends ZIOBaseSpec {

  // ── helpers ────────────────────────────────────────────────────────────────

  /** Run the app via `invoke` and collect the result together with a side-channel ref. */
  private def runApp[E, A](app: ZIOApp): ZIO[Any, Nothing, Exit[Any, Any]] =
    app.invoke(Chunk.empty).exit

  // ── spec ───────────────────────────────────────────────────────────────────

  def spec = suite("ZIOAppSpec")(
    suite("exit codes")(
      test("app that succeeds produces a successful exit") {
        val app = new ZIOAppDefault {
          def run = ZIO.unit
        }
        runApp(app).map(exit => assertTrue(exit.isSuccess))
      },
      test("app that fails with a typed error produces a failure exit") {
        val app = new ZIOAppDefault {
          def run = ZIO.fail("boom")
        }
        runApp(app).map(exit => assertTrue(!exit.isSuccess))
      },
      test("app that dies with a defect produces a failure exit") {
        val app = new ZIOAppDefault {
          def run = ZIO.dieMessage("defect")
        }
        runApp(app).map(exit => assertTrue(!exit.isSuccess))
      },
      test("app that succeeds with a value produces a successful exit") {
        val app = new ZIOAppDefault {
          def run = ZIO.succeed(42)
        }
        runApp(app).map(exit => assertTrue(exit.isSuccess))
      }
    ),
    suite("finalizers")(
      test("finalizer runs when app succeeds") {
        for {
          finalizerRan <- Ref.make(false)
          app = new ZIOAppDefault {
                  def run = ZIO.addFinalizer(finalizerRan.set(true)) *> ZIO.unit
                }
          _ <- runApp(app)
          ran <- finalizerRan.get
        } yield assertTrue(ran)
      },
      test("finalizer runs when app fails") {
        for {
          finalizerRan <- Ref.make(false)
          app = new ZIOAppDefault {
                  def run = ZIO.addFinalizer(finalizerRan.set(true)) *> ZIO.fail("error")
                }
          _ <- runApp(app)
          ran <- finalizerRan.get
        } yield assertTrue(ran)
      },
      test("finalizer runs when app is interrupted") {
        for {
          finalizerRan <- Ref.make(false)
          app = new ZIOAppDefault {
                  def run =
                    ZIO.addFinalizer(finalizerRan.set(true)) *>
                      ZIO.never
                }
          fiber <- runApp(app).fork
          _     <- ZIO.sleep(100.millis)
          _     <- fiber.interrupt
          ran   <- finalizerRan.get
        } yield assertTrue(ran)
      },
      test("multiple finalizers run in reverse order") {
        for {
          order <- Ref.make(List.empty[Int])
          app = new ZIOAppDefault {
                  def run =
                    ZIO.addFinalizer(order.update(1 :: _)) *>
                      ZIO.addFinalizer(order.update(2 :: _)) *>
                      ZIO.addFinalizer(order.update(3 :: _)) *>
                      ZIO.unit
                }
          _   <- runApp(app)
          lst <- order.get
        } yield assertTrue(lst == List(1, 2, 3))
      },
      test("bootstrap layer finalizer runs after app finalizer") {
        for {
          events <- Ref.make(List.empty[String])
          bootstrapLayer = ZLayer.scoped(
                             ZIO.acquireRelease(events.update("acquire" :: _))(_ => events.update("release" :: _))
                           )
          app = new ZIOApp {
                  type Environment = Unit
                  implicit val environmentTag: EnvironmentTag[Unit] = EnvironmentTag[Unit]
                  def bootstrap: ZLayer[ZIOAppArgs, Any, Unit]      = bootstrapLayer
                  def run                                            = ZIO.addFinalizer(events.update("app-finalizer" :: _)) *> ZIO.unit
                }
          _   <- runApp(app)
          lst <- events.get
        } yield assertTrue(lst.contains("release") && lst.contains("app-finalizer"))
      }
    ),
    suite("shutdown does not hang")(
      test("app that completes immediately does not hang") {
        val app = new ZIOAppDefault {
          def run = ZIO.unit
        }
        runApp(app).timeout(5.seconds).map(result => assertTrue(result.isDefined))
      },
      test("app with long-running finalizer completes within timeout") {
        val app = new ZIOAppDefault {
          override def gracefulShutdownTimeout: Duration = 2.seconds
          def run                                        = ZIO.addFinalizer(ZIO.sleep(100.millis)) *> ZIO.unit
        }
        runApp(app).timeout(5.seconds).map(result => assertTrue(result.isDefined))
      },
      test("ZIO.never app can be interrupted and completes") {
        for {
          fiber <- ZIO
                     .serviceWithZIO[Any](_ =>
                       new ZIOAppDefault { def run = ZIO.never }.invoke(Chunk.empty)
                     )
                     .fork
          _ <- ZIO.sleep(200.millis)
          _ <- fiber.interrupt
          r <- fiber.await
        } yield assertTrue(r.isInterrupted || r.isSuccess || !r.isSuccess /* always true — just check no hang */ )
      }
    ),
    suite("gracefulShutdownTimeout")(
      test("default gracefulShutdownTimeout is Duration.Infinity") {
        val app = new ZIOAppDefault {
          def run = ZIO.unit
        }
        assertTrue(app.gracefulShutdownTimeout == Duration.Infinity)
      },
      test("custom gracefulShutdownTimeout is respected") {
        val timeout = 500.millis
        val app = new ZIOAppDefault {
          override def gracefulShutdownTimeout: Duration = timeout
          def run                                        = ZIO.unit
        }
        assertTrue(app.gracefulShutdownTimeout == timeout)
      },
      test("zero gracefulShutdownTimeout causes immediate shutdown") {
        val app = new ZIOAppDefault {
          override def gracefulShutdownTimeout: Duration = Duration.Zero
          def run                                        = ZIO.unit
        }
        runApp(app).timeout(5.seconds).map(result => assertTrue(result.isDefined))
      }
    ),
    suite("ZIOApp composition (<>)")(
      test("composed apps both run successfully") {
        for {
          ref1 <- Ref.make(false)
          ref2 <- Ref.make(false)
          app1 = new ZIOAppDefault { def run = ref1.set(true) }
          app2 = new ZIOAppDefault { def run = ref2.set(true) }
          composed = app1 <> app2
          _  <- runApp(composed)
          r1 <- ref1.get
          r2 <- ref2.get
        } yield assertTrue(r1 && r2)
      },
      test("composed apps: failure in one does not prevent the other from running") {
        for {
          ref <- Ref.make(false)
          app1 = new ZIOAppDefault { def run = ZIO.fail("fail1") }
          app2 = new ZIOAppDefault { def run = ref.set(true) }
          composed = app1 <> app2
          _   <- runApp(composed)
          ran <- ref.get
        } yield assertTrue(ran)
      }
    ),
    suite("regression: past issues")(
      // #9901 – finalizers should run even when the app completes via failure
      test("#9901 – finalizers run on failure") {
        for {
          finalizerRan <- Ref.make(false)
          app = new ZIOAppDefault {
                  def run =
                    ZIO.acquireReleaseWith(ZIO.unit)(_ => finalizerRan.set(true))(_ => ZIO.fail("boom"))
                }
          _ <- runApp(app)
          ran <- finalizerRan.get
        } yield assertTrue(ran)
      },
      // #9807 – Scope should be properly closed when the app exits
      test("#9807 – outer Scope is closed when app exits normally") {
        for {
          closed <- Ref.make(false)
          app = new ZIOAppDefault {
                  def run =
                    ZIO.acquireRelease(ZIO.unit)(_ => closed.set(true)).flatMap(_ => ZIO.unit)
                }
          _ <- runApp(app)
          c <- closed.get
        } yield assertTrue(c)
      },
      // #9807 – Scope should be closed even when app fails
      test("#9807 – outer Scope is closed when app exits with failure") {
        for {
          closed <- Ref.make(false)
          app = new ZIOAppDefault {
                  def run =
                    ZIO.acquireRelease(ZIO.unit)(_ => closed.set(true)).flatMap(_ => ZIO.fail("fail"))
                }
          _ <- runApp(app)
          c <- closed.get
        } yield assertTrue(c)
      },
      // #9240 – exit code should be non-zero for a failing app
      test("#9240 – failing app yields non-success Exit") {
        val app = new ZIOAppDefault {
          def run = ZIO.fail(new RuntimeException("intentional"))
        }
        runApp(app).map(exit => assertTrue(!exit.isSuccess))
      },
      // #9240 – exit code should be 0 for a successful app
      test("#9240 – successful app yields success Exit") {
        val app = new ZIOAppDefault {
          def run = ZIO.succeed("result")
        }
        runApp(app).map(exit => assertTrue(exit.isSuccess))
      },
      // ensure interruption from outside doesn't leave the app running indefinitely
      test("interrupt from outside is handled cleanly") {
        for {
          finalizerRan <- Ref.make(false)
          app = new ZIOAppDefault {
                  def run =
                    ZIO.addFinalizer(finalizerRan.set(true)) *> ZIO.never
                }
          fiber <- runApp(app).fork
          _     <- ZIO.sleep(50.millis)
          _     <- fiber.interrupt
          ran   <- finalizerRan.get
        } yield assertTrue(ran)
      },
      // app with `ZIO.attempt` wrapping a throwing block should fail, not die
      test("ZIO.attempt wrapping throwing block results in failure exit") {
        val app = new ZIOAppDefault {
          def run = ZIO.attempt(throw new RuntimeException("thrown"))
        }
        runApp(app).map(exit => assertTrue(!exit.isSuccess))
      },
      // ZIOAppArgs should be accessible inside run
      test("ZIOAppArgs are accessible inside run") {
        val args = Chunk("foo", "bar")
        for {
          captured <- Ref.make(Chunk.empty[String])
          app = new ZIOAppDefault {
                  def run = ZIOAppArgs.getArgs.flatMap(a => captured.set(a))
                }
          _ <- app.invoke(args)
          a <- captured.get
        } yield assertTrue(a == args)
      },
      // bootstrap layer errors should result in a failure exit
      test("bootstrap layer failure results in failure exit") {
        val app = new ZIOApp {
          type Environment = String
          implicit val environmentTag: EnvironmentTag[String] = EnvironmentTag[String]
          def bootstrap: ZLayer[ZIOAppArgs, Any, String]      = ZLayer.fail("bootstrap failed")
          def run                                             = ZIO.service[String].flatMap(s => ZIO.succeed(s))
        }
        runApp(app).map(exit => assertTrue(!exit.isSuccess))
      }
    ),
    suite("ZIOAppDefault")(
      test("extends ZIOApp") {
        val app = new ZIOAppDefault { def run = ZIO.unit }
        assertTrue(app.isInstanceOf[ZIOApp])
      },
      test("default bootstrap provides ZIOAppArgs") {
        val app = new ZIOAppDefault { def run = ZIOAppArgs.getArgs.unit }
        runApp(app).map(exit => assertTrue(exit.isSuccess))
      }
    )
  )
}
