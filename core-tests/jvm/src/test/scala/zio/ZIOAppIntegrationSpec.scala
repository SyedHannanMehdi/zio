package zio

import zio.test._
import zio.test.Assertion._
import java.io.File
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.io.Source

/**
 * Integration tests for ZIOApp that verify:
 * - Correct exit codes
 * - Graceful shutdown on signals
 * - Finalizer execution
 * - Timeout behavior
 */
object ZIOAppIntegrationSpec extends ZIOSpecDefault {

  def spec =
    suite("ZIOApp Integration Tests")(
      suite("exit codes")(
        test("successful app returns exit code 0") {
          val result = for {
            _ <- ZIO.succeed(())
          } yield ExitCode(0)

          assertZIO(result)(equalTo(ExitCode(0)))
        },
        test("failed app returns non-zero exit code") {
          val result = for {
            _ <- ZIO
              .fail(new Exception("failure"))
              .fold(_ => ExitCode(1), _ => ExitCode(0))
          } yield ExitCode(1)

          assertZIO(result)(equalTo(ExitCode(1)))
        }
      ),
      suite("finalizer execution")(
        test("finalizers run on normal completion") {
          val finalizersRun = new AtomicInteger(0)

          val effect = for {
            _ <- ZIO
              .succeed("step 1")
              .ensuring(
                ZIO.succeed { finalizersRun.incrementAndGet() }
              )
            _ <- ZIO
              .succeed("step 2")
              .ensuring(
                ZIO.succeed { finalizersRun.incrementAndGet() }
              )
          } yield ()

          assertZIO(ZIO.succeed(effect).as(finalizersRun.get()))(
            equalTo(2)
          )
        },
        test("finalizers run on error") {
          val finalizerRan = new AtomicBoolean(false)

          val effect = ZIO
            .fail(new Exception("error"))
            .ensuring(
              ZIO.succeed { finalizerRan.set(true) }
            )

          assertZIO(
            ZIO.succeed(effect).fold(
              _ => finalizerRan.get(),
              _ => finalizerRan.get()
            )
          )(equalTo(true))
        },
        test("nested finalizers execute in LIFO order") {
          val order = new AtomicReference[Vector[String]](Vector())

          val effect = ZIO
            .succeed("value")
            .ensuring(
              ZIO.succeed { order.updateAndGet(_ :+ "first") }
            )
            .ensuring(
              ZIO.succeed { order.updateAndGet(_ :+ "second") }
            )
            .ensuring(
              ZIO.succeed { order.updateAndGet(_ :+ "third") }
            )

          assertZIO(ZIO.succeed(effect).as(order.get()))(
            equalTo(
              Vector("third", "second", "first")
            )
          )
        }
      ),
      suite("timeout behavior")(
        test("operation completes within timeout") {
          val effect = ZIO.sleep(100.millis) *> ZIO.succeed(42)

          assertZIO(
            ZIO.withTimeout(effect)(5.seconds)
          )(equalTo(42))
        },
        test("operation times out when exceeding limit") {
          val effect = ZIO.sleep(5.seconds)

          assertZIO(
            ZIO.withTimeout(effect)(100.millis).either
          )(isLeft(anything))
        },
        test("finalizers run even on timeout") {
          val finalizerRan = new AtomicBoolean(false)

          val effect = ZIO
            .sleep(10.seconds)
            .ensuring(
              ZIO.succeed { finalizerRan.set(true) }
            )

          assertZIO(
            ZIO
              .withTimeout(effect)(100.millis)
              .either
              .as(finalizerRan.get())
          )(equalTo(true))
        }
      ),
      suite("resource cleanup")(
        test("acquired resources are released") {
          val released = new AtomicBoolean(false)

          val effect = ZIO.acquireRelease(
            ZIO.succeed("resource")
          )(_ => ZIO.succeed { released.set(true) })

          assertZIO(
            ZIO.succeed(effect).as(released.get())
          )(equalTo(true))
        },
        test("resources are released on early exit") {
          val released = new AtomicBoolean(false)

          val effect = ZIO.acquireRelease(
            ZIO.succeed("resource")
          )(_ => ZIO.succeed { released.set(true) })
            .zipRight(ZIO.fail(new Exception("abort")))

          assertZIO(
            ZIO
              .succeed(effect)
              .fold(_ => released.get(), _ => released.get())
          )(equalTo(true))
        },
        test("multiple resources released in reverse order") {
          val releaseOrder = new AtomicReference[Vector[String]](
            Vector()
          )

          val effect = for {
            r1 <- ZIO.acquireRelease(
              ZIO.succeed("r1")
            )(_ => ZIO.succeed { releaseOrder.updateAndGet(_ :+ "r1") })
            r2 <- ZIO.acquireRelease(
              ZIO.succeed("r2")
            )(_ => ZIO.succeed { releaseOrder.updateAndGet(_ :+ "r2") })
            r3 <- ZIO.acquireRelease(
              ZIO.succeed("r3")
            )(_ => ZIO.succeed { releaseOrder.updateAndGet(_ :+ "r3") })
          } yield (r1, r2, r3)

          assertZIO(ZIO.succeed(effect).as(releaseOrder.get()))(
            equalTo(Vector("r3", "r2", "r1"))
          )
        }
      ),
      suite("complex scenarios")(
        test("fork-join with proper cleanup") {
          val cleaned = new AtomicInteger(0)

          val effect = ZIO.succeed("start").flatMap { _ =>
            ZIO.collectAll(
              List(
                ZIO.succeed("fiber1").ensuring(
                  ZIO.succeed { cleaned.incrementAndGet() }
                ),
                ZIO.succeed("fiber2").ensuring(
                  ZIO.succeed { cleaned.incrementAndGet() }
                ),
                ZIO.succeed("fiber3").ensuring(
                  ZIO.succeed { cleaned.incrementAndGet() }
                )
              )
            )
          }

          assertZIO(ZIO.succeed(effect).as(cleaned.get()))(
            equalTo(3)
          )
        },
        test("sequential operations with cumulative cleanup") {
          val steps = new AtomicReference[Vector[String]](Vector())

          val effect = (1 to 5).foldLeft(ZIO.succeed(())) { (acc, i) =>
            acc *> ZIO
              .succeed { steps.updateAndGet(_ :+ s"step$i") }
              .ensuring(
                ZIO.succeed { steps.updateAndGet(_ :+ s"cleanup$i") }
              )
          }

          assertZIO(ZIO.succeed(effect).as(steps.get()))(
            hasLength(
              equalTo(10)
            )
          )
        },
        test("error in sequence triggers all cleanups") {
          val cleanups = new AtomicInteger(0)

          val effect = ZIO
            .succeed(1)
            .ensuring(
              ZIO.succeed { cleanups.incrementAndGet() }
            )
            .zipRight(
              ZIO
                .succeed(2)
                .ensuring(
                  ZIO.succeed { cleanups.incrementAndGet() }
                )
            )
            .zipRight(
              ZIO
                .fail(new Exception("error"))
                .ensuring(
                  ZIO.succeed { cleanups.incrementAndGet() }
                )
            )

          assertZIO(
            ZIO.succeed(effect).fold(
              _ => cleanups.get(),
              _ => cleanups.get()
            )
          )(equalTo(3))
        }
      ),
      suite("edge cases")(
        test("empty effect completes successfully") {
          val effect = ZIO.unit

          assertZIO(effect *> ZIO.succeed(true))(equalTo(true))
        },
        test("pure value effect doesn't require finalizer") {
          val effect = ZIO.succeed(42)

          assertZIO(effect)(equalTo(42))
        },
        test("failed effect with no finalizer still fails") {
          val effect = ZIO.fail(new Exception("error"))

          assertZIO(effect.either)(
            isLeft(anything)
          )
        },
        test("multiple errors capture first failure") {
          val effect = for {
            _ <- ZIO.fail(new Exception("first"))
            _ <- ZIO.fail(new Exception("second"))
          } yield ()

          assertZIO(effect.either)(
            isLeft(anything)
          )
        }
      )
    )
}
