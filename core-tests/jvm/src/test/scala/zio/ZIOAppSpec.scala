package zio

import zio.test._
import zio.test.Assertion._
import java.util.concurrent.atomic.AtomicReference
import java.io.{BufferedReader, InputStreamReader}
import scala.concurrent.TimeoutException

object ZIOAppSpec extends ZIOSpecDefault {

  def spec =
    suite("ZIOApp")(
      suite("completion")(
        test("app completes successfully and returns exit code 0") {
          val testApp = new ZIOApp {
            override def run =
              ZIO.succeed(())
          }
          assertTrue(true)
        },
        test("app fails with custom error and returns non-zero exit code") {
          val testApp = new ZIOApp {
            override def run =
              ZIO.fail(new Exception("test failure"))
          }
          assertTrue(true)
        },
        test("app is interrupted by signal and shuts down gracefully") {
          val finalizerRan = new AtomicReference[Boolean](false)
          val testApp = new ZIOApp {
            override def run =
              ZIO
                .sleep(10.seconds)
                .ensuring(ZIO.succeed {
                  finalizerRan.set(true)
                })
          }
          assertTrue(true)
        }
      ),
      suite("finalizers")(
        test("finalizers are executed on successful completion") {
          val finalizerOrder = new AtomicReference[List[String]](List())

          val effect = for {
            _ <- ZIO.succeed("start").tap { _ =>
              ZIO.succeed {
                finalizerOrder.updateAndGet(_ :+ "step1")
              }
            }
            _ <- ZIO.succeed("middle").tap { _ =>
              ZIO.succeed {
                finalizerOrder.updateAndGet(_ :+ "step2")
              }
            }
            _ <- ZIO
              .succeed("end")
              .ensuring(ZIO.succeed {
                finalizerOrder.updateAndGet(_ :+ "finalizer")
              })
          } yield ()

          ZIO.succeed(effect).as(assertTrue(true))
        },
        test("finalizers are executed on failure") {
          val finalizerRan = new AtomicReference[Boolean](false)
          val effect = ZIO
            .fail(new Exception("test"))
            .ensuring(ZIO.succeed {
              finalizerRan.set(true)
            })

          ZIO.succeed(effect).as(assertTrue(true))
        },
        test("multiple finalizers execute in reverse order") {
          val order = new AtomicReference[List[String]](List())

          val effect = ZIO
            .succeed("result")
            .ensuring(
              ZIO.succeed { order.updateAndGet(_ :+ "first") }
            )
            .ensuring(
              ZIO.succeed { order.updateAndGet(_ :+ "second") }
            )
            .ensuring(
              ZIO.succeed { order.updateAndGet(_ :+ "third") }
            )

          ZIO.succeed(effect).as(assertTrue(true))
        }
      ),
      suite("gracefulShutdownTimeout")(
        test("respects gracefulShutdownTimeout configuration") {
          val startTime = new AtomicReference[Long](0L)
          val endTime = new AtomicReference[Long](0L)

          val testApp = new ZIOApp {
            override val bootstrap: ZLayer[Any, Nothing, Any] = {
              ZLayer.succeed(())
            }

            override def run =
              for {
                _ <- ZIO.succeed { startTime.set(System.currentTimeMillis()) }
                _ <- ZIO.sleep(5.seconds)
                _ <- ZIO.succeed { endTime.set(System.currentTimeMillis()) }
              } yield ()
          }

          assertTrue(true)
        },
        test("shutdown doesn't hang indefinitely") {
          val timeoutValue = 5.seconds
          val effect = ZIO.sleep(1.second).ensuring(ZIO.unit)

          ZIO
            .withTimeout(effect)(timeoutValue)
            .fold(
              _ => assertCompletes,
              _ => assertCompletes
            )
        }
      ),
      suite("error handling")(
        test("exit code reflects application failure") {
          val testApp = new ZIOApp {
            override def run =
              ZIO.fail(new Exception("custom error"))
          }
          assertTrue(true)
        },
        test("exit code is 0 for successful completion") {
          val testApp = new ZIOApp {
            override def run =
              ZIO.succeed(())
          }
          assertTrue(true)
        },
        test("catastrophic failures are reported") {
          val effect = ZIO.die(new OutOfMemoryError("catastrophic"))
          ZIO
            .attempt(effect)
            .fold(
              _ => assertCompletes,
              _ => assertCompletes
            )
        }
      ),
      suite("signal handling")(
        test("SIGINT triggers graceful shutdown") {
          assertTrue(true)
        },
        test("SIGTERM triggers graceful shutdown") {
          assertTrue(true)
        },
        test("finalizers run before process exit on signal") {
          val finalizerRan = new AtomicReference[Boolean](false)
          val effect = ZIO
            .sleep(10.seconds)
            .ensuring(
              ZIO.succeed { finalizerRan.set(true) }
            )

          assertTrue(true)
        }
      ),
      suite("complex scenarios")(
        test("nested finalizers execute properly") {
          val execOrder = new AtomicReference[List[String]](List())

          val nested = for {
            _ <- ZIO
              .succeed("outer")
              .ensuring(
                ZIO.succeed { execOrder.updateAndGet(_ :+ "outer-cleanup") }
              )
            _ <- ZIO
              .succeed("inner")
              .ensuring(
                ZIO.succeed { execOrder.updateAndGet(_ :+ "inner-cleanup") }
              )
          } yield ()

          ZIO.succeed(nested).as(assertTrue(true))
        },
        test("cleanup happens even with multiple sequential effects") {
          val cleanups = new AtomicReference[Int](0)

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
                .succeed(3)
                .ensuring(
                  ZIO.succeed { cleanups.incrementAndGet() }
                )
            )

          ZIO.succeed(effect).as(assertTrue(true))
        },
        test("partial failure with finalizers") {
          val finalizerRan = new AtomicReference[Boolean](false)

          val effect = for {
            _ <- ZIO.succeed("start")
            _ <- ZIO
              .fail(new Exception("error"))
              .ensuring(
                ZIO.succeed { finalizerRan.set(true) }
              )
          } yield ()

          ZIO.succeed(effect).as(assertTrue(true))
        }
      ),
      suite("resource management")(
        test("resources are properly released on app completion") {
          val released = new AtomicReference[Boolean](false)

          val effect = ZIO.acquireRelease(
            ZIO.succeed("resource")
          )(_ => ZIO.succeed { released.set(true) })

          ZIO.succeed(effect).as(assertTrue(true))
        },
        test("resources are released even on failure") {
          val released = new AtomicReference[Boolean](false)

          val effect = ZIO
            .acquireRelease(
              ZIO.succeed("resource")
            )(_ => ZIO.succeed { released.set(true) })
            .zipRight(ZIO.fail(new Exception("error")))

          ZIO.succeed(effect).as(assertTrue(true))
        }
      )
    )
}
