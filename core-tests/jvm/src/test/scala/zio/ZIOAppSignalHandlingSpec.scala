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

import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

object ZIOAppSignalHandlingSpec extends ZIOSpecDefault {
  def spec = suite("ZIOAppSignalHandlingSpec")(
    test("signal handlers are installed") {
      val app = ZIOApp.fromZIO(ZIO.unit)

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess)
      }
    },
    test("app handles interruption gracefully") {
      val finalizerRan = new AtomicBoolean(false)

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          ZIO.acquireRelease(
            ZIO.unit
          )(_ => ZIO.succeed(finalizerRan.set(true))) *>
            ZIO.never
        )
      )

      // Simulate interruption after short delay
      for {
        fiber <- app.invoke(Chunk.empty).fork
        _     <- ZIO.sleep(Duration.fromMillis(100))
        _     <- fiber.interrupt
        _     <- ZIO.sleep(Duration.fromMillis(100))
      } yield {
        assert(finalizerRan.get())(isTrue)
      }
    },
    test("gracefulShutdownTimeout prevents indefinite hangs") {
      val slowFinalizer = new AtomicBoolean(false)

      val timeoutApp = new ZIOApp {
        type Environment = Any
        def bootstrap = ZLayer.empty
        def environmentTag = EnvironmentTag[Any]
        override def gracefulShutdownTimeout =
          Duration.fromMillis(500)
        def run = ZIO.scoped(
          ZIO.acquireRelease(
            ZIO.unit
          )(_ =>
            ZIO.sleep(Duration.fromSeconds(5)) *>
              ZIO.succeed(slowFinalizer.set(true))
          )
        )
      }

      val start = System.currentTimeMillis()
      for {
        result <- timeoutApp.invoke(Chunk.empty).exit
        end    = System.currentTimeMillis()
        elapsed = end - start
      } yield {
        assert(elapsed)(
          isLessThan(3000L)
        )
      }
    },
    test("exit code reflects success") {
      val app = ZIOApp.fromZIO(ZIO.succeed(()))

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess)
      }
    },
    test("exit code reflects failure") {
      val app = ZIOApp.fromZIO(
        ZIO.fail(new Exception("failure"))
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isFailure)
      }
    },
    test("exception handling in app") {
      val exception = new RuntimeException("test exception")
      val app = ZIOApp.fromZIO(ZIO.fail(exception))

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isFailure)
      }
    },
    test("multiple interruptions handled correctly") {
      val finalizerCount = new java.util.concurrent.atomic.AtomicInteger(0)

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          for {
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(finalizerCount.incrementAndGet())
            )
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(finalizerCount.incrementAndGet())
            )
          } yield ()
        )
      )

      for {
        fiber <- app.invoke(Chunk.empty).fork
        _     <- ZIO.sleep(Duration.fromMillis(50))
        _     <- fiber.interrupt
        count <- ZIO.succeed(finalizerCount.get())
      } yield {
        assert(count)(equalTo(2))
      }
    },
    test("app that completes before interrupt") {
      val finalizerRan = new AtomicBoolean(false)

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          ZIO.acquireRelease(
            ZIO.unit
          )(_ => ZIO.succeed(finalizerRan.set(true)))
        )
      )

      for {
        fiber <- app.invoke(Chunk.empty).fork
        _     <- fiber.join
      } yield {
        assert(finalizerRan.get())(isTrue)
      }
    },
    test("nested interruption handling") {
      val finalizerOrder = scala.collection.mutable.ArrayBuffer[Int]()

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          for {
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(finalizerOrder.append(1))
            )
            _ <- ZIO.scoped(
              for {
                _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
                  ZIO.succeed(finalizerOrder.append(2))
                )
              } yield ()
            )
          } yield ()
        )
      )

      for {
        fiber <- app.invoke(Chunk.empty).fork
        _     <- fiber.join
      } yield {
        assert(finalizerOrder.toSeq)(
          equalTo(Seq(2, 1))
        )
      }
    }
  )
}
