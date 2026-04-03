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

import zio.internal.RuntimePlatformSpecific
import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

object ZIOAppSpec extends ZIOSpecDefault {
  def spec = suite("ZIOAppSpec")(
    test(
      "app completes successfully with exit code 0"
    ) {
      val app = ZIOApp.fromZIO(ZIO.succeed(()))

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess(isUnit))
      }
    },
    test("app fails with non-zero exit code") {
      val app = ZIOApp.fromZIO(ZIO.fail(new Exception("test failure")))

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(
          isFailure
        )
      }
    },
    test("finalizers are run on success") {
      val finalizerRan = new AtomicBoolean(false)

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          ZIO.acquireRelease(
            ZIO.unit
          )(_ => ZIO.succeed(finalizerRan.set(true)))
        )
      )

      for {
        result <- app.invoke(Chunk.empty).exit
        ran    <- ZIO.succeed(finalizerRan.get())
      } yield {
        assert(ran)(isTrue)
      }
    },
    test("finalizers are run on failure") {
      val finalizerRan = new AtomicBoolean(false)

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          ZIO.acquireRelease(
            ZIO.unit
          )(_ => ZIO.succeed(finalizerRan.set(true))) *> ZIO.fail(
            new Exception("test")
          )
        )
      )

      for {
        result <- app.invoke(Chunk.empty).exit
        ran    <- ZIO.succeed(finalizerRan.get())
      } yield {
        assert(ran)(isTrue)
      }
    },
    test("multiple finalizers are run in reverse order") {
      val order = scala.collection.mutable.ArrayBuffer[Int]()

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          for {
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(order.append(1))
            )
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(order.append(2))
            )
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(order.append(3))
            )
          } yield ()
        )
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(order.toSeq)(equalTo(Seq(3, 2, 1)))
      }
    },
    test("gracefulShutdownTimeout is respected") {
      val timeoutApp = new ZIOApp {
        type Environment = Any
        def bootstrap = ZLayer.empty
        def environmentTag = EnvironmentTag[Any]
        override def gracefulShutdownTimeout =
          Duration.fromMillis(100)
        def run = ZIO.sleep(Duration.fromSeconds(10))
      }

      val start = System.currentTimeMillis()
      for {
        result <- timeoutApp.invoke(Chunk.empty).exit
        end    = System.currentTimeMillis()
        elapsed = end - start
      } yield {
        assert(elapsed)(
          isGreaterThanOrEqualTo(100L) &&
            isLessThan(2000L)
        )
      }
    },
    test("app receives command-line arguments") {
      val capturedArgs = scala.collection.mutable.ArrayBuffer[String]()

      val app = ZIOApp.fromZIO(
        for {
          args <- ZIOAppArgs.getArgs
          _    <- ZIO.succeed(capturedArgs.appendAll(args))
        } yield ()
      )

      for {
        result <- app.invoke(Chunk("arg1", "arg2", "arg3")).exit
      } yield {
        assert(capturedArgs.toSeq)(
          equalTo(Seq("arg1", "arg2", "arg3"))
        )
      }
    },
    test("bootstrap layer is properly initialized") {
      val initialized = new AtomicBoolean(false)

      val customLayer: ZLayer[ZIOAppArgs, Nothing, String] =
        ZLayer.succeed("test-value").tap(_ =>
          ZIO.succeed(initialized.set(true))
        )

      val app = ZIOApp(
        ZIO.service[String].map(_ => ()),
        customLayer
      )(EnvironmentTag[String])

      for {
        result <- app.invoke(Chunk.empty).exit
        init   <- ZIO.succeed(initialized.get())
      } yield {
        assert(init)(isTrue)
      }
    },
    test("nested scopes are properly cleaned up") {
      val cleanups = new AtomicInteger(0)

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          for {
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(cleanups.incrementAndGet())
            )
            _ <- ZIO.scoped(
              for {
                _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
                  ZIO.succeed(cleanups.incrementAndGet())
                )
              } yield ()
            )
          } yield ()
        )
      )

      for {
        result <- app.invoke(Chunk.empty).exit
        count  <- ZIO.succeed(cleanups.get())
      } yield {
        assert(count)(equalTo(2))
      }
    },
    test("error cause is logged for failures") {
      val app = ZIOApp.fromZIO(
        ZIO.fail(new Exception("explicit test error"))
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isFailure)
      }
    },
    test("app with dependencies on bootstrap") {
      val value = "bootstrap-value"

      val layer: ZLayer[ZIOAppArgs, Nothing, String] =
        ZLayer.succeed(value)

      val app = ZIOApp(
        ZIO.service[String],
        layer
      )(EnvironmentTag[String])

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess(equalTo(value)))
      }
    },
    test(
      "shutdown sequence doesn't hang on success"
    ) {
      val app = ZIOApp.fromZIO(ZIO.succeed(42))

      val start = System.currentTimeMillis()
      for {
        result <- app.invoke(Chunk.empty).exit
        end    = System.currentTimeMillis()
        elapsed = end - start
      } yield {
        assert(elapsed)(
          isLessThan(5000L)
        ) && assert(result)(
          isSuccess(equalTo(42))
        )
      }
    },
    test(
      "shutdown sequence doesn't hang on failure"
    ) {
      val app = ZIOApp.fromZIO(
        ZIO.fail(new Exception("fail"))
      )

      val start = System.currentTimeMillis()
      for {
        result <- app.invoke(Chunk.empty).exit
        end    = System.currentTimeMillis()
        elapsed = end - start
      } yield {
        assert(elapsed)(
          isLessThan(5000L)
        ) && assert(result)(
          isFailure
        )
      }
    },
    test("finalizer errors don't prevent other finalizers") {
      val order = scala.collection.mutable.ArrayBuffer[Int]()

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          for {
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(order.append(1))
            )
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(order.append(2)) *>
                ZIO.fail(new Exception("finalizer error"))
            )
            _ <- ZIO.acquireRelease(ZIO.unit)(_ =>
              ZIO.succeed(order.append(3))
            )
          } yield ()
        )
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(order.toSeq.sorted)(
          equalTo(Seq(1, 2, 3))
        )
      }
    },
    test("app composition works correctly") {
      val app1 = ZIOApp.fromZIO(ZIO.succeed(1))
      val app2 = ZIOApp.fromZIO(ZIO.succeed(2))

      val combined = app1 <> app2

      for {
        result <- combined.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess)
      }
    }
  )
}
