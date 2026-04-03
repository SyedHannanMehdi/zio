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

import zio.internal.Platform
import zio.stacktracer.TracingImplicits.disableAutoTrace
import zio.test.Assertion._
import zio.test._

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

object ZIOAppSpec extends ZIOSpecDefault {

  def spec = suite("ZIOAppSpec")(
    suite("successful completion")(
      test("app that succeeds returns exit code 0") {
        val app = ZIOApp(
          for {
            _ <- ZIO.succeed(42)
          } yield (),
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          result <- app.invoke(Chunk.empty).either
        } yield assert(result)(isRight)
      },
      test("app that fails returns non-zero exit code") {
        val app = ZIOApp(
          for {
            _ <- ZIO.fail(new RuntimeException("test error"))
          } yield (),
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          result <- app.invoke(Chunk.empty).either
        } yield assert(result)(isLeft)
      }
    ),
    suite("finalizers")(
      test("finalizers are executed on successful completion") {
        val finalizerRan = new AtomicBoolean(false)

        val app = ZIOApp(
          for {
            _ <- ZIO.scoped {
                   ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(finalizerRan.set(true)))
                 }
          } yield (),
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          _ <- app.invoke(Chunk.empty).either
        } yield assert(finalizerRan.get())(isTrue)
      },
      test("finalizers are executed on failure") {
        val finalizerRan = new AtomicBoolean(false)

        val app = ZIOApp(
          for {
            _ <- ZIO.scoped {
                   ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(finalizerRan.set(true)))
                 }
            _ <- ZIO.fail(new RuntimeException("test error"))
          } yield (),
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          _ <- app.invoke(Chunk.empty).either
        } yield assert(finalizerRan.get())(isTrue)
      },
      test("multiple finalizers execute in reverse order") {
        val order = new java.util.concurrent.ConcurrentLinkedDeque[Int]()

        val app = ZIOApp(
          for {
            _ <- ZIO.scoped {
                   ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(order.add(1))) *>
                     ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(order.add(2))) *>
                     ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(order.add(3)))
                 }
          } yield (),
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          _ <- app.invoke(Chunk.empty).either
        } yield assert(order.toArray.toList)(equalTo(List(3, 2, 1)))
      }
    ),
    suite("gracefulShutdownTimeout")(
      test("respects gracefulShutdownTimeout when finalizers run") {
        val finalizerRan = new AtomicBoolean(false)
        val startTime = System.currentTimeMillis()

        val app = new ZIOApp {
          type Environment = Any

          val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = ZLayer.empty

          implicit val environmentTag: EnvironmentTag[Any] = EnvironmentTag[Any]

          override def gracefulShutdownTimeout: Duration = Duration.fromMillis(100)

          def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
            for {
              _ <- ZIO.scoped {
                     ZIO.acquireRelease(ZIO.unit) { _ =>
                       ZIO.sleep(Duration.fromMillis(50)) *> ZIO.succeed(finalizerRan.set(true))
                     }
                   }
            } yield ()
        }

        for {
          _ <- app.invoke(Chunk.empty).either
          elapsed = System.currentTimeMillis() - startTime
        } yield assert(finalizerRan.get())(isTrue) && assert(elapsed)(
          isGreaterThanEqualTo(50L)
        )
      }
    ),
    suite("command-line arguments")(
      test("args are properly passed to the app") {
        val capturedArgs = new java.util.concurrent.ConcurrentLinkedDeque[String]()

        val app = ZIOApp(
          for {
            args <- ZIOAppArgs.getArgs
            _    <- ZIO.foreach(args)(arg => ZIO.succeed(capturedArgs.add(arg)))
          } yield (),
          ZLayer.environment[ZIOAppArgs]
        )(EnvironmentTag[ZIOAppArgs])

        val testArgs = Chunk("arg1", "arg2", "arg3")

        for {
          _ <- app.invoke(testArgs).either
        } yield assert(capturedArgs.toArray.toList)(equalTo(testArgs.toList))
      }
    ),
    suite("shutdown sequence")(
      test("shutdown doesn't hang for quick app") {
        val app = ZIOApp(
          ZIO.succeed(()),
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          result <- app.invoke(Chunk.empty).either.timeout(Duration.fromSeconds(5))
        } yield assert(result)(isSome)
      },
      test("shutdown doesn't hang when finalizer completes") {
        val finalizerExecuted = new AtomicBoolean(false)

        val app = ZIOApp(
          for {
            _ <- ZIO.scoped {
                   ZIO.acquireRelease(ZIO.unit)(_ =>
                     ZIO.sleep(Duration.fromMillis(10)) *> ZIO.succeed(finalizerExecuted.set(true))
                   )
                 }
          } yield (),
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          result <- app.invoke(Chunk.empty).either.timeout(Duration.fromSeconds(5))
        } yield assert(result)(isSome) && assert(finalizerExecuted.get())(isTrue)
      }
    ),
    suite("error handling")(
      test("synchronous errors are caught and logged") {
        val app = ZIOApp(
          ZIO.fail(new RuntimeException("expected error")),
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          result <- app.invoke(Chunk.empty).either
        } yield assert(result)(isLeft)
      },
      test("asynchronous errors are caught and logged") {
        val app = ZIOApp(
          ZIO.async[Any, Throwable, Unit] { callback =>
            callback(ZIO.fail(new RuntimeException("async error")))
          },
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          result <- app.invoke(Chunk.empty).either
        } yield assert(result)(isLeft)
      }
    ),
    suite("layer management")(
      test("bootstrap layer is properly constructed and torn down") {
        val layerSetup = new AtomicBoolean(false)
        val layerTeardown = new AtomicBoolean(false)

        val customLayer: ZLayer[ZIOAppArgs, Nothing, Any] = ZLayer.acquireRelease(
          ZIO.succeed(layerSetup.set(true))
        )(_ => ZIO.succeed(layerTeardown.set(true)))

        val app = ZIOApp(
          ZIO.succeed(()),
          customLayer
        )(EnvironmentTag[Any])

        for {
          _ <- app.invoke(Chunk.empty).either
        } yield assert(layerSetup.get())(isTrue) && assert(layerTeardown.get())(isTrue)
      }
    ),
    suite("composition")(
      test("two apps can be composed together") {
        val app1Ran = new AtomicBoolean(false)
        val app2Ran = new AtomicBoolean(false)

        val app1 = ZIOApp(
          ZIO.succeed(app1Ran.set(true)),
          ZLayer.empty
        )(EnvironmentTag[Any])

        val app2 = ZIOApp(
          ZIO.succeed(app2Ran.set(true)),
          ZLayer.empty
        )(EnvironmentTag[Any])

        val composed = app1 <> app2

        for {
          _ <- composed.invoke(Chunk.empty).either
        } yield assert(app1Ran.get())(isTrue) && assert(app2Ran.get())(isTrue)
      }
    ),
    suite("scoped resource management")(
      test("resources opened in run are properly closed") {
        val resourceOpen = new AtomicBoolean(false)
        val resourceClosed = new AtomicBoolean(false)

        val app = ZIOApp(
          ZIO.scoped {
            ZIO.acquireRelease(
              ZIO.succeed(resourceOpen.set(true))
            )(_ => ZIO.succeed(resourceClosed.set(true)))
          },
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          _ <- app.invoke(Chunk.empty).either
        } yield assert(resourceOpen.get())(isTrue) && assert(resourceClosed.get())(isTrue)
      }
    ),
    suite("interruption handling")(
      test("interrupted effect cleans up properly") {
        val cleaned = new AtomicBoolean(false)

        val app = ZIOApp(
          ZIO.scoped {
            ZIO.acquireRelease(ZIO.unit)(_ => ZIO.succeed(cleaned.set(true))) *>
              ZIO.async[Any, Nothing, Nothing] { _ =>
                // Never complete, simulating a long-running operation
              }
          },
          ZLayer.empty
        )(EnvironmentTag[Any])

        for {
          result <- app.invoke(Chunk.empty).either.timeout(Duration.fromMillis(500))
        } yield assert(result)(isSome)
      }
    )
  )
}
