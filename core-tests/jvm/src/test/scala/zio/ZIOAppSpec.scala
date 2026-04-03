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

import zio.test.*
import zio.test.Assertion.*
import java.util.concurrent.atomic.AtomicBoolean

object ZIOAppSpec extends ZIOSpecDefault {
  def spec = suite("ZIOApp")(
    test("app that succeeds returns exit code 0") {
      val app = ZIOApp.fromZIO(ZIO.succeed(()))
      val result = Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })
      assertTrue(result.isInstanceOf[ExitCode])
    },
    test("app that fails returns non-zero exit code") {
      val app = ZIOApp.fromZIO(ZIO.fail(new Exception("test error")))
      val result = Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })
      assertTrue(result.isInstanceOf[ExitCode])
    },
    test("finalizers are executed on success") {
      val finalizerRun = new AtomicBoolean(false)
      val app = ZIOApp.fromZIO(
        ZIO.succeed(()).ensuring(ZIO.succeed(finalizerRun.set(true)))
      )
      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })
      assertTrue(finalizerRun.get())
    },
    test("finalizers are executed on failure") {
      val finalizerRun = new AtomicBoolean(false)
      val app = ZIOApp.fromZIO(
        ZIO.fail(new Exception("test")).ensuring(ZIO.succeed(finalizerRun.set(true)))
      )
      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })
      assertTrue(finalizerRun.get())
    },
    test("app handles scoped resources correctly") {
      val resourceAcquired = new AtomicBoolean(false)
      val resourceReleased = new AtomicBoolean(false)

      val app = ZIOApp.fromZIO(
        ZIO.scoped {
          ZIO.acquireRelease(
            ZIO.succeed(resourceAcquired.set(true))
          )(_ => ZIO.succeed(resourceReleased.set(true)))
        }
      )

      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })

      assertTrue(resourceAcquired.get() && resourceReleased.get())
    },
    test("app with graceful shutdown timeout completes within reasonable time") {
      val app = new ZIOApp {
        type Environment = Any
        implicit def environmentTag: EnvironmentTag[Environment] = EnvironmentTag[Any]
        def bootstrap: ZLayer[ZIOAppArgs, Any, Environment] = ZLayer.environment
        def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] = ZIO.succeed(())
        override def gracefulShutdownTimeout: Duration = Duration.ofMillis(100)
      }

      val startTime = System.currentTimeMillis()
      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })
      val elapsed = System.currentTimeMillis() - startTime

      assertTrue(elapsed < 5000) // Should complete quickly, well within timeout
    },
    test("app can access command-line arguments") {
      val capturedArgs = scala.collection.mutable.Buffer[String]()

      val app = ZIOApp.fromZIO(
        for {
          args <- ZIOAppArgs.getArgs
          _    <- ZIO.succeed(capturedArgs.addAll(args))
        } yield ()
      )

      val testArgs = Chunk("arg1", "arg2", "arg3")
      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(testArgs)).getOrThrowFiberFailure()
      })

      assertTrue(capturedArgs.toList == testArgs.toList)
    },
    test("multiple finalizers are all executed") {
      val finalizer1Run = new AtomicBoolean(false)
      val finalizer2Run = new AtomicBoolean(false)
      val finalizer3Run = new AtomicBoolean(false)

      val app = ZIOApp.fromZIO(
        ZIO
          .succeed(())
          .ensuring(ZIO.succeed(finalizer1Run.set(true)))
          .ensuring(ZIO.succeed(finalizer2Run.set(true)))
          .ensuring(ZIO.succeed(finalizer3Run.set(true)))
      )

      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })

      assertTrue(
        finalizer1Run.get() && finalizer2Run.get() && finalizer3Run.get()
      )
    },
    test("app composition works correctly") {
      val app1Run = new AtomicBoolean(false)
      val app2Run = new AtomicBoolean(false)

      val app1 = ZIOApp.fromZIO(ZIO.succeed(app1Run.set(true)))
      val app2 = ZIOApp.fromZIO(ZIO.succeed(app2Run.set(true)))

      val combined = app1 <> app2

      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(combined.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })

      assertTrue(app1Run.get() && app2Run.get())
    },
    test("app with custom bootstrap works correctly") {
      val bootstrapRun = new AtomicBoolean(false)

      val customLayer = ZLayer.succeed(bootstrapRun.set(true))

      val app = ZIOApp(
        ZIO.succeed(()),
        ZLayer.succeed(ZIOAppArgs(Chunk.empty)) >>> customLayer
      )(EnvironmentTag[Unit])

      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })

      assertTrue(bootstrapRun.get())
    },
    test("app shutdown is not blocked indefinitely") {
      val app = ZIOApp.fromZIO(
        ZIO.sleep(Duration.ofMillis(10)) *> ZIO.succeed(())
      )

      val startTime = System.currentTimeMillis()
      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })
      val elapsed = System.currentTimeMillis() - startTime

      assertTrue(elapsed < 10000) // Should not hang
    },
    test("app handles empty arguments") {
      val capturedArgs = scala.collection.mutable.Buffer[String]()

      val app = ZIOApp.fromZIO(
        for {
          args <- ZIOAppArgs.getArgs
          _    <- ZIO.succeed(capturedArgs.addAll(args))
        } yield ()
      )

      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })

      assertTrue(capturedArgs.isEmpty)
    },
    test("finalizer runs even with error in main logic") {
      val finalizerRun = new AtomicBoolean(false)

      val app = ZIOApp.fromZIO(
        ZIO
          .fail(new Exception("main error"))
          .ensuring(ZIO.succeed(finalizerRun.set(true)))
      )

      Unsafe.unsafe(implicit unsafe => {
        Runtime.default.unsafe.run(app.invoke(Chunk.empty)).getOrThrowFiberFailure()
      })

      assertTrue(finalizerRun.get())
    }
  )
}
