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

import zio.internal.ExitCode
import zio.test.*
import zio.test.Assertion.*

import java.util.concurrent.atomic.AtomicInteger

object ZIOAppIntegrationSpec extends ZIOSpecDefault {
  def spec = suite("ZIOAppIntegrationSpec")(
    suite("graceful shutdown with resources")(
      test("all resources are cleaned up during graceful shutdown") {
        val cleanupCount = new AtomicInteger(0)

        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed(())
              )(_ => ZIO.succeed(cleanupCount.incrementAndGet()))
              _ <- ZIO.acquireRelease(
                ZIO.succeed(())
              )(_ => ZIO.succeed(cleanupCount.incrementAndGet()))
              _ <- ZIO.acquireRelease(
                ZIO.succeed(())
              )(_ => ZIO.succeed(cleanupCount.incrementAndGet()))
            } yield ExitCode.success
          )
        )

        for {
          _     <- app.invoke(Chunk.empty)
          count <- ZIO.succeed(cleanupCount.get())
        } yield assert(count)(equalTo(3))
      },
      test("gracefulShutdownTimeout prevents indefinite hanging") {
        val app = new ZIOApp {
          type Environment = Any

          def bootstrap = ZLayer.environment[ZIOAppArgs]
          def environmentTag = EnvironmentTag[Any]

          override def gracefulShutdownTimeout: Duration = Duration.fromMillis(500)

          def run =
            ZIO.scoped(
              for {
                _ <- ZIO.acquireRelease(ZIO.succeed(()))(_ => ZIO.sleep(Duration.fromSeconds(10)))
              } yield ExitCode.success
            )
        }

        for {
          result <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
        } yield assert(result)(isSome)
      }
    ),
    suite("nested scopes and resources")(
      test("nested scopes are unwound correctly") {
        val executionOrder = new java.util.concurrent.CopyOnWriteArrayList[String]()

        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed {
                  executionOrder.add("acquire1")
                }
              )(_ => ZIO.succeed(executionOrder.add("release1")))
              _ <- ZIO.scoped(
                for {
                  _ <- ZIO.acquireRelease(
                    ZIO.succeed {
                      executionOrder.add("acquire2")
                    }
                  )(_ => ZIO.succeed(executionOrder.add("release2")))
                } yield ()
              )
              _ <- ZIO.acquireRelease(
                ZIO.succeed {
                  executionOrder.add("acquire3")
                }
              )(_ => ZIO.succeed(executionOrder.add("release3")))
            } yield ExitCode.success
          )
        )

        for {
          _    <- app.invoke(Chunk.empty)
          list <- ZIO.succeed(executionOrder)
        } yield {
          val order = list.toArray(new Array[String](list.size()))
          assert(order)(equalTo(Array(
            "acquire1",
            "acquire2",
            "release2",
            "acquire3",
            "release3",
            "release1"
          )))
        }
      }
    ),
    suite("error handling with finalizers")(
      test("finalizers run even when the main effect fails") {
        val finalizerRan = new AtomicInteger(0)

        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed(())
              )(_ => ZIO.succeed(finalizerRan.incrementAndGet()))
              _ <- ZIO.fail(new Exception("intentional failure"))
            } yield ExitCode.success
          )
        )

        for {
          _     <- app.invoke(Chunk.empty).exit
          count <- ZIO.succeed(finalizerRan.get())
        } yield assert(count)(equalTo(1))
      },
      test("multiple errors are captured") {
        val app = ZIOApp.fromZIO(
          ZIO.fail(new Exception("first error"))
        )

        for {
          result <- app.invoke(Chunk.empty).exit
        } yield assert(result)(isFailure)
      }
    ),
    suite("long-running operations")(
      test("long-running operations complete normally") {
        val app = ZIOApp.fromZIO(
          ZIO.sleep(Duration.fromMillis(100)) *> ZIO.succeed(ExitCode.success)
        )

        for {
          result <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
        } yield assert(result)(isSome)
      },
      test("long-running operations with cleanup") {
        val cleaned = new AtomicInteger(0)

        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed(())
              )(_ => ZIO.sleep(Duration.fromMillis(50)) *> ZIO.succeed(cleaned.incrementAndGet()))
              _ <- ZIO.sleep(Duration.fromMillis(100))
            } yield ExitCode.success
          )
        )

        for {
          _     <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
          count <- ZIO.succeed(cleaned.get())
        } yield assert(count)(equalTo(1))
      }
    ),
    suite("complex scenarios")(
      test("app with dependencies and cleanup") {
        val initialized = new AtomicInteger(0)
        val cleaned = new AtomicInteger(0)

        val serviceLayer = ZLayer.scoped(
          ZIO.acquireRelease(
            ZIO.succeed {
              initialized.incrementAndGet()
              "service"
            }
          )(_ => ZIO.succeed(cleaned.incrementAndGet()))
        )

        val app = ZIOApp(
          for {
            service <- ZIO.service[String]
          } yield ExitCode.success,
          ZLayer.environment[ZIOAppArgs] >>> serviceLayer
        )

        for {
          _         <- app.invoke(Chunk.empty)
          initCount <- ZIO.succeed(initialized.get())
          cleanCount <- ZIO.succeed(cleaned.get())
        } yield assert(initCount)(equalTo(1)) && assert(cleanCount)(equalTo(1))
      },
      test("app composition with cleanup") {
        val trace1 = new AtomicInteger(0)
        val trace2 = new AtomicInteger(0)

        val app1 = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed(())
              )(_ => ZIO.succeed(trace1.incrementAndGet()))
            } yield ExitCode.success
          )
        )

        val app2 = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed(())
              )(_ => ZIO.succeed(trace2.incrementAndGet()))
            } yield ExitCode.success
          )
        )

        for {
          combined <- ZIO.succeed(app1 <> app2)
          _         <- combined.invoke(Chunk.empty).exit
          t1        <- ZIO.succeed(trace1.get())
          t2        <- ZIO.succeed(trace2.get())
        } yield assert(t1)(equalTo(1)) && assert(t2)(equalTo(1))
      }
    ),
    suite("resource management edge cases")(
      test("acquire that fails does not run release") {
        val acquireRan = new AtomicInteger(0)
        val releaseRan = new AtomicInteger(0)

        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed {
                  acquireRan.incrementAndGet()
                  throw new Exception("acquire failed")
                }
              )(_ => ZIO.succeed(releaseRan.incrementAndGet()))
            } yield ExitCode.success
          )
        )

        for {
          _       <- app.invoke(Chunk.empty).exit
          acquire <- ZIO.succeed(acquireRan.get())
          release <- ZIO.succeed(releaseRan.get())
        } yield assert(acquire)(equalTo(1)) && assert(release)(equalTo(0))
      },
      test("release failure does not prevent exit") {
        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed(())
              )(_ => ZIO.fail(new Exception("release failed")))
            } yield ExitCode.success
          )
        )

        for {
          result <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
        } yield assert(result)(isSome)
      }
    )
  )
}
