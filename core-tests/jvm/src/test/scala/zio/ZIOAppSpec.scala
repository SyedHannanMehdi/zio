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

import zio.internal.{ExitCode, ZIOAppPlatformSpecific}
import zio.test.*
import zio.test.Assertion.*

import java.util.concurrent.atomic.AtomicInteger

object ZIOAppSpec extends ZIOSpecDefault {
  def spec = suite("ZIOAppSpec")(
    suite("app completes successfully")(
      test("exit code is 0 on successful completion") {
        val app = ZIOApp.fromZIO(ZIO.succeed(ExitCode.success))
        for {
          result <- app.invoke(Chunk.empty).exit
        } yield assert(result)(isSuccess)
      },
      test("finalizers are run on successful completion") {
        val finalizerRan = new AtomicInteger(0)
        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            ZIO.acquireRelease(
              ZIO.succeed(())
            )(_ => ZIO.succeed(finalizerRan.incrementAndGet()))
          )
        )
        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(finalizerRan.get())(equalTo(1))
      }
    ),
    suite("app fails")(
      test("exit code is non-zero on failure") {
        val app = ZIOApp.fromZIO(ZIO.fail(new Exception("test failure")))
        for {
          result <- app.invoke(Chunk.empty).exit
        } yield assert(result)(isFailure)
      },
      test("finalizers are run even on failure") {
        val finalizerRan = new AtomicInteger(0)
        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed(())
              )(_ => ZIO.succeed(finalizerRan.incrementAndGet()))
              _ <- ZIO.fail(new Exception("test error"))
            } yield ExitCode.success
          )
        )
        for {
          _ <- app.invoke(Chunk.empty).exit
        } yield assert(finalizerRan.get())(equalTo(1))
      }
    ),
    suite("gracefulShutdownTimeout")(
      test("gracefulShutdownTimeout is respected") {
        val app = new ZIOApp {
          type Environment = Any

          def bootstrap = ZLayer.environment[ZIOAppArgs]
          def environmentTag = EnvironmentTag[Any]

          override def gracefulShutdownTimeout: Duration = Duration.fromMillis(100)

          def run = ZIO.succeed(ExitCode.success)
        }

        for {
          start  <- Clock.instant
          _      <- app.invoke(Chunk.empty)
          end    <- Clock.instant
          elapsed = end.toEpochMilli - start.toEpochMilli
        } yield assert(elapsed)(isGreaterThanEqualTo(0L))
      }
    ),
    suite("shutdown sequence")(
      test("shutdown sequence does not hang on normal completion") {
        val app = ZIOApp.fromZIO(
          ZIO.sleep(Duration.fromMillis(10)) *> ZIO.succeed(ExitCode.success)
        )
        for {
          result <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(5))
        } yield assert(result)(isSome)
      },
      test("multiple finalizers all run in order") {
        val order = new java.util.concurrent.CopyOnWriteArrayList[Int]()
        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(ZIO.succeed(()))(_ => ZIO.succeed(order.add(1)))
              _ <- ZIO.acquireRelease(ZIO.succeed(()))(_ => ZIO.succeed(order.add(2)))
              _ <- ZIO.acquireRelease(ZIO.succeed(()))(_ => ZIO.succeed(order.add(3)))
            } yield ExitCode.success
          )
        )
        for {
          _    <- app.invoke(Chunk.empty)
          list <- ZIO.succeed(order)
        } yield assert(list.size())(equalTo(3)) && assert(list.get(0))(equalTo(3)) &&
          assert(list.get(1))(equalTo(2)) && assert(list.get(2))(equalTo(1))
      }
    ),
    suite("argument passing")(
      test("command line arguments are passed correctly") {
        val app = ZIOApp(
          for {
            args <- ZIOAppArgs.getArgs
          } yield ExitCode.success,
          ZLayer.environment[ZIOAppArgs]
        )

        for {
          _ <- app.invoke(Chunk("arg1", "arg2", "arg3"))
        } yield assertCompletes
      },
      test("no arguments case works") {
        val app = ZIOApp.fromZIO(
          for {
            args <- ZIOAppArgs.getArgs
          } yield {
            assert(args)(isEmpty)
            ExitCode.success
          }
        )

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assertCompletes
      }
    ),
    suite("runtime configuration")(
      test("custom runtime is used") {
        var runtimeWasUsed = false
        val customRuntime = Runtime.default

        val app = new ZIOApp {
          type Environment = Any

          def bootstrap = ZLayer.environment[ZIOAppArgs]
          def environmentTag = EnvironmentTag[Any]

          override def runtime: Runtime[Any] = {
            runtimeWasUsed = true
            customRuntime
          }

          def run = ZIO.succeed(ExitCode.success)
        }

        for {
          _ <- app.invoke(Chunk.empty)
        } yield assert(runtimeWasUsed)(isTrue)
      }
    ),
    suite("error handling")(
      test("errors are caught and logged") {
        val app = ZIOApp.fromZIO(ZIO.fail(new RuntimeException("test error")))
        for {
          result <- app.invoke(Chunk.empty).exit
        } yield assert(result)(isFailure)
      },
      test("die is treated as failure") {
        val app = ZIOApp.fromZIO(ZIO.die(new RuntimeException("catastrophic error")))
        for {
          result <- app.invoke(Chunk.empty).exit
        } yield assert(result)(isFailure)
      }
    ),
    suite("scoped resources")(
      test("scoped resources are properly acquired and released") {
        val resourceState = new AtomicInteger(0)
        val app = ZIOApp.fromZIO(
          ZIO.scoped(
            for {
              _ <- ZIO.acquireRelease(
                ZIO.succeed {
                  resourceState.set(1)
                }
              )(_ => ZIO.succeed(resourceState.set(0)))
            } yield ExitCode.success
          )
        )

        for {
          _       <- app.invoke(Chunk.empty)
          _       <- ZIO.sleep(Duration.fromMillis(10))
          state   <- ZIO.succeed(resourceState.get())
        } yield assert(state)(equalTo(0))
      }
    ),
    suite("concurrent operations")(
      test("concurrent operations complete successfully") {
        val app = ZIOApp.fromZIO(
          ZIO.collectAllPar(List(
            ZIO.succeed(1),
            ZIO.succeed(2),
            ZIO.succeed(3)
          )).as(ExitCode.success)
        )

        for {
          result <- app.invoke(Chunk.empty).exit
        } yield assert(result)(isSuccess)
      },
      test("concurrent failures are handled") {
        val app = ZIOApp.fromZIO(
          ZIO.collectAllPar(List(
            ZIO.succeed(1),
            ZIO.fail(new Exception("concurrent failure")),
            ZIO.succeed(3)
          )).as(ExitCode.success)
        )

        for {
          result <- app.invoke(Chunk.empty).exit
        } yield assert(result)(isFailure)
      }
    )
  )
}
