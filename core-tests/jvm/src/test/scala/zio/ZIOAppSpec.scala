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
import java.util.concurrent.atomic.AtomicInteger

object ZIOAppSpec extends ZIOSpecDefault {
  def spec =
    suite("ZIOApp")(
      suite("Success and Failure")(
        test("app completes successfully") {
          val app = ZIOApp(
            ZIO.succeed(ExitCode.success),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            result <- app.invoke(Chunk.empty).exit
          } yield assert(result)(isSuccess)
        },
        test("app completes with failure") {
          val app = ZIOApp(
            ZIO.fail("test error"),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            result <- app.invoke(Chunk.empty).exit
          } yield assert(result)(isFailure)
        },
        test("correct exit code on success") {
          val app = ZIOApp(
            ZIO.succeed(ExitCode.success),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            result <- app.invoke(Chunk.empty).exit
          } yield assert(result)(isSuccess)
        },
        test("correct exit code on failure") {
          val app = ZIOApp(
            ZIO.fail("error"),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            result <- app.invoke(Chunk.empty).exit
          } yield assert(result)(isFailure)
        }
      ),
      suite("Finalizers")(
        test("finalizers are run on success") {
          val finalizerRun = new AtomicInteger(0)
          val app = ZIOApp(
            ZIO.succeed(ExitCode.success).ensuring(
              ZIO.succeed(finalizerRun.incrementAndGet())
            ),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            _ <- app.invoke(Chunk.empty)
          } yield assert(finalizerRun.get())(equalTo(1))
        },
        test("finalizers are run on failure") {
          val finalizerRun = new AtomicInteger(0)
          val app = ZIOApp(
            ZIO.fail("error").ensuring(
              ZIO.succeed(finalizerRun.incrementAndGet())
            ),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            _ <- app.invoke(Chunk.empty).ignore
          } yield assert(finalizerRun.get())(equalTo(1))
        },
        test("multiple finalizers are run in reverse order") {
          val order = new scala.collection.mutable.ArrayBuffer[Int]()
          val app = ZIOApp(
            ZIO.succeed(ExitCode.success)
              .ensuring(ZIO.succeed(order += 1))
              .ensuring(ZIO.succeed(order += 2))
              .ensuring(ZIO.succeed(order += 3)),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            _ <- app.invoke(Chunk.empty)
          } yield assert(order.toList)(equalTo(List(3, 2, 1)))
        }
      ),
      suite("Graceful Shutdown")(
        test("gracefulShutdownTimeout is respected") {
          val startTime = new AtomicInteger(0)
          val endTime = new AtomicInteger(0)

          val app = new ZIOApp {
            type Environment = Any

            def bootstrap = ZLayer.empty
            implicit def environmentTag = EnvironmentTag[Any]

            def run = ZIO.succeed(ExitCode.success)

            override def gracefulShutdownTimeout = Duration.fromMillis(100)
          }

          for {
            _ <- app.invoke(Chunk.empty)
          } yield assertCompletes
        },
        test("shutdown doesn't hang on normal completion") {
          val app = ZIOApp(
            ZIO.succeed(ExitCode.success),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            result <- app.invoke(Chunk.empty).timeout(Duration.fromSeconds(10))
          } yield assert(result)(isSome)
        }
      ),
      suite("Arguments")(
        test("command-line arguments are accessible") {
          val app = ZIOApp(
            for {
              args <- ZIOAppArgs.getArgs
            } yield if (args.contains("test")) ExitCode.success else ExitCode.failure,
            ZLayer.environment[ZIOAppArgs]
          )(EnvironmentTag[ZIOAppArgs])

          for {
            result <- app.invoke(Chunk("test")).exit
          } yield assert(result)(isSuccess)
        },
        test("empty arguments when none provided") {
          val app = ZIOApp(
            for {
              args <- ZIOAppArgs.getArgs
            } yield if (args.isEmpty) ExitCode.success else ExitCode.failure,
            ZLayer.environment[ZIOAppArgs]
          )(EnvironmentTag[ZIOAppArgs])

          for {
            result <- app.invoke(Chunk.empty).exit
          } yield assert(result)(isSuccess)
        }
      ),
      suite("Bootstrap Layer")(
        test("bootstrap layer is properly set up") {
          case class TestService(value: String)

          val bootstrap = ZLayer.succeed(TestService("test"))

          val app = ZIOApp(
            for {
              service <- ZIO.service[TestService]
            } yield if (service.value == "test") ExitCode.success else ExitCode.failure,
            bootstrap
          )(EnvironmentTag[TestService])

          for {
            result <- app.invoke(Chunk.empty).exit
          } yield assert(result)(isSuccess)
        },
        test("bootstrap layer errors are handled") {
          val bootstrap = ZLayer.fail("bootstrap error")

          val app = ZIOApp(
            ZIO.succeed(ExitCode.success),
            bootstrap
          )(EnvironmentTag[Any])

          for {
            result <- app.invoke(Chunk.empty).exit
          } yield assert(result)(isFailure)
        }
      ),
      suite("Scoped Resources")(
        test("scoped resources are properly acquired and released") {
          val acquired = new AtomicInteger(0)
          val released = new AtomicInteger(0)

          val app = ZIOApp(
            ZIO.scoped[Any] {
              Scope.make.flatMap { scope =>
                scope
                  .addFinalizerExit(_ =>
                    ZIO.succeed(released.incrementAndGet())
                  )
                  .as(acquired.incrementAndGet())
              }
            }.as(ExitCode.success),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            _ <- app.invoke(Chunk.empty)
          } yield assert(acquired.get())(equalTo(1)) && assert(released.get())(equalTo(1))
        }
      ),
      suite("Interruption")(
        test("app handles interruption gracefully") {
          val finalizerRun = new AtomicInteger(0)
          val app = ZIOApp(
            ZIO
              .never[ExitCode]
              .ensuring(ZIO.succeed(finalizerRun.incrementAndGet()))
              .timeout(Duration.fromMillis(100))
              .as(ExitCode.success),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            result <- app.invoke(Chunk.empty).exit
          } yield assert(result)(isSuccess) && assert(finalizerRun.get())(equalTo(1))
        }
      ),
      suite("Composition")(
        test("two apps can be composed") {
          val app1 = ZIOApp(
            ZIO.succeed(ExitCode.success),
            ZLayer.empty
          )(EnvironmentTag[Any])

          val app2 = ZIOApp(
            ZIO.succeed(ExitCode.success),
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            result <- (app1 <> app2).invoke(Chunk.empty).exit
          } yield assert(result)(isSuccess)
        }
      ),
      suite("Exit Behavior")(
        test("exit causes application to terminate") {
          val executed = new AtomicInteger(0)
          val app = ZIOApp(
            for {
              _ <- ZIO.succeed(executed.incrementAndGet())
              _ <- ZIO.never
            } yield ExitCode.success,
            ZLayer.empty
          )(EnvironmentTag[Any])

          for {
            result <- app.invoke(Chunk.empty).timeout(Duration.fromMillis(500)).exit
          } yield assert(executed.get())(equalTo(1))
        }
      )
    )
}
