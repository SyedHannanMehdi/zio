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

import java.util.concurrent.atomic.AtomicBoolean

object ZIOAppEdgeCasesSpec extends ZIOSpecDefault {
  def spec = suite("ZIOAppEdgeCasesSpec")(
    test("app with no-op run") {
      val app = ZIOApp.fromZIO(ZIO.unit)

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess)
      }
    },
    test("app with pure value") {
      val app = ZIOApp.fromZIO(ZIO.succeed(42))

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess(equalTo(42)))
      }
    },
    test("app with defect") {
      val app = ZIOApp.fromZIO(
        ZIO.succeed(throw new RuntimeException("defect"))
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isFailure)
      }
    },
    test("app with fiber death") {
      val app = ZIOApp.fromZIO(
        for {
          fiber <- ZIO.fail(new Exception("fiber failure")).fork
          _     <- fiber.join
        } yield ()
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isFailure)
      }
    },
    test("app with successful fork") {
      val app = ZIOApp.fromZIO(
        for {
          fiber <- ZIO.succeed(()).fork
          _     <- fiber.join
        } yield ()
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess)
      }
    },
    test("app with zero arguments") {
      val app = ZIOApp.fromZIO(
        for {
          args <- ZIOAppArgs.getArgs
        } yield args.length
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess(equalTo(0)))
      }
    },
    test("app with many arguments") {
      val manyArgs = Chunk.fromIterable((0 until 1000).map(_.toString))
      val app = ZIOApp.fromZIO(
        for {
          args <- ZIOAppArgs.getArgs
        } yield args.length
      )

      for {
        result <- app.invoke(manyArgs).exit
      } yield {
        assert(result)(isSuccess(equalTo(1000)))
      }
    },
    test("app with special characters in arguments") {
      val specialArgs = Chunk("arg with spaces", "arg\twith\ttabs", "arg\nwith\nnewlines")
      val app = ZIOApp.fromZIO(
        for {
          args <- ZIOAppArgs.getArgs
        } yield args
      )

      for {
        result <- app.invoke(specialArgs).exit
      } yield {
        assert(result)(
          isSuccess(
            equalTo(specialArgs)
          )
        )
      }
    },
    test("finalizer with side effects") {
      val sideEffectTracker = scala.collection.mutable.ArrayBuffer[String]()

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          for {
            _ <- ZIO.acquireRelease(
              ZIO.succeed(sideEffectTracker.append("acquire"))
            )(_ =>
              ZIO.succeed(sideEffectTracker.append("release"))
            )
          } yield ()
        )
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(sideEffectTracker.toSeq)(
          equalTo(Seq("acquire", "release"))
        )
      }
    },
    test("multiple sequential invocations") {
      val app = ZIOApp.fromZIO(ZIO.succeed(()))

      for {
        result1 <- app.invoke(Chunk.empty).exit
        result2 <- app.invoke(Chunk.empty).exit
        result3 <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result1)(isSuccess) &&
        assert(result2)(isSuccess) &&
        assert(result3)(isSuccess)
      }
    },
    test("app with complex error chain") {
      val app = ZIOApp.fromZIO(
        ZIO.fail(new Exception("root cause")).flatMap(_ => ZIO.fail(new Exception("wrapped")))
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isFailure)
      }
    },
    test("app that acquires and releases resources") {
      val acquired = new AtomicBoolean(false)
      val released = new AtomicBoolean(false)

      val app = ZIOApp.fromZIO(
        ZIO.scoped(
          ZIO.acquireRelease(
            ZIO.succeed {
              acquired.set(true)
            }
          )(_ =>
            ZIO.succeed {
              released.set(true)
            }
          ) *>
            ZIO.unit
        )
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(acquired.get())(isTrue) &&
        assert(released.get())(isTrue)
      }
    },
    test("app with deferred completion") {
      val promise = Promise.make[Nothing, String]
      val app = ZIOApp.fromZIO(
        for {
          fiber <- promise.await.fork
          _     <- ZIO.succeed(())
          _     <- fiber.interrupt
        } yield ()
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess)
      }
    },
    test("app that logs during execution") {
      val app = ZIOApp.fromZIO(
        for {
          _ <- ZIO.logInfo("starting")
          _ <- ZIO.unit
          _ <- ZIO.logInfo("done")
        } yield ()
      )

      for {
        result <- app.invoke(Chunk.empty).exit
      } yield {
        assert(result)(isSuccess)
      }
    }
  )
}
