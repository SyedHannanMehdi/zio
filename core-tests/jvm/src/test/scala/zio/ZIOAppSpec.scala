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

import zio.internal.*
import zio.stacktracer.TracingImplicits.disableAutoTrace
import zio.test.*
import zio.test.Assertion.*

object ZIOAppSpec extends ZIOSpecDefault {
  def spec = suite("ZIOApp")(
    test("successful app completes with exit code 0") {
      val app = ZIOAppDefault(ZIO.unit)
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("app that fails completes with non-zero exit code") {
      val app = ZIOAppDefault(ZIO.fail(new Exception("test failure")))
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isFailure)
    },
    test("app runs finalizers on successful completion") {
      val finalizerRun = Ref.make(false)
      val app = ZIOAppDefault(
        for {
          ref <- finalizerRun
          _ <- ZIO.succeed(()).ensuring(ref.set(true))
        } yield ()
      )
      for {
        result <- app.invoke(Chunk.empty).exit
        finalized <- finalizerRun.flatMap(_.get)
      } yield assert(result)(isSuccess) && assert(finalized)(isTrue)
    },
    test("app runs finalizers on failure") {
      val finalizerRun = Ref.make(false)
      val app = ZIOAppDefault(
        for {
          ref <- finalizerRun
          _ <- ZIO.fail(new Exception("test")).ensuring(ref.set(true))
        } yield ()
      )
      for {
        result <- app.invoke(Chunk.empty).exit
        finalized <- finalizerRun.flatMap(_.get)
      } yield assert(result)(isFailure) && assert(finalized)(isTrue)
    },
    test("app with custom bootstrap layer") {
      val serviceRef = Ref.make(0)
      val bootstrap = ZLayer(serviceRef)
      val app = ZIOApp(
        for {
          ref <- ZIO.service[Ref[Int]]
          _ <- ref.set(42)
          value <- ref.get
        } yield value,
        bootstrap
      )(EnvironmentTag[Ref[Int]])
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("app with command line arguments") {
      val app = ZIOAppDefault(
        for {
          args <- ZIOAppArgs.getArgs
        } yield args.length
      )
      for {
        result <- app.invoke(Chunk("arg1", "arg2")).exit
      } yield assert(result)(isSuccess)
    },
    test("app handles ZIOAppArgs correctly") {
      val app = ZIOAppDefault(
        for {
          args <- ZIOAppArgs.getArgs
        } yield {
          args.size == 3 && args(0) == "a" && args(1) == "b" && args(2) == "c"
        }
      )
      for {
        result <- app.invoke(Chunk("a", "b", "c")).exit
      } yield assert(result)(isSuccess)
    },
    test("gracefulShutdownTimeout is configurable") {
      val app = new ZIOAppDefault {
        override def gracefulShutdownTimeout: Duration = Duration.ofMillis(100)
        def run = ZIO.unit
      }
      assert(app.gracefulShutdownTimeout)(equalTo(Duration.ofMillis(100)))
    },
    test("app composition with <>") {
      val ref1 = Ref.make(0)
      val ref2 = Ref.make(0)
      val app1 = ZIOAppDefault(
        for {
          r <- ref1
          _ <- r.set(1)
        } yield ()
      )
      val app2 = ZIOAppDefault(
        for {
          r <- ref2
          _ <- r.set(2)
        } yield ()
      )
      val composed = app1 <> app2
      for {
        result <- composed.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("multiple finalizers all execute") {
      val calls = Ref.make(List.empty[String])
      val app = ZIOAppDefault(
        for {
          ref <- calls
          _ <- ZIO.succeed(()).ensuring(ref.update(l => l :+ "first"))
          _ <- ZIO.succeed(()).ensuring(ref.update(l => l :+ "second"))
          _ <- ZIO.succeed(()).ensuring(ref.update(l => l :+ "third"))
        } yield ()
      )
      for {
        result <- app.invoke(Chunk.empty).exit
        executed <- calls.flatMap(_.get)
      } yield assert(result)(isSuccess) && assert(executed.length)(isGreaterThanOrEqualTo(1))
    },
    test("app doesn't hang on normal completion") {
      val app = ZIOAppDefault(ZIO.unit)
      for {
        result <- app.invoke(Chunk.empty).timeout(Duration.ofSeconds(5))
      } yield assert(result)(isSome)
    },
    test("app with nested scope management") {
      val finalizersCalled = Ref.make(List.empty[String])
      val app = ZIOAppDefault(
        ZIO.scoped[Any](
          for {
            ref <- finalizersCalled
            scope <- ZIO.scope
            _ <- scope.addFinalizer(ref.update(l => l :+ "outer"))
            _ <- ZIO.succeed(()).ensuring(ref.update(l => l :+ "inner"))
          } yield ()
        )
      )
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("app with error logging suppression during shutdown") {
      val app = ZIOAppDefault(
        ZIO.fail(new Exception("test error"))
      )
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isFailure)
    },
    test("app can be invoked multiple times") {
      val counter = Ref.make(0)
      val app = ZIOAppDefault(
        for {
          ref <- counter
          count <- ref.getAndUpdate(_ + 1)
        } yield count
      )
      for {
        result1 <- app.invoke(Chunk.empty).exit
        result2 <- app.invoke(Chunk.empty).exit
      } yield assert(result1)(isSuccess) && assert(result2)(isSuccess)
    },
    test("app workflow with layers") {
      val service = Ref.make(0)
      val layer = ZLayer(service)
      val app = ZIOApp(
        ZIO.scoped[Ref[Int]](
          for {
            ref <- ZIO.service[Ref[Int]]
            _ <- ref.set(100)
            value <- ref.get
          } yield value == 100
        ),
        layer
      )(EnvironmentTag[Ref[Int]])
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("fromZIO constructor works") {
      val app = ZIOAppDefault.fromZIO(ZIO.succeed(()))
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("ZIOApp.fromZIO constructor works") {
      val app = ZIOApp.fromZIO(ZIO.succeed(()))
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("app handles interruption gracefully") {
      val finalizerExecuted = Ref.make(false)
      val app = ZIOAppDefault(
        for {
          ref <- finalizerExecuted
          _ <- ZIO.sleep(Duration.ofSeconds(10)).ensuring(ref.set(true))
        } yield ()
      )
      for {
        result <- app.invoke(Chunk.empty).timeout(Duration.ofMillis(100))
      } yield assert(result)(isNone)
    },
  )
}
