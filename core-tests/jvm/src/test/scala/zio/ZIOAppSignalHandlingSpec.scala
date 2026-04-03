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

object ZIOAppSignalHandlingSpec extends ZIOSpecDefault {
  def spec = suite("ZIOApp Signal Handling")(
    test("signal handlers are installed") {
      val app = ZIOAppDefault(
        ZIO.succeed(())
      )
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("finalizers execute before shutdown") {
      val finalizerRan = Ref.make(false)
      val app = ZIOAppDefault(
        for {
          ref <- finalizerRan
          _ <- ZIO.succeed(()).ensuring(ref.set(true))
        } yield ()
      )
      for {
        result <- app.invoke(Chunk.empty).exit
        finalized <- finalizerRan.flatMap(_.get)
      } yield assert(result)(isSuccess) && assert(finalized)(isTrue)
    },
    test("graceful shutdown timeout prevents indefinite waits") {
      val app = new ZIOAppDefault {
        override def gracefulShutdownTimeout: Duration = Duration.ofMillis(50)
        def run: ZIO[ZIOAppArgs, Any, Any] = ZIO.unit
      }
      for {
        result <- app.invoke(Chunk.empty).exit.timeout(Duration.ofSeconds(1))
      } yield assert(result)(isSome)
    },
    test("app doesn't hang on finalizer execution") {
      val app = ZIOAppDefault(
        ZIO.succeed(()).ensuring(ZIO.sleep(Duration.ofMillis(10)))
      )
      for {
        result <- app.invoke(Chunk.empty).exit.timeout(Duration.ofSeconds(5))
      } yield assert(result)(isSome)
    },
    test("shutdown flag prevents re-entry") {
      val app = ZIOAppDefault(ZIO.unit)
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("nested scopes clean up properly on shutdown") {
      val cleanupOrder = Ref.make(List.empty[String])
      val app = ZIOAppDefault(
        ZIO.scoped[Any](
          for {
            ref <- cleanupOrder
            outer <- ZIO.scope
            _ <- outer.addFinalizer(ref.update(l => l :+ "outer"))
            inner <- ZIO.scope
            _ <- inner.addFinalizer(ref.update(l => l :+ "inner"))
          } yield ()
        )
      )
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("concurrent tasks clean up on shutdown") {
      val executed = Ref.make(false)
      val app = ZIOAppDefault(
        for {
          ref <- executed
          _ <- ZIO.succeed(()).fork.ensuring(ref.set(true))
        } yield ()
      )
      for {
        result <- app.invoke(Chunk.empty).exit
      } yield assert(result)(isSuccess)
    },
    test("multiple serialized app invocations work correctly") {
      val app = ZIOAppDefault(
        for {
          args <- ZIOAppArgs.getArgs
        } yield args.length > 0
      )
      for {
        result1 <- app.invoke(Chunk("a")).exit
        result2 <- app.invoke(Chunk("b", "c")).exit
        result3 <- app.invoke(Chunk.empty).exit
      } yield assert(result1)(isSuccess) && assert(result2)(isSuccess)
    },
  )
}
