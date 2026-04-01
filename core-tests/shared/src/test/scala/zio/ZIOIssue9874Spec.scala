/*
 * Copyright 2017-2024 John A. De Goes and the ZIO Contributors
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

import zio.test.Assertion._
import zio.test._

/**
 * Tests for issue #9874: Handling errors allows recovering from defects.
 *
 * When a Cause contains both a failure and a defect (i.e., Fail & Die),
 * failure handling should NOT silently ignore the defects. Defects and
 * interruptions must always be prioritised over failures.
 */
object ZIOIssue9874Spec extends ZIOBaseSpec {

  val boom: RuntimeException = new RuntimeException("boom")

  def spec = suite("ZIOIssue9874Spec")(
    suite("catchAll should not recover from defects")(
      test("catchAll does not swallow defect when cause has both fail and die") {
        val dieCause: Cause[String]  = Cause.die(boom)
        val combinedCause            = dieCause && Cause.fail("oops")
        val effect: ZIO[Any, String, Int] =
          ZIO.failCause(combinedCause).catchAll(_ => ZIO.succeed(42))
        for {
          exit <- effect.exit
        } yield assert(exit)(fails(anything)) &&
          assert(exit.causeOption.map(_.defects))(isSome(equalTo(List(boom))))
      },
      test("catchAll does not swallow interruption when cause has both fail and interrupt") {
        for {
          fiberId <- ZIO.fiberId
          interruptCause: Cause[String] = Cause.interrupt(fiberId)
          combinedCause                 = interruptCause && Cause.fail("oops")
          exit <- ZIO.failCause(combinedCause).catchAll(_ => ZIO.succeed(42)).exit
        } yield assert(exit.isInterrupted)(isTrue)
      },
      test("catchAll works normally when cause has only failures") {
        val effect: ZIO[Any, String, Int] =
          ZIO.fail("oops").catchAll(_ => ZIO.succeed(42))
        assertZIO(effect)(equalTo(42))
      },
      test("catchAll works normally when cause has only a defect") {
        val effect: ZIO[Any, String, Int] =
          ZIO.die(boom).catchAll(_ => ZIO.succeed(42))
        for {
          exit <- effect.exit
        } yield assert(exit)(fails(anything)) &&
          assert(exit.causeOption.map(_.defects))(isSome(equalTo(List(boom))))
      }
    ),
    suite("catchSome should not recover from defects")(
      test("catchSome does not swallow defect when cause has both fail and die") {
        val dieCause: Cause[String]  = Cause.die(boom)
        val combinedCause            = dieCause && Cause.fail("oops")
        val effect: ZIO[Any, String, Int] =
          ZIO.failCause(combinedCause).catchSome { case "oops" => ZIO.succeed(42) }
        for {
          exit <- effect.exit
        } yield assert(exit)(fails(anything)) &&
          assert(exit.causeOption.map(_.defects))(isSome(equalTo(List(boom))))
      }
    ),
    suite("foldZIO should not recover from defects when handling errors")(
      test("foldZIO error handler does not swallow defect") {
        val dieCause: Cause[String]  = Cause.die(boom)
        val combinedCause            = dieCause && Cause.fail("oops")
        val effect: ZIO[Any, String, Int] =
          ZIO.failCause(combinedCause).foldZIO(_ => ZIO.succeed(42), ZIO.successFn)
        for {
          exit <- effect.exit
        } yield assert(exit)(fails(anything)) &&
          assert(exit.causeOption.map(_.defects))(isSome(equalTo(List(boom))))
      }
    )
  )
}
