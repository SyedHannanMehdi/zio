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
 * Tests that error-handling operators do not accidentally recover from defects
 * when the [[Cause]] contains both a failure and a defect (issue #9874).
 *
 * Defects (and interruptions) must always be prioritised over typed failures:
 * handling the failure must not swallow the defect.
 */
object ZIODefectSpec extends ZIOBaseSpec {

  val boom: RuntimeException = new RuntimeException("boom")

  def spec = suite("ZIODefectSpec")(
    suite("catchAll does not swallow defects")(
      test("Both(Die, Fail) – defect is propagated after catchAll handles the failure") {
        val dieCause: Cause[String]  = Cause.die(boom)
        val combinedCause            = dieCause && Cause.fail("boom")
        val effect: IO[String, Unit] = ZIO.failCause(combinedCause)

        for {
          exit <- effect.catchAll(_ => ZIO.unit).exit
        } yield assert(exit)(dies(equalTo(boom)))
      },
      test("Then(Die, Fail) – defect is propagated after catchAll handles the failure") {
        val combinedCause = Cause.die(boom) ++ Cause.fail("boom")
        val effect        = ZIO.failCause(combinedCause)

        for {
          exit <- effect.catchAll(_ => ZIO.unit).exit
        } yield assert(exit)(dies(equalTo(boom)))
      },
      test("pure Fail (no defect) – catchAll still works normally") {
        val effect: IO[String, Int] = ZIO.fail("oops")

        for {
          result <- effect.catchAll(_ => ZIO.succeed(42))
        } yield assert(result)(equalTo(42))
      },
      test("pure Die (no failure) – catchAll does not intercept the defect") {
        val effect: IO[String, Unit] = ZIO.die(boom)

        for {
          exit <- effect.catchAll(_ => ZIO.unit).exit
        } yield assert(exit)(dies(equalTo(boom)))
      }
    ),
    suite("catchSome does not swallow defects")(
      test("Both(Die, Fail) – defect is propagated after catchSome handles the failure") {
        val dieCause: Cause[String]  = Cause.die(boom)
        val combinedCause            = dieCause && Cause.fail("boom")
        val effect: IO[String, Unit] = ZIO.failCause(combinedCause)

        for {
          exit <- effect.catchSome { case _ => ZIO.unit }.exit
        } yield assert(exit)(dies(equalTo(boom)))
      }
    ),
    suite("mapError does not swallow defects")(
      test("Both(Die, Fail) – defect is propagated after mapError transforms the failure") {
        val dieCause: Cause[String]  = Cause.die(boom)
        val combinedCause            = dieCause && Cause.fail("boom")
        val effect: IO[String, Unit] = ZIO.failCause(combinedCause)

        for {
          exit <- effect.mapError(identity).exit
        } yield assert(exit)(dies(equalTo(boom)))
      }
    ),
    suite("orElse does not swallow defects")(
      test("Both(Die, Fail) – defect is propagated even when orElse fallback succeeds") {
        val dieCause: Cause[String]  = Cause.die(boom)
        val combinedCause            = dieCause && Cause.fail("boom")
        val effect: IO[String, Unit] = ZIO.failCause(combinedCause)

        for {
          exit <- (effect orElse ZIO.unit).exit
        } yield assert(exit)(dies(equalTo(boom)))
      }
    )
  )
}
