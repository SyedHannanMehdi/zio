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
 * Regression tests for https://github.com/zio/zio/issues/9874
 *
 * When a Cause contains both a failure and a defect (Fail & Die), failure
 * handling operators (catchAll, mapError, orElse, etc.) must NOT silently
 * swallow the defect. Defects and interruptions are non-recoverable by
 * failure handlers, and they must be re-raised after the failure is handled.
 */
object ZIOCatchAllDefectSpec extends ZIOBaseSpec {

  val defect: Throwable = new RuntimeException("boom defect")
  val failure: String   = "boom failure"

  val dieCause: Cause[String]      = Cause.die(defect)
  val failCause: Cause[String]     = Cause.fail(failure)
  val combinedCause: Cause[String] = dieCause && failCause

  def spec = suite("ZIOCatchAllDefectSpec")(
    suite("catchAll with combined Fail+Die cause")(
      test("should re-raise the defect even after handling the failure") {
        for {
          exit <- ZIO
                    .failCause(combinedCause)
                    .catchAll(_ => ZIO.unit)
                    .exit
        } yield assert(exit)(fails(anything)) &&
          assert(exit)(fails(hasCause(isSubtype[Cause[Nothing]](anything)))) &&
          assert(exit.causeOption.map(_.defects))(isSome(contains(defect)))
      },
      test("should not succeed when cause contains a defect alongside a failure") {
        for {
          exit <- ZIO
                    .failCause(combinedCause)
                    .catchAll(_ => ZIO.unit)
                    .exit
        } yield assert(exit)(not(succeeds(anything)))
      },
      test("defect-only cause is still a defect after catchAll") {
        for {
          exit <- ZIO
                    .failCause(dieCause)
                    .catchAll((_: String) => ZIO.unit)
                    .exit
        } yield assert(exit.causeOption.map(_.defects))(isSome(contains(defect)))
      },
      test("failure-only cause is fully handled by catchAll") {
        for {
          result <- ZIO
                      .failCause(failCause)
                      .catchAll(_ => ZIO.succeed(42))
        } yield assert(result)(equalTo(42))
      }
    ),
    suite("mapError with combined Fail+Die cause")(
      test("should re-raise the defect when mapping the error") {
        for {
          exit <- ZIO
                    .failCause(combinedCause)
                    .mapError(identity)
                    .exit
        } yield assert(exit.causeOption.map(_.defects))(isSome(contains(defect)))
      }
    ),
    suite("orElse with combined Fail+Die cause")(
      test("should re-raise the defect when falling back") {
        for {
          exit <- ZIO
                    .failCause(combinedCause)
                    .orElse(ZIO.unit)
                    .exit
        } yield assert(exit.causeOption.map(_.defects))(isSome(contains(defect)))
      }
    ),
    suite("catchSome with combined Fail+Die cause")(
      test("should re-raise the defect even when the failure is caught") {
        for {
          exit <- ZIO
                    .failCause(combinedCause)
                    .catchSome { case _ => ZIO.unit }
                    .exit
        } yield assert(exit.causeOption.map(_.defects))(isSome(contains(defect)))
      }
    ),
    suite("foldZIO with combined Fail+Die cause")(
      test("should re-raise the defect after the error handler runs") {
        for {
          exit <- ZIO
                    .failCause(combinedCause)
                    .foldZIO(_ => ZIO.unit, _ => ZIO.unit)
                    .exit
        } yield assert(exit.causeOption.map(_.defects))(isSome(contains(defect)))
      }
    )
  )
}
