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
 * Tests for the fix to issue #9874: Handling errors allows recovering from defects.
 *
 * When a Cause contains both a failure (Fail) and a defect (Die), failure
 * handlers like catchAll should NOT silently swallow the defect. Defects
 * must always be propagated, as they represent unrecoverable errors.
 */
object CatchAllDefectSpec extends ZIOBaseSpec {

  def spec = suite("CatchAllDefectSpec")(
    suite("catchAll with combined Fail+Die cause")(
      test("catchAll should not swallow a defect when cause contains both Fail and Die") {
        val boom         = new RuntimeException("boom")
        val dieCause     = Cause.die(boom)
        val combinedCause = dieCause && Cause.fail("fail")

        for {
          exit <- ZIO.failCause(combinedCause).catchAll(_ => ZIO.unit).exit
        } yield assert(exit)(fails(anything)) &&
          assert(exit)(failsCause(containsCause(Cause.die(boom))))
      },
      test("catchAll should not swallow an interruption when cause contains both Fail and Interrupt") {
        for {
          fiberId <- ZIO.fiberId
          intCause     = Cause.interrupt(fiberId)
          combinedCause = intCause && Cause.fail("fail")
          exit <- ZIO.failCause(combinedCause).catchAll(_ => ZIO.unit).exit
        } yield assert(exit)(failsCause(containsCause(Cause.interrupt(fiberId))))
      },
      test("catchAll should handle pure failure normally (no defects)") {
        for {
          result <- ZIO.fail("boom").catchAll(_ => ZIO.succeed(42))
        } yield assert(result)(equalTo(42))
      },
      test("catchSome should not swallow a defect when cause contains both Fail and Die") {
        val boom          = new RuntimeException("boom")
        val dieCause      = Cause.die(boom)
        val combinedCause = dieCause && Cause.fail("fail")

        for {
          exit <- ZIO.failCause(combinedCause).catchSome { case _ => ZIO.unit }.exit
        } yield assert(exit)(fails(anything)) &&
          assert(exit)(failsCause(containsCause(Cause.die(boom))))
      },
      test("mapError should not swallow a defect when cause contains both Fail and Die") {
        val boom          = new RuntimeException("boom")
        val dieCause      = Cause.die(boom)
        val combinedCause = dieCause && Cause.fail("fail")

        for {
          exit <- ZIO.failCause(combinedCause).mapError(_ => "mapped").exit
        } yield assert(exit)(failsCause(containsCause(Cause.die(boom))))
      },
      test("foldZIO should not swallow a defect when cause has both Fail and Die") {
        val boom          = new RuntimeException("boom")
        val dieCause      = Cause.die(boom)
        val combinedCause = dieCause && Cause.fail("fail")

        for {
          exit <- ZIO.failCause(combinedCause).foldZIO(_ => ZIO.unit, _ => ZIO.unit).exit
        } yield assert(exit)(fails(anything)) &&
          assert(exit)(failsCause(containsCause(Cause.die(boom))))
      },
      test("pure die cause still propagates without catchAll") {
        val boom = new RuntimeException("boom")
        for {
          exit <- ZIO.die(boom).exit
        } yield assert(exit)(failsCause(containsCause(Cause.die(boom))))
      },
      test("combined cause defects are re-raised after handling failure") {
        val boom          = new RuntimeException("defect")
        val dieCause      = Cause.die(boom)
        val combinedCause = dieCause && Cause.fail("fail")
        val handledRef    = new java.util.concurrent.atomic.AtomicBoolean(false)

        for {
          exit <- ZIO
                    .failCause(combinedCause)
                    .catchAll { _ =>
                      ZIO.succeed(handledRef.set(true))
                    }
                    .exit
        } yield assert(exit)(failsCause(containsCause(Cause.die(boom)))) &&
          assert(handledRef.get())(isTrue)
      }
    ),
    suite("Cause.keepDefects")(
      test("keepDefects returns None for pure Fail") {
        val cause = Cause.fail("boom")
        assert(cause.keepDefects)(isNone)
      },
      test("keepDefects returns Some for Die") {
        val boom  = new RuntimeException("boom")
        val cause = Cause.die(boom)
        assert(cause.keepDefects)(isSome(equalTo(Cause.die(boom))))
      },
      test("keepDefects returns Some for Both(Die, Fail)") {
        val boom  = new RuntimeException("boom")
        val cause = Cause.die(boom) && Cause.fail("fail")
        assert(cause.keepDefects)(isSome(equalTo(Cause.die(boom))))
      },
      test("keepDefects returns Some for Both(Fail, Die)") {
        val boom  = new RuntimeException("boom")
        val cause = Cause.fail("fail") && Cause.die(boom)
        assert(cause.keepDefects)(isSome(equalTo(Cause.die(boom))))
      }
    )
  )
}
