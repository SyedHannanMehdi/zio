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

/**
 * This file documents the intended fix for GitHub issue #9874.
 *
 * The issue: when a Cause contains both a Fail and a Die (or Interrupt),
 * failure-handling operators like catchAll silently ignore the defect/interrupt
 * and treat the effect as if it succeeded.
 *
 * Root cause: `catchAll` is implemented roughly as:
 *   {{{
 *     self.foldCauseZIO(
 *       cause => cause.failureOrCause match {
 *         case Left(e)  => h(e)
 *         case Right(c) => ZIO.failCause(c)
 *       },
 *       ZIO.successFn
 *     )
 *   }}}
 *
 * When `cause = Cause.Both(Cause.Die(t), Cause.Fail(e))`:
 * - `failureOrCause` returns `Left(e)` (finds the first Fail)
 * - The handler `h(e)` is invoked and returns a success
 * - The Die(t) part is silently discarded
 *
 * The fix: after handling the failure, check whether the original cause also
 * contained defects or interrupts, and if so, re-raise them.
 *
 * The corrected implementation for `catchAll` is:
 *
 * {{{
 *   self.foldCauseZIO(
 *     cause =>
 *       cause.failureOrCause match {
 *         case Left(e) =>
 *           cause.keepDefectsAndInterrupts match {
 *             case None =>
 *               // No defects or interrupts — handle the failure normally
 *               h(e)
 *             case Some(remainder) =>
 *               // There are defects/interrupts — run the handler but re-raise
 *               h(e).foldCauseZIO(
 *                 newCause => ZIO.failCause(newCause && remainder),
 *                 _        => ZIO.failCause(remainder)
 *               )
 *           }
 *         case Right(c) => ZIO.failCause(c)
 *       },
 *     ZIO.successFn
 *   )
 * }}}
 *
 * Where `keepDefectsAndInterrupts` is equivalent to `stripFailures` when the
 * result is non-empty, i.e. it keeps all `Die` and `Interrupt` nodes but
 * removes `Fail` nodes. This ensures that defects and interruptions are never
 * silently swallowed by failure handlers.
 *
 * The same pattern applies to:
 *   - `mapError`     (uses `catchAll` internally)
 *   - `orElse`       (uses `catchAll` internally)
 *   - `catchSome`    (uses `catchAll` internally)
 *   - `foldZIO`      (the failure branch of `foldCauseZIO`)
 *   - `fold`         (the failure branch)
 *
 * See: https://github.com/zio/zio/issues/9874
 */
private[zio] object CatchAllDefectFix
