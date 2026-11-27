from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Thread
from unittest import TestCase

from demo import run
from placement import Booking, Demand, Lease, Permit, Pool, Worker


class PlacementTest(TestCase):
    def test_label_and_memory_change_actual_binding(self):
        self.assertEqual(run(), (None, "large", "small", "local-model-v1",
                                 {"small": (1, 2), "large": (3, 6)}))

    def test_release_of_previous_or_forged_lease_cannot_free_current_job(self):
        pool = Pool([Worker("one", 2, 2)])
        old = pool.reserve("job", Demand(1, 1))
        self.assertTrue(pool.release(old))
        current = pool.reserve("job", Demand(2, 2))
        self.assertFalse(pool.release(old))
        self.assertFalse(pool.release(Lease("job", "one")))
        self.assertEqual(pool.available(), {"one": (0, 0)})
        self.assertTrue(pool.release(current))
        self.assertEqual(pool.available(), {"one": (2, 2)})

    def test_competing_jobs_cannot_overbook_last_unit(self):
        pool = Pool([Worker("one", 1, 1)])
        gate = Barrier(2)

        def place(job):
            gate.wait(5)
            return pool.reserve(job, Demand(1, 1))

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(place, ["left", "right"]))
        self.assertEqual(sum(result is not None for result in results), 1)
        self.assertEqual(pool.available(), {"one": (0, 0)})

    def test_failed_rejection_and_duplicate_leave_reservation_unchanged(self):
        pool = Pool([Worker("b", 2, 4), Worker("a", 2, 4)])
        first = pool.reserve("first", Demand(1, 1))
        self.assertEqual(first.worker, "a")
        before = pool.available()
        self.assertIsNone(pool.reserve("blocked", Demand(1, 9)))
        with self.assertRaises(ValueError):
            pool.reserve("first", Demand(1, 2))
        self.assertEqual(pool.available(), before)


def _scenario_one_pool():
    """Two jobs on a source; only one target carries the accel label.

    The source also carries accel (source-only stays unique to it) so that the
    existing reserve ordering places both jobs on the source initially.
    """
    pool = Pool([
        Worker("src", 3, 5, frozenset({"source-only", "accel"})),
        Worker("t-accel", 4, 3, frozenset({"accel"})),
        Worker("t-plain", 5, 3, frozenset({"plain-only"})),
    ])
    plain_lease = pool.reserve("plain-job", Demand(1, 2))
    accel_lease = pool.reserve("accel-job", Demand(1, 2, frozenset({"accel"})))
    assert plain_lease.worker == "src" and accel_lease.worker == "src"
    return pool, plain_lease, accel_lease


class DrainTest(TestCase):
    def test_drain_finds_complete_plan_and_publishes_fenced_migrations(self):
        pool, plain_lease, accel_lease = _scenario_one_pool()
        calls = []

        def prepare(old_lease, target):
            calls.append((old_lease, target))
            return True

        result = pool.drain("src", prepare)
        self.assertEqual(result["status"], "drained")
        self.assertEqual(len(result["moved"]), 2)
        self.assertEqual(len(calls), 2)
        # Greedy per-job placement would put the plain job on the small target;
        # only backtracking leaves that target for the labelled job.
        plain_new = result["moved"]["plain-job"]
        accel_new = result["moved"]["accel-job"]
        self.assertEqual(plain_new.worker, "t-plain")
        self.assertEqual(accel_new.worker, "t-accel")
        self.assertEqual(pool.current("plain-job"), plain_new)
        self.assertEqual(pool.current("accel-job"), accel_new)
        # source returned to full capacity; targets carry the new occupancy
        self.assertEqual(pool.available(), {
            "src": (3, 5),
            "t-accel": (3, 1),
            "t-plain": (4, 1),
        })
        # Old lease objects cannot release the new occupancy.
        self.assertFalse(pool.release(plain_lease))
        self.assertFalse(pool.release(accel_lease))
        self.assertFalse(pool.release(Lease("plain-job", "t-plain")))
        self.assertEqual(pool.available()["t-plain"], (4, 1))
        # The published leases do release their own occupancy.
        self.assertTrue(pool.release(plain_new))
        self.assertTrue(pool.release(accel_new))
        self.assertEqual(pool.available(), {
            "src": (3, 5),
            "t-accel": (4, 3),
            "t-plain": (5, 3),
        })

    def test_no_capacity_changes_nothing_and_skips_callbacks_then_retry_succeeds(self):
        # Reproduce the scenario-1 bindings in a pool missing the second target.
        pool = Pool([
            Worker("src", 3, 5, frozenset({"source-only", "accel"})),
            Worker("t-accel", 4, 3, frozenset({"accel"})),
        ])
        plain_lease = pool.reserve("plain-job", Demand(1, 2))
        accel_lease = pool.reserve("accel-job", Demand(1, 2, frozenset({"accel"})))
        before = pool.available()
        called = Event()

        def prepare(*_args):
            called.set()
            return True

        result = pool.drain("src", prepare)
        self.assertEqual(result["status"], "no_capacity")
        self.assertEqual(result["moved"], {})
        self.assertFalse(called.is_set())
        # Bindings, objects and units are untouched.
        self.assertIs(pool.current("plain-job"), plain_lease)
        self.assertIs(pool.current("accel-job"), accel_lease)
        self.assertEqual(pool.available(), before)
        # The source is still closed to new jobs, even though t-accel can
        # serve ordinary demands: a source-only requirement cannot land there.
        self.assertIsNone(pool.reserve("source-only-job",
                                       Demand(1, 1, frozenset({"source-only"}))))
        # Remove one job; the remaining labelled job now has a complete plan.
        self.assertTrue(pool.release(accel_lease))
        result = pool.drain("src", prepare)
        self.assertEqual(result["status"], "drained")
        self.assertEqual(set(result["moved"]), {"plain-job"})
        self.assertEqual(result["moved"]["plain-job"].worker, "t-accel")
        self.assertEqual(pool.available(), {"src": (3, 5), "t-accel": (3, 1)})

    def test_blocked_prepare_after_first_publish_is_retryable_without_reprepare(self):
        # Jobs are processed in name order: accel-job is prepared and published
        # first; plain-job's callback is then blocked and eventually fails.
        pool, plain_lease, accel_lease = _scenario_one_pool()
        entered = Event()
        release_block = Event()
        order = []

        def prepare(old_lease, target):
            order.append(old_lease.job)
            if old_lease.job == "plain-job":
                entered.set()
                release_block.wait(5)
                return False
            return True

        thread_result = {}
        thread = Thread(target=lambda: thread_result.update(
            pool.drain("src", prepare)))
        thread.start()
        self.assertTrue(entered.wait(5))
        # The failed job still sees its original binding while the callback waits;
        # the earlier job is already on its new binding.
        self.assertIs(pool.current("plain-job"), plain_lease)
        self.assertEqual(pool.current("accel-job").worker, "t-accel")

        # The source rejects new ordinary jobs despite positive free units.
        self.assertGreater(pool.available()["src"][0], 0)
        self.assertIsNone(pool.reserve("source-only-job",
                                       Demand(1, 1, frozenset({"source-only"}))))

        # t-plain has 1 memory unit left after the batch hold (5,3) -> (4,1);
        # that held share cannot be handed out, while unheld units still can.
        self.assertIsNone(pool.reserve("too-big", Demand(1, 2,
                                                         frozenset({"plain-only"}))))
        tiny = pool.reserve("plain-tiny", Demand(1, 1,
                                                 frozenset({"plain-only"})))
        self.assertIsNotNone(tiny)
        self.assertEqual(tiny.worker, "t-plain")
        self.assertTrue(pool.release(tiny))

        # A second drain is busy and must not close another worker.
        busy = pool.drain("t-accel", lambda *_: True)
        self.assertEqual(busy, {"status": "busy", "moved": {}})
        # t-accel never closed: it can still accept a job it fits.
        small_on_accel = pool.reserve("small-accel",
                                      Demand(1, 1, frozenset({"accel"})))
        self.assertEqual(small_on_accel.worker, "t-accel")
        self.assertTrue(pool.release(small_on_accel))

        release_block.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(thread_result["status"], "prepare_failed")
        # Published migrations never roll back and stay recorded.
        self.assertEqual(set(thread_result["moved"]), {"accel-job"})
        accel_new = thread_result["moved"]["accel-job"]
        self.assertEqual(order, ["accel-job", "plain-job"])
        # The unpublished job keeps its original binding; its hold was returned.
        self.assertIs(pool.current("plain-job"), plain_lease)
        self.assertIs(pool.current("accel-job"), accel_new)
        self.assertEqual(pool.available(), {
            "src": (2, 3),
            "t-accel": (3, 1),
            "t-plain": (5, 3),
        })

        # Retry only handles the binding still on the source: accel-job is not
        # prepared again and keeps the exact same published lease object.
        retried = []

        def retry_prepare(old_lease, target):
            retried.append(old_lease.job)
            return True

        result = pool.drain("src", retry_prepare)
        self.assertEqual(result["status"], "drained")
        self.assertEqual(retried, ["plain-job"])
        self.assertEqual(result["moved"]["plain-job"].worker, "t-plain")
        self.assertIs(pool.current("accel-job"), accel_new)
        self.assertEqual(pool.available(), {
            "src": (3, 5),
            "t-accel": (3, 1),
            "t-plain": (4, 1),
        })

    def test_blocked_first_prepare_keeps_everything_unpublished_until_done(self):
        # Name the blocked job so it sorts first: nothing may be published
        # before its callback finishes, and a retry must re-prepare it.
        pool = Pool([
            Worker("src", 3, 5, frozenset({"source-only", "accel"})),
            Worker("t-accel", 4, 3, frozenset({"accel"})),
            Worker("t-plain", 5, 3),
        ])
        a_lease = pool.reserve("aaa-job", Demand(1, 2, frozenset({"accel"})))
        b_lease = pool.reserve("bbb-job", Demand(1, 2))
        self.assertEqual(a_lease.worker, "src")
        self.assertEqual(b_lease.worker, "src")
        entered = Event()
        release_block = Event()

        def prepare(old_lease, target):
            if old_lease.job == "aaa-job":
                entered.set()
                release_block.wait(5)
                return False
            return True

        thread_result = {}
        thread = Thread(target=lambda: thread_result.update(
            pool.drain("src", prepare)))
        thread.start()
        self.assertTrue(entered.wait(5))
        # Nothing published yet: both original leases remain current.
        self.assertIs(pool.current("aaa-job"), a_lease)
        self.assertIs(pool.current("bbb-job"), b_lease)
        # Both target holds are outstanding.
        self.assertEqual(pool.available(), {
            "src": (1, 1),
            "t-accel": (3, 1),
            "t-plain": (4, 1),
        })

        release_block.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(thread_result["status"], "prepare_failed")
        self.assertEqual(thread_result["moved"], {})
        self.assertIs(pool.current("aaa-job"), a_lease)
        self.assertIs(pool.current("bbb-job"), b_lease)
        # All holds returned.
        self.assertEqual(pool.available(), {
            "src": (1, 1),
            "t-accel": (4, 3),
            "t-plain": (5, 3),
        })

        # A fresh attempt re-prepares both jobs and completes the drain.
        retried = []
        result = pool.drain("src", lambda old, target: retried.append(old.job) or True)
        self.assertEqual(result["status"], "drained")
        self.assertEqual(sorted(retried), ["aaa-job", "bbb-job"])
        self.assertEqual(pool.current("aaa-job").worker, "t-accel")
        self.assertEqual(pool.current("bbb-job").worker, "t-plain")
        self.assertEqual(pool.available(), {
            "src": (3, 5),
            "t-accel": (3, 1),
            "t-plain": (4, 1),
        })

    def test_released_then_rebound_job_is_not_touched_by_late_callback(self):
        # A third plain target keeps the source out of every feasible plan, so
        # the already-published job is known to live outside the source.
        pool = Pool([
            Worker("src", 3, 5, frozenset({"source-only", "accel"})),
            Worker("t-accel", 4, 3, frozenset({"accel"})),
            Worker("t-plain", 5, 3, frozenset({"plain-only"})),
            Worker("t-spare", 6, 6),
        ])
        plain_lease = pool.reserve("plain-job", Demand(1, 2))
        accel_lease = pool.reserve("accel-job", Demand(1, 2, frozenset({"accel"})))
        self.assertEqual(plain_lease.worker, "src")
        self.assertEqual(accel_lease.worker, "src")
        entered = Event()
        release_block = Event()

        def prepare(old_lease, target):
            if old_lease.job == "accel-job":
                entered.set()
                release_block.wait(5)
                return True
            return True

        thread_result = {}
        thread = Thread(target=lambda: thread_result.update(
            pool.drain("src", prepare)))
        thread.start()
        self.assertTrue(entered.wait(5))
        # accel-job's callback runs first (name order) and is still waiting, so
        # nothing is published yet: plain-job is still on the source while its
        # target hold on t-plain is already outstanding.
        plain_new = pool.current("plain-job")
        self.assertIs(plain_new, plain_lease)
        self.assertEqual(pool.available(), {
            "src": (1, 1),
            "t-accel": (3, 1),
            "t-plain": (4, 1),
            "t-spare": (6, 6),
        })
        # Its held share cannot be re-given while the callback is still running.
        self.assertIsNone(pool.reserve("steal",
                                       Demand(1, 2, frozenset({"accel"}))))

        # Release the old binding, then bind the same job name elsewhere with
        # different requirements. The fresh demand must fit in the units the
        # stale attempt does not hold ((3,1) free on t-accel at this moment).
        self.assertTrue(pool.release(accel_lease))
        rebound = pool.reserve("accel-job", Demand(2, 1, frozenset({"accel"})))
        self.assertIsNotNone(rebound)
        self.assertEqual(rebound.worker, "t-accel")
        self.assertIsNot(rebound, accel_lease)
        # The fresh binding fits into the units not held by the stale attempt.
        self.assertEqual(pool.available()["t-accel"], (1, 0))

        release_block.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        # The stale accel-job result published nothing; plain-job, whose turn
        # came after the block was lifted, was published normally.
        self.assertEqual(thread_result["status"], "drained")
        self.assertEqual(set(thread_result["moved"]), {"plain-job"})
        plain_new = thread_result["moved"]["plain-job"]
        self.assertEqual(plain_new.worker, "t-plain")

        # The new binding identity and both resource deductions survive; after
        # the stale callback exits its hold is returned, so t-accel (4,3) minus
        # the fresh (2,1) occupancy reads (2,2), never a leaked extra unit.
        self.assertIs(pool.current("accel-job"), rebound)
        self.assertEqual(pool.available(), {
            "src": (3, 5),
            "t-accel": (2, 2),
            "t-plain": (4, 1),
            "t-spare": (6, 6),
        })
        # Neither the old lease nor a forged same-field lease frees the new job.
        self.assertFalse(pool.release(accel_lease))
        self.assertFalse(pool.release(Lease("accel-job", "t-accel")))
        self.assertIs(pool.current("accel-job"), rebound)
        self.assertTrue(pool.release(rebound))
        self.assertEqual(pool.available()["t-accel"], (4, 3))

        # The source is empty now: retrying returns drained with an empty moved
        # map and no callbacks.
        again_calls = []
        again = pool.drain("src", lambda old, target: again_calls.append(old.job) or True)
        self.assertEqual(again, {"status": "drained", "moved": {}})
        self.assertEqual(again_calls, [])
        self.assertTrue(pool.release(plain_new))
        self.assertEqual(pool.available()["t-plain"], (5, 3))

    def test_drain_validation_and_busy_rules_do_not_mutate_pool(self):
        pool = Pool([Worker("src", 3, 5), Worker("z-dst", 4, 3)])
        lease = pool.reserve("job", Demand(1, 1))
        before = pool.available()
        with self.assertRaises(ValueError):
            pool.drain("missing", lambda *_: True)
        with self.assertRaises(ValueError):
            pool.drain("src", None)
        with self.assertRaises(ValueError):
            pool.drain(123, lambda *_: True)
        self.assertEqual(pool.available(), before)
        self.assertIs(pool.current("job"), lease)

        # Oversized pool: drain is rejected even with zero source jobs.
        big = Pool([Worker(f"w{i}", 65, 1) for i in range(8)])
        with self.assertRaises(ValueError):
            big.drain("w0", lambda *_: True)
        many_workers = Pool([Worker(f"w{i}", 1, 1) for i in range(9)])
        with self.assertRaises(ValueError):
            many_workers.drain("w0", lambda *_: True)
        seven_jobs = Pool([Worker("src", 64, 64), Worker("z-dst", 64, 64)])
        leases = [seven_jobs.reserve(f"j{i}", Demand(1, 1)) for i in range(7)]
        with self.assertRaises(ValueError):
            seven_jobs.drain("src", lambda *_: True)
        # Rejected validation did not close the source.
        self.assertEqual(seven_jobs.current("j0"), leases[0])
        self.assertIsNotNone(seven_jobs.reserve("extra", Demand(1, 1)))

        # Empty source drains successfully without calling prepare.
        empty = Pool([Worker("src", 2, 2, frozenset({"only-src"})),
                      Worker("z-dst", 2, 2)])
        called = Event()
        result = empty.drain("src", lambda *_: called.set() or True)
        self.assertEqual(result, {"status": "drained", "moved": {}})
        self.assertFalse(called.is_set())
        # Even an empty attempt closes the source: only the source carries
        # this label, so the demand cannot be served anywhere.
        self.assertIsNone(empty.reserve("late",
                                        Demand(1, 1, frozenset({"only-src"}))))
        # The other worker keeps accepting ordinary demands.
        self.assertEqual(empty.reserve("other", Demand(1, 1)).worker, "z-dst")

    def test_closed_workers_are_excluded_from_targets_and_reserve(self):
        pool = Pool([Worker("a-src", 2, 2, frozenset({"k"})),
                     Worker("b-dst", 2, 2, frozenset({"k"}))])
        lease = pool.reserve("job", Demand(1, 1, frozenset({"k"})))
        self.assertEqual(lease.worker, "a-src")
        # Close b-dst via an empty drain; only a-src remains usable for new jobs.
        self.assertEqual(pool.drain("b-dst", lambda *_: True)["status"], "drained")
        next_lease = pool.reserve("job2", Demand(1, 1, frozenset({"k"})))
        self.assertEqual(next_lease.worker, "a-src")
        # Draining a-src cannot use the closed b-dst as a target.
        self.assertEqual(pool.drain("a-src", lambda *_: True)["status"],
                         "no_capacity")

    def test_current_tracks_binding_lifecycle(self):
        pool = Pool([Worker("w", 2, 2)])
        self.assertIsNone(pool.current("ghost"))
        lease = pool.reserve("job", Demand(1, 1))
        self.assertIs(pool.current("job"), lease)
        self.assertTrue(pool.release(lease))
        self.assertIsNone(pool.current("job"))

    def test_prepare_exception_and_non_true_both_fail_unpublished_jobs(self):
        for outcome in ("raise", "false", "truthy"):
            pool = Pool([Worker("src", 4, 4), Worker("z-dst", 4, 4)])
            first = pool.reserve("first", Demand(1, 1))
            second = pool.reserve("second", Demand(1, 1))

            def prepare(old_lease, target):
                if old_lease.job == "first":
                    if outcome == "raise":
                        raise RuntimeError("boom")
                    if outcome == "false":
                        return False
                    return "yes"  # truthy but not exactly True
                return True

            result = pool.drain("src", prepare)
            self.assertEqual(result["status"], "prepare_failed", outcome)
            self.assertEqual(result["moved"], {})
            self.assertIs(pool.current("first"), first)
            self.assertIs(pool.current("second"), second)
            # All holds returned; only the original source occupancy remains.
            self.assertEqual(pool.available(), {"src": (2, 2), "z-dst": (4, 4)})


class TimedSchedulingTest(TestCase):
    def test_future_promises_constrain_ordinary_leases_and_expiry_fences_names(self):
        pool = Pool([Worker("one", 5, 6)])
        first = pool.book("first", Demand(4, 1), 2, 4)
        second = pool.book("second", Demand(1, 5), 4, 6)
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)

        # At t=4 the disjoint promises need (1, 7) memory in aggregate, so an
        # indefinite (1, 2) lease is unsafe even though t=0 looks empty.
        self.assertIsNone(pool.reserve("ordinary", Demand(1, 2)))

        third = pool.book("third", Demand(1, 1), 2, 6)
        self.assertIsNotNone(third)
        self.assertIsNone(pool.book("tight", Demand(1, 1), 3, 5))
        self.assertIsNone(pool.reserve("tiny", Demand(1, 1)))
        # Future bookings are not deducted from current availability.
        self.assertEqual(pool.available(), {"one": (5, 6)})

        pool.advance(4)
        # The started-but-unactivated second and third bookings already hold.
        self.assertEqual(pool.available(), {"one": (3, 0)})
        self.assertIsNone(pool.activate(first))

        second_lease = pool.activate(second)
        third_lease = pool.activate(third)
        self.assertIsNotNone(second_lease)
        self.assertIsNotNone(third_lease)
        self.assertIs(pool.current("second"), second_lease)
        self.assertIs(pool.current("third"), third_lease)
        # Activation never deducts the same reservation twice.
        self.assertEqual(pool.available(), {"one": (3, 0)})
        self.assertIs(pool.activate(second), second_lease)

        pool.advance(6)
        self.assertEqual(pool.available(), {"one": (5, 6)})
        self.assertIsNone(pool.current("second"))
        self.assertIsNone(pool.current("third"))

        replacement = pool.reserve("second", Demand(1, 1))
        self.assertIsNotNone(replacement)
        # The expired original lease cannot release the replacement occupancy.
        self.assertFalse(pool.release(second_lease))
        self.assertEqual(pool.available(), {"one": (4, 5)})
        self.assertTrue(pool.release(replacement))
        self.assertEqual(pool.available(), {"one": (5, 6)})

    def test_ordinary_reserve_prefers_current_margin_only_among_safe_workers(self):
        unsafe = Pool([Worker("a", 2, 2), Worker("b", 5, 6)])
        self.assertIsNotNone(unsafe.book("full-a", Demand(2, 2), 2, 4))
        lease = unsafe.reserve("long", Demand(1, 1))
        self.assertEqual(lease.worker, "b")

        safe = Pool([Worker("a", 2, 2), Worker("b", 5, 6)])
        lease = safe.reserve("long", Demand(1, 1))
        self.assertEqual(lease.worker, "a")

    def test_booking_starting_at_current_time_is_reserved_before_activation(self):
        pool = Pool([Worker("w", 2, 2)])
        booking = pool.book("job", Demand(1, 1), 0, 2)
        self.assertIsNone(pool.current("job"))
        self.assertEqual(pool.available(), {"w": (1, 1)})
        lease = pool.activate(booking)
        self.assertEqual(pool.available(), {"w": (1, 1)})
        pool.advance(2)
        self.assertIsNone(pool.current("job"))
        self.assertEqual(pool.available(), {"w": (2, 2)})

    def test_booking_tie_uses_smallest_worker_name(self):
        pool = Pool([Worker("z", 2, 2), Worker("a", 2, 2)])
        booking = pool.book("job", Demand(1, 1), 1, 3)
        self.assertEqual(booking.worker, "a")

    def test_booking_identity_window_and_foreign_credentials(self):
        pool = Pool([Worker("w", 4, 4)])
        booking = pool.book("job", Demand(1, 1), 1, 3)
        self.assertIsInstance(booking, Booking)
        self.assertEqual((booking.job, booking.worker, booking.start, booking.end),
                         ("job", "w", 1, 3))

        with self.assertRaises(ValueError):
            pool.activate(object())
        forged = Booking("job", "w", 1, 3)
        self.assertIsNone(pool.activate(forged))

        other = Pool([Worker("w", 4, 4)])
        foreign = other.book("job", Demand(1, 1), 1, 3)
        self.assertIsNone(pool.activate(foreign))
        self.assertIsNone(pool.activate(booking))

        pool.advance(1)
        lease = pool.activate(booking)
        self.assertIsNotNone(lease)
        self.assertIs(pool.activate(booking), lease)
        self.assertTrue(pool.release(lease))
        self.assertIsNone(pool.activate(booking))

    def test_backfill_choice_must_match_runtime_and_capacity_consequences(self):
        pool = Pool([
            Worker("a", 4, 4, frozenset({"base", "staging"})),
            Worker("b", 5, 4, frozenset({"base", "accel"})),
        ])
        booking = pool.book("future-a", Demand(3, 3), 4, 8)
        lease, until = pool.backfill(
            "backfill", Demand(2, 2, frozenset({"base"})), 3, 6,
        )
        accel = Demand(4, 3, frozenset({"accel"}))

        if lease.worker == "a":
            self.assertEqual(until, 4)
            ordinary = pool.reserve("accel-job", accel)
            self.assertIsNotNone(ordinary)
            self.assertEqual(ordinary.worker, "b")
        else:
            self.assertEqual(lease.worker, "b")
            self.assertEqual(until, 6)
            self.assertIsNone(pool.reserve("accel-job", accel))

        pool.advance(4)
        booking_lease = pool.activate(booking)
        self.assertIsNotNone(booking_lease)
        self.assertEqual(pool.available()["a"], (1, 1))

        if lease.worker == "a":
            # [0, 4) is half-open: t=4 belongs to the booking, not backfill.
            self.assertIsNone(pool.current("backfill"))
            self.assertFalse(pool.release(lease))
            self.assertEqual(pool.available()["a"], (1, 1))
        else:
            self.assertIs(pool.current("backfill"), lease)
            self.assertEqual(pool.available()["b"], (3, 2))
            self.assertIsNone(pool.reserve("accel-at-4", accel))
            pool.advance(6)
            self.assertIsNone(pool.current("backfill"))
            self.assertFalse(pool.release(lease))
            self.assertEqual(pool.available()["b"], (5, 4))
            landed = pool.reserve("accel-after-6", accel)
            self.assertIsNotNone(landed)
            self.assertEqual(landed.worker, "b")

    def test_forced_a_backfill_gap_minimum_and_earlier_start(self):
        def build(start):
            pool = Pool([
                Worker("a", 4, 4, frozenset({"base", "staging"})),
                Worker("b", 5, 4, frozenset({"base", "accel"})),
            ])
            long_lease = pool.reserve(
                "accel-long", Demand(4, 3, frozenset({"accel"})),
            )
            self.assertEqual(long_lease.worker, "b")
            booking = pool.book("future-a", Demand(3, 3), start, start + 4)
            return pool, booking

        pool, booking = build(4)
        lease, until = pool.backfill(
            "backfill", Demand(2, 2, frozenset({"base"})), 3, 6,
        )
        self.assertEqual(lease.worker, "a")
        self.assertEqual(until, 4)
        before = pool.available()
        self.assertIsNone(pool.backfill(
            "too-short", Demand(2, 2, frozenset({"base"})), 5, 6,
        ))
        self.assertEqual(pool.available(), before)

        pool.advance(3)
        self.assertIs(pool.current("backfill"), lease)
        self.assertEqual(pool.available()["a"], (2, 2))
        pool.advance(4)
        self.assertIsNone(pool.current("backfill"))
        self.assertIsNotNone(pool.activate(booking))
        self.assertEqual(pool.available()["a"], (1, 1))
        self.assertFalse(pool.release(lease))
        pool.advance(8)
        self.assertEqual(pool.available()["a"], (4, 4))

        earlier, earlier_booking = build(3)
        earlier_lease, earlier_until = earlier.backfill(
            "backfill", Demand(2, 2, frozenset({"base"})), 3, 6,
        )
        self.assertEqual(earlier_lease.worker, "a")
        self.assertEqual(earlier_until, 3)
        earlier.advance(3)
        self.assertIsNone(earlier.current("backfill"))
        self.assertIsNotNone(earlier.activate(earlier_booking))
        self.assertEqual(earlier.available()["a"], (1, 1))

    def test_early_release_abandons_remaining_booking_window(self):
        pool = Pool([Worker("w", 5, 6)])
        booking = pool.book("job", Demand(2, 2), 2, 6)
        pool.advance(2)
        lease = pool.activate(booking)
        self.assertEqual(pool.available(), {"w": (3, 4)})

        pool.advance(3)
        self.assertTrue(pool.release(lease))
        self.assertEqual(pool.available(), {"w": (5, 6)})
        self.assertIsNone(pool.activate(booking))
        self.assertFalse(pool.release(lease))

        # The formerly conflicting remainder is genuinely free again.
        replacement = pool.book("job", Demand(5, 6), 3, 5)
        self.assertIsNotNone(replacement)
        replacement_lease = pool.activate(replacement)
        self.assertEqual(pool.available(), {"w": (0, 0)})
        pool.advance(5)
        self.assertIsNone(pool.current("job"))
        self.assertEqual(pool.available(), {"w": (5, 6)})
        self.assertFalse(pool.release(replacement_lease))

    def test_duplicate_job_is_rejected_across_all_three_ledger_entries(self):
        pool = Pool([Worker("w", 4, 4)])
        booking = pool.book("job", Demand(1, 1), 2, 4)
        self.assertIsNotNone(booking)
        with self.assertRaises(ValueError):
            pool.reserve("job", Demand(1, 1))
        with self.assertRaises(ValueError):
            pool.book("job", Demand(1, 1), 4, 5)
        with self.assertRaises(ValueError):
            pool.backfill("job", Demand(1, 1), 1, 2)

        pool.advance(4)
        ordinary = pool.reserve("job", Demand(1, 1))
        self.assertIsNotNone(ordinary)
        with self.assertRaises(ValueError):
            pool.book("job", Demand(1, 1), 4, 5)
        with self.assertRaises(ValueError):
            pool.backfill("job", Demand(1, 1), 1, 2)

    def test_time_parameter_validation_and_backwards_clock(self):
        pool = Pool([Worker("w", 4, 4)])
        for bad_now in (-1, True, 1.0, "1"):
            with self.assertRaises(ValueError):
                pool.advance(bad_now)

        pool.advance(2)
        pool.advance(2)  # Equal advance is legal.
        with self.assertRaises(ValueError):
            pool.advance(1)

        with self.assertRaises(ValueError):
            pool.book(7, Demand(1, 1), 3, 4)
        with self.assertRaises(ValueError):
            pool.book("j", object(), 3, 4)
        with self.assertRaises(ValueError):
            pool.book("j", Demand(1, 1), True, 4)
        with self.assertRaises(ValueError):
            pool.book("j", Demand(1, 1), 3, True)
        with self.assertRaises(ValueError):
            pool.book("j", Demand(1, 1), 2, 2)
        with self.assertRaises(ValueError):
            pool.book("j", Demand(1, 1), 1, 2)  # Before current time 2.
        with self.assertRaises(ValueError):
            pool.book("j", Demand(1, 1), 2, 67)

        for min_ticks, max_ticks in ((True, 2), (1, True), (0, 2),
                                     (2, 1), (1, 65)):
            with self.assertRaises(ValueError):
                pool.backfill("bf", Demand(1, 1), min_ticks, max_ticks)

    def test_timed_pool_scale_and_record_limit(self):
        oversized = Pool([Worker(f"w{i}", 1, 1) for i in range(9)])
        with self.assertRaises(ValueError):
            oversized.book("timed", Demand(1, 1), 1, 2)
        with self.assertRaises(ValueError):
            oversized.backfill("timed", Demand(1, 1), 1, 2)
        # Old admission keeps working without the timed scale restriction.
        self.assertIsNotNone(oversized.reserve("ordinary", Demand(1, 1)))

        tall = Pool([Worker("w", 65, 1)])
        with self.assertRaises(ValueError):
            tall.book("timed", Demand(1, 1), 1, 2)
        with self.assertRaises(ValueError):
            tall.backfill("timed", Demand(1, 1), 1, 2)
        self.assertIsNotNone(tall.reserve("ordinary", Demand(1, 1)))

        pool = Pool([Worker("w", 64, 64)])
        bookings = []
        for index in range(16):
            booking = pool.book(f"b{index}", Demand(1, 1), 1, 2)
            self.assertIsNotNone(booking)
            bookings.append(booking)
        # The 17th otherwise-feasible request hits the unfinished-record cap.
        self.assertIsNone(pool.book("overflow", Demand(1, 1), 1, 2))
        self.assertEqual(len(pool._timed), 16)

        # Activation changes the lease state, not the timed-record count.
        pool.advance(1)
        self.assertIsNotNone(pool.activate(bookings[0]))
        self.assertEqual(len(pool._timed), 16)
        self.assertIsNone(pool.book("overflow-active", Demand(1, 1), 1, 2))

        pool.advance(2)
        self.assertEqual(len(pool._timed), 0)
        self.assertIsNotNone(pool.book("after-expiry", Demand(1, 1), 2, 3))

    def test_drain_is_busy_while_timed_records_exist_and_works_after_expiry(self):
        pool = Pool([Worker("a-src", 4, 4), Worker("b-dst", 4, 4)])
        ordinary = pool.reserve("ordinary", Demand(1, 1))
        self.assertEqual(ordinary.worker, "a-src")
        booking = pool.book("scheduled", Demand(1, 1), 2, 4)
        self.assertEqual(booking.worker, "a-src")
        calls = []

        result = pool.drain(
            "a-src", lambda old, target: calls.append((old.job, target)) or True,
        )
        self.assertEqual(result, {"status": "busy", "moved": {}})
        self.assertEqual(calls, [])

        # Busy timed drain did not close the source.
        late = pool.reserve("late", Demand(1, 1))
        self.assertEqual(late.worker, "a-src")
        self.assertEqual(pool.drain("a-src", lambda *_: True)["status"], "busy")

        pool.advance(4)
        after_clear = pool.reserve("after-clear", Demand(1, 1))
        self.assertEqual(after_clear.worker, "a-src")
        result = pool.drain("a-src", lambda *_: True)
        self.assertEqual(result["status"], "drained")
        self.assertEqual(set(result["moved"]),
                         {"ordinary", "late", "after-clear"})
        self.assertFalse(pool.release(ordinary))
        self.assertEqual(pool.available(),
                         {"a-src": (4, 4), "b-dst": (1, 1)})

    def test_timed_admission_raises_runtime_error_during_running_drain(self):
        pool = Pool([Worker("src", 3, 3), Worker("dst", 4, 4)])
        old_lease = pool.reserve("moving", Demand(1, 1))
        entered = Event()
        release_block = Event()
        result = {}

        def prepare(old, target):
            entered.set()
            release_block.wait(5)
            return True

        thread = Thread(target=lambda: result.update(
            pool.drain("src", prepare),
        ))
        thread.start()
        self.assertTrue(entered.wait(5))
        try:
            with self.assertRaises(RuntimeError):
                pool.book("blocked-booking", Demand(1, 1), 1, 2)
            with self.assertRaises(RuntimeError):
                pool.backfill("blocked-backfill", Demand(1, 1), 1, 2)
            with self.assertRaises(ValueError):
                pool.book("bad-window", Demand(1, 1), 2, 1)
        finally:
            release_block.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())

        self.assertEqual(result["status"], "drained")
        self.assertEqual(pool._timed, [])
        self.assertEqual(pool.available(), {"src": (3, 3), "dst": (3, 3)})
        self.assertIs(pool.current("moving"), result["moved"]["moving"])
        self.assertFalse(pool.release(old_lease))
        # Timed admission works once the evacuation has finished.
        self.assertIsNotNone(pool.book("after-drain", Demand(1, 1), 1, 2))

    def test_concurrent_same_job_admission_has_single_winner(self):
        pool = Pool([Worker("w", 64, 64)])
        gate = Barrier(3)

        def attempt(kind):
            gate.wait(5)
            try:
                if kind == 0:
                    return pool.book("same", Demand(1, 1), 1, 2) is not None
                if kind == 1:
                    return pool.backfill("same", Demand(1, 1), 1, 1) is not None
                return pool.reserve("same", Demand(1, 1)) is not None
            except ValueError:
                return False

        with ThreadPoolExecutor(max_workers=3) as executor:
            outcomes = list(executor.map(attempt, range(3)))
        self.assertEqual(outcomes.count(True), 1)


class PermitAdmissionTest(TestCase):
    @staticmethod
    def _two_unique_workers():
        return Pool([
            Worker("a", 4, 4, frozenset({"a-only"})),
            Worker("b", 4, 4, frozenset({"b-only"})),
        ])

    def test_static_arrangement_does_not_imply_online_permit(self):
        pool = Pool([
            Worker("a", 2, 2, frozenset({"a-only"})),
            Worker("b", 2, 2, frozenset({"b-only"})),
        ])
        both = (
            Demand(2, 2, frozenset({"a-only"})),
            Demand(2, 2, frozenset({"b-only"})),
        )
        g_permit = pool.admit("g", both)
        self.assertIsInstance(g_permit, Permit)
        self.assertEqual(g_permit.job, "g")
        self.assertIsNone(pool.current("g"))
        self.assertEqual(pool.available(), {"a": (2, 2), "b": (2, 2)})

        # If g's worker were known, f could use the other worker.  But f may
        # claim first, so admitting f would leave one of g's legal options dead.
        self.assertIsNone(pool.admit("f", (Demand(1, 1),)))
        self.assertIsNone(pool.reserve("f", Demand(1, 1)))
        self.assertEqual(pool.available(), {"a": (2, 2), "b": (2, 2)})

        self.assertTrue(pool.cancel_permit(g_permit))
        direct = pool.reserve("f", Demand(1, 1))
        self.assertIsNotNone(direct)
        # Equal residual margins preserve the original smallest-name preference.
        self.assertEqual(direct.worker, "a")

    def test_removing_one_contingency_lets_reserve_choose_safe_worker(self):
        pool = Pool([
            Worker("a", 2, 2, frozenset({"a-only"})),
            Worker("b", 2, 2, frozenset({"b-only"})),
        ])
        g_permit = pool.admit(
            "g", (Demand(2, 2, frozenset({"a-only"})),),
        )
        f_lease = pool.reserve("f", Demand(1, 1))
        self.assertEqual(f_lease.worker, "b")
        g_lease = pool.claim(g_permit, 0)
        self.assertEqual(g_lease.worker, "a")
        self.assertEqual(pool.available(), {"a": (0, 0), "b": (1, 1)})

    def test_first_claim_worker_choice_changes_which_direct_request_is_safe(self):
        pool = self._two_unique_workers()
        g_permit = pool.admit("g", (
            Demand(2, 1, frozenset({"a-only"})),
            Demand(1, 2, frozenset({"b-only"})),
        ))
        f_permit = pool.admit("f", (Demand(2, 2),))
        f_lease = pool.claim(f_permit, 0)
        self.assertIn(f_lease.worker, ("a", "b"))

        a_request = Demand(1, 2, frozenset({"a-only"}))
        b_request = Demand(2, 1, frozenset({"b-only"}))
        if f_lease.worker == "a":
            # It physically fits the residual on a, but would kill g's option 0.
            before = pool.available()
            self.assertIsNone(pool.reserve("a-job", a_request))
            self.assertEqual(pool.available(), before)
            accepted = pool.reserve("b-job", b_request)
            self.assertIsNotNone(accepted)
            self.assertEqual(accepted.worker, "b")
            self.assertIsNone(pool.reserve("a-after", a_request))
        else:
            before = pool.available()
            self.assertIsNone(pool.reserve("b-job", b_request))
            self.assertEqual(pool.available(), before)
            accepted = pool.reserve("a-job", a_request)
            self.assertIsNotNone(accepted)
            self.assertEqual(accepted.worker, "a")
            self.assertIsNone(pool.reserve("b-after", b_request))

        # The recursive promise to g remains redeemable for either option.
        g_lease = pool.claim(g_permit, 0)
        self.assertIsNotNone(g_lease)
        self.assertEqual(g_lease.worker, "a")

        for lease in (f_lease, accepted, g_lease):
            self.assertTrue(pool.release(lease))
        self.assertEqual(pool.available(), {"a": (4, 4), "b": (4, 4)})

    def test_each_named_option_lands_on_its_labelled_worker_and_releases_all(self):
        expected = {
            0: ("a", (2, 3), (4, 4)),
            1: ("b", (4, 4), (3, 2)),
        }
        for option, (worker, a_free, b_free) in expected.items():
            pool = self._two_unique_workers()
            g_permit = pool.admit("g", (
                Demand(2, 1, frozenset({"a-only"})),
                Demand(1, 2, frozenset({"b-only"})),
            ))
            f_permit = pool.admit("f", (Demand(2, 2),))

            g_lease = pool.claim(g_permit, option)
            self.assertIsNotNone(g_lease)
            self.assertEqual(g_lease.worker, worker)
            self.assertEqual(pool.current("g"), g_lease)
            self.assertEqual(pool.available(), {"a": a_free, "b": b_free})

            before_f = pool.available()
            f_lease = pool.claim(f_permit, 0)
            self.assertIsNotNone(f_lease)
            cpu, memory = before_f[f_lease.worker]
            self.assertEqual(
                pool.available()[f_lease.worker], (cpu - 2, memory - 2),
            )
            self.assertTrue(pool.release(f_lease))
            self.assertTrue(pool.release(g_lease))
            self.assertEqual(pool.available(), {"a": (4, 4), "b": (4, 4)})

    def test_concurrent_claims_of_same_original_permit_have_one_winner(self):
        pool = Pool([Worker("w", 3, 3)])
        permit = pool.admit("same", (Demand(1, 2), Demand(2, 1)))
        gate = Barrier(2)
        outcomes = {}

        def claim(option):
            gate.wait(5)
            outcomes[option] = pool.claim(permit, option)

        threads = [Thread(target=claim, args=(option,)) for option in (0, 1)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
            self.assertFalse(thread.is_alive())

        leases = [lease for lease in outcomes.values() if lease is not None]
        self.assertEqual(len(leases), 1)
        lease = leases[0]
        self.assertEqual(lease.worker, "w")
        self.assertIs(pool.current("same"), lease)
        self.assertIn(pool.available(), [{"w": (2, 1)}, {"w": (1, 2)}])
        self.assertTrue(pool.release(lease))
        self.assertEqual(pool.available(), {"w": (3, 3)})
        self.assertIsNone(pool.claim(permit, 0))

    def test_stale_forged_foreign_and_reissued_credentials_are_fenced(self):
        pool = Pool([Worker("w", 3, 3)])
        old = pool.admit("job", (Demand(1, 2), Demand(2, 1)))
        self.assertTrue(pool.cancel_permit(old))

        forged = Permit("job")
        other = Pool([Worker("w", 3, 3)])
        foreign = other.admit("job", (Demand(1, 1),))

        new = pool.admit("job", (Demand(1, 1), Demand(2, 1)))
        self.assertIsNone(pool.claim(old, 0))
        self.assertFalse(pool.cancel_permit(old))
        self.assertIsNone(pool.claim(forged, 0))
        self.assertFalse(pool.cancel_permit(forged))
        self.assertIsNone(pool.claim(foreign, 0))
        self.assertFalse(pool.cancel_permit(foreign))

        with self.assertRaises(ValueError):
            pool.claim(new, -1)
        with self.assertRaises(ValueError):
            pool.claim(new, 2)
        with self.assertRaises(ValueError):
            pool.claim(new, True)
        with self.assertRaises(ValueError):
            pool.claim(new, "0")
        # Validation failures do not consume the live permit.
        lease = pool.claim(new, 0)
        self.assertIsNotNone(lease)
        self.assertIsNone(pool.claim(new, 1))
        self.assertFalse(pool.cancel_permit(new))
        self.assertTrue(pool.release(lease))

        reissued = pool.admit("job", (Demand(1, 1),))
        self.assertIsNone(pool.claim(new, 0))
        self.assertFalse(pool.cancel_permit(new))
        self.assertTrue(pool.cancel_permit(reissued))

        with self.assertRaises(ValueError):
            pool.claim(object(), 0)
        with self.assertRaises(ValueError):
            pool.cancel_permit(object())

    def test_permit_is_frozen_and_has_no_worker_or_time_fields(self):
        permit = Pool([Worker("w", 1, 1)]).admit("job", (Demand(1, 1),))
        self.assertEqual(permit.job, "job")
        self.assertFalse(hasattr(permit, "worker"))
        self.assertFalse(hasattr(permit, "start"))
        with self.assertRaises(AttributeError):
            permit.job = "other"

    def test_closed_worker_is_not_available_for_permit_redemption(self):
        pool = self._two_unique_workers()
        self.assertEqual(
            pool.drain("a", lambda *_: True)["status"], "drained",
        )
        # Option 0 can only use closed worker a, so the two-option permit is
        # not redeemable for every legal option.
        self.assertIsNone(pool.admit("g", (
            Demand(1, 1, frozenset({"a-only"})),
            Demand(1, 1, frozenset({"b-only"})),
        )))
        permit = pool.admit("h", (Demand(1, 1, frozenset({"b-only"})),))
        lease = pool.claim(permit, 0)
        self.assertEqual(lease.worker, "b")

    def test_permit_has_no_time_or_automatic_expiry(self):
        pool = Pool([Worker("w", 1, 1)])
        permit = pool.admit("job", (Demand(1, 1),))
        pool.advance(10_000)
        lease = pool.claim(permit, 0)
        self.assertIsNotNone(lease)
        self.assertEqual(pool.current("job"), lease)

    def test_admit_validation_scale_and_five_permit_limit(self):
        pool = Pool([Worker("w", 64, 64)])
        with self.assertRaises(ValueError):
            pool.admit("no-options", ())
        with self.assertRaises(ValueError):
            pool.admit("three", (Demand(1, 1),) * 3)
        with self.assertRaises(ValueError):
            pool.admit("a-list", [Demand(1, 1)])
        with self.assertRaises(ValueError):
            pool.admit("bad-option", (Demand(1, 1), object()))
        with self.assertRaises(ValueError):
            pool.admit("too-big", (Demand(65, 1),))
        with self.assertRaises(ValueError):
            pool.admit("too-big-mem", (Demand(1, 65),))
        self.assertIsNotNone(pool.admit("edge", (Demand(64, 64),)))
        with self.assertRaises(ValueError):
            pool.reserve("edge", Demand(1, 1))

        seven = Pool([Worker(f"w{i}", 1, 1) for i in range(7)])
        with self.assertRaises(ValueError):
            seven.admit("too-many-workers", (Demand(1, 1),))
        # The new permit envelope does not constrain the old reserve entry.
        self.assertIsNotNone(seven.reserve("old-reserve", Demand(1, 1)))

        tall = Pool([Worker("w", 65, 64)])
        with self.assertRaises(ValueError):
            tall.admit("tall", (Demand(1, 1),))
        self.assertIsNotNone(tall.reserve("old-reserve", Demand(1, 1)))

        limited = Pool([Worker(f"w{i}", 64, 64) for i in range(6)])
        permits = [
            limited.admit(f"p{i}", (Demand(1, 1),))
            for i in range(5)
        ]
        self.assertTrue(all(permits))
        self.assertIsNone(limited.admit("sixth", (Demand(1, 1),)))
        self.assertTrue(limited.cancel_permit(permits[0]))
        self.assertIsNotNone(limited.admit("sixth", (Demand(1, 1),)))

    def test_duplicate_name_is_rejected_across_permits_leases_and_timed(self):
        pool = Pool([Worker("w", 4, 4)])
        permit = pool.admit("job", (Demand(1, 1),))
        with self.assertRaises(ValueError):
            pool.admit("job", (Demand(1, 1),))
        with self.assertRaises(ValueError):
            pool.reserve("job", Demand(1, 1))
        with self.assertRaises(ValueError):
            pool.book("job", Demand(1, 1), 1, 2)
        with self.assertRaises(ValueError):
            pool.backfill("job", Demand(1, 1), 1, 1)

        lease = pool.claim(permit, 0)
        with self.assertRaises(ValueError):
            pool.admit("job", (Demand(1, 1),))
        self.assertTrue(pool.release(lease))
        replacement = pool.admit("job", (Demand(1, 1),))
        self.assertIsNotNone(replacement)

    def test_timed_entries_and_running_drain_block_admit_but_not_validation(self):
        timed_pool = Pool([Worker("w", 4, 4)])
        self.assertIsNotNone(timed_pool.book("future", Demand(1, 1), 1, 2))
        self.assertIsNone(timed_pool.admit("permit-job", (Demand(1, 1),)))
        self.assertEqual(timed_pool.available(), {"w": (4, 4)})

        pool = Pool([Worker("src", 4, 4), Worker("dst", 4, 4)])
        moving = pool.reserve("moving", Demand(1, 1))
        source = moving.worker
        entered = Event()
        release_prepare = Event()

        def prepare(_old, _target):
            entered.set()
            release_prepare.wait(5)
            return True

        result = {}
        thread = Thread(target=lambda: result.update(
            pool.drain(source, prepare),
        ))
        thread.start()
        self.assertTrue(entered.wait(5))
        try:
            self.assertIsNone(pool.admit("during-prepare", (Demand(1, 1),)))
        finally:
            release_prepare.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result["status"], "drained")

    def test_outstanding_permits_block_timed_and_drain_without_closing_worker(self):
        pool = Pool([Worker("w", 4, 4)])
        permit = pool.admit("choice", (Demand(1, 1), Demand(2, 1)))
        self.assertIsNotNone(permit)

        with self.assertRaises(RuntimeError):
            pool.book("booked", Demand(1, 1), 1, 2)
        with self.assertRaises(RuntimeError):
            pool.backfill("filled", Demand(1, 1), 1, 1)
        # Parameter and duplicate-name ValueErrors retain their priority.
        with self.assertRaises(ValueError):
            pool.book("bad-window", Demand(1, 1), 2, 1)
        with self.assertRaises(ValueError):
            pool.backfill("bad-ticks", Demand(1, 1), True, 1)
        with self.assertRaises(ValueError):
            pool.book("choice", Demand(1, 1), 1, 2)

        result = pool.drain("w", lambda *_: True)
        self.assertEqual(result, {"status": "busy", "moved": {}})
        # Busy returned before closing w.
        ordinary = pool.reserve("ordinary", Demand(1, 1))
        self.assertIsNotNone(ordinary)
        self.assertEqual(ordinary.worker, "w")
