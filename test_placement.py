from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Thread
from unittest import TestCase

from demo import run
from placement import Demand, Lease, Pool, Worker


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
