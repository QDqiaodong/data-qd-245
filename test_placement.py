from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
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
