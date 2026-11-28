"""滚动租户预算的离线验收测试。

所有场景都基于显式推进的虚拟时钟，不读取系统时间，也不需要真实等待。
关键场景用简短中文打印实际读到的计费量、预约/回填结果与工位余量，便于
人工核对；断言里的期望值都是独立写死的常量，而不是从实现回读的中间量。
"""
import time
from unittest import TestCase

from placement import Artifact, Booking, Demand, Lease, Pool, Worker


def _two_worker_pool(budget, jobs, **kwargs):
    """a、b 各有 (4,6) 且带独占标签，便于把作业精确引导到指定工位。"""
    return Pool(
        [
            Worker("a", 4, 6, frozenset({"a"})),
            Worker("b", 4, 6, frozenset({"b"})),
        ],
        tenant_budgets={"t": budget},
        metered_jobs=jobs,
        **kwargs,
    )


def _show(title, pool, extra=""):
    print(f"[{title}] 时刻={pool._now} 计费={pool.tenant_usage('t')} "
          f"余额={pool.available()} {extra}")


class ScenarioOneTest(TestCase):
    """验收 1：只有 x 的实际释放时刻不同，却决定 y 能否预约。"""

    def _run(self, release_tick):
        pool = _two_worker_pool((4, 8, 11), {"x": "t", "y": "t"})
        x_booking = pool.book("x", Demand(2, 3, frozenset({"a"})), 0, 4)
        self.assertIsNotNone(x_booking)
        x_lease = pool.activate(x_booking)
        self.assertEqual(x_lease.worker, "a")

        pool.advance(release_tick)
        self.assertTrue(pool.release(x_lease))
        pool.advance(2)
        _show(f"验收1-x在tick{release_tick}释放", pool)
        usage = pool.tenant_usage("t")
        available = pool.available()

        y_booking = pool.book("y", Demand(2, 3, frozenset({"b"})), 2, 5)
        _show(f"验收1-y首选[tick2,5)", pool, f"预约={y_booking}")
        return pool, x_lease, usage, available, y_booking

    def test_release_at_one_leaves_room_for_next_booking(self):
        pool, x_lease, usage, available, y_booking = self._run(1)
        self.assertEqual(usage, (2, 3))
        self.assertEqual(available, {"a": (4, 6), "b": (4, 6)})
        self.assertIsNotNone(y_booking)
        self.assertEqual((y_booking.worker, y_booking.start, y_booking.end),
                         ("b", 2, 5))
        # 预约在 tick 2 进入窗口时已真实扣减 b 的两维容量，激活不重复扣。
        self.assertEqual(pool.available(), {"a": (4, 6), "b": (2, 3)})
        y_lease = pool.activate(y_booking)
        self.assertEqual(pool.current("y"), y_lease)
        self.assertEqual(pool.available(), {"a": (4, 6), "b": (2, 3)})
        self.assertFalse(pool.release(x_lease))

    def test_release_at_two_blocks_then_shifted_booking_starts(self):
        pool, x_lease, usage, available, y_booking = self._run(2)
        self.assertEqual(usage, (4, 6))
        self.assertEqual(available, {"a": (4, 6), "b": (4, 6)})
        # 窗口 [0,4) 内 x 与 y 合起来是 12 内存·tick，超过预算 11。
        self.assertIsNone(y_booking)
        self.assertEqual(pool.available(), {"a": (4, 6), "b": (4, 6)})
        self.assertEqual(pool.tenant_usage("t"), (4, 6))
        self.assertIsNone(pool.current("y"))

        retry = pool.book("y", Demand(2, 3, frozenset({"b"})), 3, 6)
        _show("验收1-y改约[tick3,6)", pool, f"预约={retry}")
        self.assertIsNotNone(retry)
        pool.advance(3)
        y_lease = pool.activate(retry)
        self.assertEqual(pool.current("y"), y_lease)
        self.assertEqual(pool.available(), {"a": (4, 6), "b": (2, 3)})
        # 旧 x 租约与同名新绑定无关，旧的释放不改变任何余额。
        self.assertFalse(pool.release(x_lease))
        self.assertEqual(pool.available(), {"a": (4, 6), "b": (2, 3)})
        # 未登记作业不受预算限制，仍可使用空着的工位。
        plain = pool.reserve("plain", Demand(1, 1))
        self.assertIsNotNone(plain)
        self.assertEqual(pool.tenant_usage("t"), (4, 6))


class ScenarioTwoTest(TestCase):
    """验收 2：候选片段结束之后也要保护已有的未来预约。"""

    def test_shortest_prefix_respects_existing_future_window(self):
        pool = _two_worker_pool((4, 8, 12), {"prep": "t", "old": "t"})
        old_booking = pool.book("old", Demand(3, 1, frozenset({"b"})), 4, 6)
        self.assertIsNotNone(old_booking)
        # 未来预约还不扣工位余量，但已进入预算承诺。
        self.assertEqual(pool.available(), {"a": (4, 6), "b": (4, 6)})
        self.assertEqual(pool.tenant_usage("t"), (0, 0))

        blocked = pool.backfill("prep", Demand(2, 2, frozenset({"a"})), 4, 4)
        _show("验收2-回填4tick", pool, f"回填={blocked}")
        # [1,5) 内候选 3 个 tick + 旧预约 1 个 tick = 9 CPU·tick > 8。
        self.assertIsNone(blocked)
        self.assertEqual(pool.available(), {"a": (4, 6), "b": (4, 6)})
        self.assertEqual(pool.tenant_usage("t"), (0, 0))
        self.assertIsNone(pool.current("prep"))

        result = pool.backfill("prep", Demand(2, 2, frozenset({"a"})), 1, 4)
        _show("验收2-回填1..4tick", pool, f"回填={result}")
        self.assertIsNotNone(result)
        lease, until = result
        self.assertEqual(until, 3)
        self.assertEqual(lease.worker, "a")
        self.assertEqual(pool.available(), {"a": (2, 4), "b": (4, 6)})

        pool.advance(3)
        _show("验收2-tick3回填退出", pool)
        self.assertIsNone(pool.current("prep"))
        self.assertFalse(pool.release(lease))
        self.assertEqual(pool.available(), {"a": (4, 6), "b": (4, 6)})
        self.assertEqual(pool.tenant_usage("t"), (6, 6))

        pool.advance(4)
        old_lease = pool.activate(old_booking)
        _show("验收2-tick4旧预约开工", pool, f"租约={old_lease}")
        self.assertEqual(pool.current("old"), old_lease)
        self.assertEqual(old_lease.worker, "b")
        self.assertEqual(pool.available(), {"a": (4, 6), "b": (1, 5)})
        self.assertEqual(pool.tenant_usage("t"), (6, 6))

    def test_without_budget_original_full_prefix_is_delivered(self):
        pool = Pool([
            Worker("a", 4, 6, frozenset({"a"})),
            Worker("b", 4, 6, frozenset({"b"})),
        ])
        self.assertIsNotNone(pool.book("old", Demand(3, 1, frozenset({"b"})), 4, 6))
        result = pool.backfill("prep", Demand(2, 2, frozenset({"a"})), 4, 4)
        print(f"[验收2-未配置预算] 回填={result} 余额={pool.available()}")
        self.assertIsNotNone(result)
        self.assertEqual(result[1], 4)


class ScenarioThreeTest(TestCase):
    """验收 3：跨过执行窗口的推进不能漏账，大跳跃不能逐 tick 追赶。"""

    def test_window_persists_after_jump_and_rebooking_is_fenced(self):
        pool = Pool(
            [Worker("w", 8, 16)],
            tenant_budgets={"t": (65536, 67108864, 67108864)},
            metered_jobs={"j": "t"},
        )
        booking = pool.book("j", Demand(2, 3), 10, 12)
        self.assertIsNotNone(booking)
        self.assertIsNone(pool.current("j"))

        pool.advance(20)
        _show("验收3-推进到20", pool)
        self.assertEqual(pool.available(), {"w": (8, 16)})
        self.assertEqual(pool.tenant_usage("t"), (4, 6))
        self.assertIsNone(pool.activate(booking))

        pool.advance(65547)
        _show("验收3-推进到65547", pool)
        self.assertEqual(pool.tenant_usage("t"), (2, 3))

        pool.advance(65548)
        _show("验收3-推进到65548", pool)
        self.assertEqual(pool.tenant_usage("t"), (0, 0))

        huge = 10 ** 12
        pool.advance(huge)
        fresh = pool.book("j", Demand(2, 3), huge, huge + 2)
        _show("验收3-大跳跃后重约", pool, f"预约={fresh}")
        self.assertIsNotNone(fresh)
        lease = pool.activate(fresh)
        self.assertEqual(pool.current("j"), lease)
        pool.advance(huge + 1)
        self.assertEqual(pool.tenant_usage("t"), (2, 3))
        self.assertTrue(pool.release(lease))
        self.assertEqual(pool.tenant_usage("t"), (2, 3))
        # 旧 Booking 与同字段伪造 Lease 都不能碰新的同名绑定。
        self.assertIsNone(pool.activate(booking))
        self.assertFalse(pool.release(Lease("j", "w")))
        self.assertEqual(pool.current("j"), None)

    def test_many_short_jobs_are_cheap_and_slide_to_zero(self):
        pool = Pool(
            [Worker("w", 1, 1)],
            tenant_budgets={"t": (65536, 67108864, 67108864)},
            metered_jobs={"k": "t"},
        )
        started = time.monotonic()
        now = 0
        for index in range(8192):
            booking = pool.book("k", Demand(1, 1), now, now + 1)
            self.assertIsNotNone(booking)
            self.assertIsNotNone(pool.activate(booking))
            if index % 2048 == 0:
                _show(f"验收3-短作业{index}", pool)
            pool.advance(now + 1)
            now += 1
        elapsed = time.monotonic() - started
        usage = pool.tenant_usage("t")
        print(f"[验收3-8192份短作业完成] 时刻={now} 计费={usage} "
              f"余额={pool.available()} 耗时={elapsed:.3f}s")
        self.assertEqual(usage, (8192, 8192))
        self.assertLess(elapsed, 20)
        pool.advance(now + 65536)
        self.assertEqual(pool.tenant_usage("t"), (0, 0))


class ScenarioFourTest(TestCase):
    """验收 4：预算不是物理账本的通行证，也不能跨租户串账。"""

    def test_two_tenants_still_compete_for_one_worker(self):
        pool = Pool(
            [Worker("w", 4, 4)],
            tenant_budgets={"t1": (4, 8, 8), "t2": (4, 8, 8)},
            metered_jobs={"a": "t1", "b": "t2"},
        )
        first = pool.book("a", Demand(3, 3), 0, 2)
        second = pool.book("b", Demand(3, 3), 0, 2)
        print(f"[验收4-单工位竞争] a预约={first} b预约={second} "
              f"t1={pool.tenant_usage('t1')} t2={pool.tenant_usage('t2')} "
              f"余额={pool.available()}")
        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(pool.available(), {"w": (1, 1)})
        self.assertEqual(pool.tenant_usage("t1"), (0, 0))
        self.assertEqual(pool.tenant_usage("t2"), (0, 0))

        pool.advance(1)
        self.assertEqual(pool.tenant_usage("t1"), (3, 3))
        self.assertEqual(pool.tenant_usage("t2"), (0, 0))

    def test_registered_names_cannot_bypass_budget_through_other_entries(self):
        # 合法的故障域与交换区：这里的 ValueError 专指预算绕过，而不是原配置拒绝。
        pool = Pool(
            [
                Worker("a", 2, 2, frozenset({"x"})),
                Worker("b", 2, 2, frozenset({"y"})),
            ],
            fault_domains={"a": "A", "b": "B"},
            exchange_blocks=4,
            tenant_budgets={"t": (4, 8, 8)},
            metered_jobs={"reg": "t", "reg2": "t"},
        )
        before = pool.available()
        with self.assertRaises(ValueError):
            pool.reserve("reg", Demand(1, 1))
        with self.assertRaises(ValueError):
            pool.admit("reg", (Demand(1, 1),))
        with self.assertRaises(ValueError):
            pool.book_group((
                ("reg", Demand(1, 1, frozenset({"x"}))),
                ("other", Demand(1, 1, frozenset({"y"}))),
            ), 1, 2)
        with self.assertRaises(ValueError):
            pool.start_reader("reg", Demand(1, 1), (Artifact("z"),))
        print(f"[验收4-登记名绕行拒绝] 余额={pool.available()} "
              f"计费={pool.tenant_usage('t')} 组={len(pool._groups)}")
        self.assertEqual(pool.available(), before)
        self.assertEqual(pool.tenant_usage("t"), (0, 0))
        self.assertEqual(pool._groups, [])
        self.assertEqual(pool._permits, [])

    def test_unregistered_entries_are_unchanged_when_budget_disabled(self):
        pool = Pool([Worker("a", 2, 2), Worker("b", 2, 2)])
        lease = pool.reserve("job", Demand(1, 1))
        self.assertIsNotNone(lease)
        booking = pool.book("timed", Demand(1, 1), 1, 2)
        self.assertIsNotNone(booking)
        self.assertGreaterEqual(pool.backfill("short", Demand(1, 1), 1, 1)[1], 1)
        # 许可与定时条目互斥，用独立池验证旧入口未被预算逻辑改动。
        permit_pool = Pool([Worker("a", 2, 2), Worker("b", 2, 2)])
        self.assertIsNotNone(permit_pool.admit("choice", (Demand(1, 1),)))
        self.assertFalse(hasattr(pool, "_metered") and pool._metered)


class TenantConfigTest(TestCase):
    def test_budget_validation_and_defensive_copies(self):
        workers = [Worker("w", 4, 4)]
        bad_budgets = [
            {"t": (0, 1, 1)},
            {"t": (65537, 1, 1)},
            {"t": (True, 1, 1)},
            {"t": (4, -1, 1)},
            {"t": (4, 1, 67108865)},
            {"": (4, 1, 1)},
            {"t": (4, 1)},
            {"t": [4, 1, 1]},
            {"t": (4, True, 1)},
        ]
        for budgets in bad_budgets:
            with self.assertRaises(ValueError):
                Pool(workers, tenant_budgets=budgets)
        with self.assertRaises(ValueError):
            Pool(workers, tenant_budgets={
                f"t{i}": (4, 1, 1) for i in range(9)})
        with self.assertRaises(ValueError):
            Pool(workers, tenant_budgets={"t": (4, 1, 1)},
                 metered_jobs={f"j{i}": "t" for i in range(33)})
        with self.assertRaises(ValueError):
            Pool(workers, tenant_budgets={"t": (4, 1, 1)},
                 metered_jobs={"j": "missing"})
        with self.assertRaises(ValueError):
            Pool(workers, tenant_budgets=[("t", (4, 1, 1))])
        with self.assertRaises(ValueError):
            Pool(workers, tenant_budgets={"t": (4, 1, 1)},
                 metered_jobs=["j"])
        with self.assertRaises(ValueError):
            Pool(workers, tenant_budgets={"t": (4, 1, 1)},
                 metered_jobs={"": "t"})

        budgets = {"t": (4, 8, 8)}
        jobs = {"j": "t"}
        pool = Pool(workers, tenant_budgets=budgets, metered_jobs=jobs)
        budgets["t"] = (1, 0, 0)
        jobs["j"] = "other"
        jobs["free"] = "t"
        # 构造后修改原字典不影响池：j 仍属 t，预算仍是 8，free 仍未登记。
        self.assertIsNotNone(pool.book("j", Demand(1, 1), 0, 2))
        self.assertEqual(pool.tenant_usage("t"), (0, 0))
        self.assertIsNotNone(pool.reserve("free", Demand(1, 1)))
        with self.assertRaises(ValueError):
            pool.tenant_usage("other")

    def test_unknown_tenant_query_raises(self):
        pool = Pool([Worker("w", 2, 2)],
                    tenant_budgets={"t": (4, 1, 1)}, metered_jobs={})
        self.assertEqual(pool.tenant_usage("t"), (0, 0))
        with self.assertRaises(ValueError):
            pool.tenant_usage("missing")
        with self.assertRaises(ValueError):
            pool.tenant_usage("")
