"""跨故障域成组预约与工位停接窗口的离线验收测试。

所有场景都基于显式推进的虚拟时钟，不需要真实等待；每个验收用例用简短中文
打印实际时刻、开工工位和两维余额，便于人工核对执行结果而非候选分配。
"""
from unittest import TestCase

from placement import (
    Booking,
    Demand,
    GroupTicket,
    Permit,
    Pool,
    Worker,
)


def _four_worker_pool():
    """a,b 承载 x 类副本（域 A/A），c,d 承载 y 类副本（域 B/C）。

    b/d 各带独占标签，旧 ``book`` 可借此被精确引导到指定工位；c 的 c-only
    标签用于证明停接期间拒绝的正是 c 自己。
    """
    return Pool([
        Worker("a", 2, 2, frozenset({"x"})),
        Worker("b", 2, 2, frozenset({"x", "old-b"})),
        Worker("c", 2, 2, frozenset({"y", "c-only"})),
        Worker("d", 2, 2, frozenset({"y", "old-d"})),
    ], fault_domains={"a": "A", "b": "A", "c": "B", "d": "C"})


DX = Demand(2, 2, frozenset({"x"}))
DY = Demand(2, 2, frozenset({"y"}))
DB = Demand(2, 2, frozenset({"old-b"}))
DD = Demand(2, 2, frozenset({"old-d"}))
DC = Demand(1, 1, frozenset({"c-only"}))


def _show(title, pool, extra=""):
    print(f"[{title}] 时刻={pool._now} 两维余额={pool.available()} {extra}")


class GroupBookingAcceptanceTest(TestCase):
    def test_scenario_one_per_replica_fit_must_not_admit_whole_group(self):
        """验收 1：同域拒绝整组；改域后到 tick 5 同时开工、tick 8 归还。"""
        labels = frozenset({"la"}), frozenset({"lb"})

        def pool_with(domains):
            return Pool([
                Worker("a", 2, 2, frozenset({"la"})),
                Worker("b", 2, 2, frozenset({"lb"})),
            ], fault_domains=domains)

        replicas = (("x", Demand(2, 2, frozenset({"la"}))),
                    ("y", Demand(2, 2, frozenset({"lb"}))))

        # a、b 同属域 A：逐副本都有位置，但整组拿不到互异故障域。
        same_domain = pool_with({"a": "A", "b": "A"})
        before = same_domain.available()
        self.assertIsNone(same_domain.book_group(replicas, 5, 8))
        self.assertEqual(same_domain.available(), before)
        self.assertIsNone(same_domain.current("x"))
        self.assertIsNone(same_domain.current("y"))
        _show("验收1-拒绝", same_domain, "同域 A/A，整组被拒，余额不动")

        # 只把 b 改为域 B：提前领用为空，到 tick 5 同时拿到两份真实租约。
        pool = pool_with({"a": "A", "b": "B"})
        ticket = pool.book_group(replicas, 5, 8)
        self.assertIsInstance(ticket, GroupTicket)
        self.assertIsNone(pool.activate_group(ticket))  # 提前领用不消费
        self.assertEqual(pool.available(), {"a": (2, 2), "b": (2, 2)})

        pool.advance(5)
        leases = pool.activate_group(ticket)
        self.assertIsNotNone(leases)
        self.assertEqual([lease.worker for lease in leases], ["a", "b"])
        self.assertEqual([lease.job for lease in leases], ["x", "y"])
        self.assertEqual(pool.available(), {"a": (0, 0), "b": (0, 0)})
        _show("验收1-开工", pool, "x->a、y->b，两工位余额归零")

        # 有效凭据重复领用返回同一批租约对象。
        again = pool.activate_group(ticket)
        self.assertEqual(again, leases)
        self.assertTrue(all(a is b for a, b in zip(again, leases)))
        self.assertIs(pool.current("x"), leases[0])
        self.assertIs(pool.current("y"), leases[1])

        # tick 8 两份租约失效，容量完整归还。
        pool.advance(8)
        self.assertIsNone(pool.current("x"))
        self.assertIsNone(pool.current("y"))
        self.assertEqual(pool.available(), {"a": (2, 2), "b": (2, 2)})
        self.assertIsNone(pool.activate_group(ticket))  # 越过开工点凭据失效
        _show("验收1-结束", pool, "tick 8 租约失效、容量归还")

    def test_scenario_two_only_the_stop_endpoint_changes_admission(self):
        """验收 2：组预约 y 占 c 后，旧 book 仅可在 d；停 c [4,5) 接纳、[4,6) 拒绝。"""
        for duration, expected in ((1, (4, 5)), (2, None)):
            pool = _four_worker_pool()
            ticket = pool.book_group((("x", DX), ("y", DY)), 5, 8)
            self.assertIsNotNone(ticket)
            old_booking = pool.book("old", DD, 5, 8)
            self.assertEqual(old_booking.worker, "d")

            window = pool.schedule_stop("c", 4, 4, duration)
            self.assertEqual(window, expected)

            pool.advance(5)
            leases = pool.activate_group(ticket)
            self.assertEqual([lease.worker for lease in leases], ["a", "c"])
            old_lease = pool.activate(old_booking)
            self.assertEqual(old_lease.worker, "d")
            self.assertEqual(pool.available(),
                             {"a": (0, 0), "b": (2, 2), "c": (0, 0), "d": (0, 0)})
            _show(
                f"验收2-停接时长{duration}",
                pool,
                f"停接窗口={window}，组 x->a、y->c，旧预约->d",
            )

        # 被拒绝的停接不能提前占走 c：另起一池验证 c 在 tick 4 仍可接单。
        pool = _four_worker_pool()
        pool.book_group((("x", DX), ("y", DY)), 5, 8)
        pool.book("old", DD, 5, 8)
        self.assertIsNone(pool.schedule_stop("c", 4, 4, 2))
        pool.advance(4)
        self.assertIsNotNone(pool.book("probe", DC, 4, 5))

    def test_scenario_three_same_stop_request_different_real_start(self):
        """验收 3：同一停接请求因真实开工顺序不同，落在 [5,7) 或 [8,10)。"""
        # 分支 A：先停接、后组预约。c 在 [5,7) 停接，与组窗口相交，d 被组占走。
        pool_a = _four_worker_pool()
        self.assertEqual(pool_a.book("bwork", DB, 5, 8).worker, "b")
        window_a = pool_a.schedule_stop("c", 5, 8, 2)
        self.assertEqual(window_a, (5, 7))
        ticket_a = pool_a.book_group((("x", DX), ("y", DY)), 5, 8)
        self.assertIsNotNone(ticket_a)
        self.assertIsNone(pool_a.book("dwork", DD, 5, 8))  # d 已被 y 占满

        pool_a.advance(5)
        leases_a = pool_a.activate_group(ticket_a)
        self.assertEqual([lease.worker for lease in leases_a], ["a", "d"])
        self.assertEqual(pool_a.available(),
                         {"a": (0, 0), "b": (0, 0), "c": (2, 2), "d": (0, 0)})
        _show("验收3A-开工", pool_a,
              "停接[5,7)，x->a、y->d；c 无副本但停接中")
        # c 此刻没有副本占用，却仍不能取得一 tick 的 book。
        self.assertIsNone(pool_a.book("during-stop", DC, 5, 6))
        # 别的工位照常工作：b 虽满，a/d 上的组副本正常运行（已通过余额体现）。

        pool_a.advance(7)  # 精确推进到停接结束点。
        reopened = pool_a.book("after-stop", DC, 7, 8)
        self.assertIsNotNone(reopened)
        self.assertEqual(reopened.worker, "c")
        _show("验收3A-停接结束", pool_a, "tick 7 起 c 重新接单")

        # 分支 B：先组预约、后停接。停接被推到 [8,10)，d 的旧请求成功。
        pool_b = _four_worker_pool()
        self.assertEqual(pool_b.book("bwork", DB, 5, 8).worker, "b")
        ticket_b = pool_b.book_group((("x", DX), ("y", DY)), 5, 8)
        window_b = pool_b.schedule_stop("c", 5, 8, 2)
        self.assertEqual(window_b, (8, 10))
        d_booking = pool_b.book("dwork", DD, 5, 8)
        self.assertEqual(d_booking.worker, "d")

        pool_b.advance(5)
        leases_b = pool_b.activate_group(ticket_b)
        self.assertEqual([lease.worker for lease in leases_b], ["a", "c"])
        self.assertEqual(pool_b.activate(d_booking).worker, "d")
        self.assertEqual(pool_b.available(),
                         {"a": (0, 0), "b": (0, 0), "c": (0, 0), "d": (0, 0)})
        _show("验收3B-开工", pool_b,
              "停接[8,10)，x->a、y->c，旧工作在 b、d 兑现")

        pool_b.advance(8)  # 组窗口结束、容量归还，停接此刻开始。
        self.assertEqual(pool_b.available(),
                         {"a": (2, 2), "b": (2, 2), "c": (2, 2), "d": (2, 2)})
        self.assertIsNone(pool_b.book("stop-start", DC, 8, 9))
        pool_b.advance(10)  # 精确推进到结束点，c 可领同样长度的预约。
        edge = pool_b.book("stop-end", DC, 10, 11)
        self.assertIsNotNone(edge)
        self.assertEqual(edge.worker, "c")
        _show("验收3B-停接结束", pool_b, "tick 10 起 c 恢复接单，容量已全部归还")


class GroupBookingMechanicsTest(TestCase):
    def test_future_promise_is_not_deducted_before_start_and_skipping_invalidates(self):
        pool = _four_worker_pool()
        ticket = pool.book_group((("x", DX), ("y", DY)), 5, 8)
        self.assertIsNotNone(ticket)
        # 未到开工点，未来允诺不是当下扣减。
        self.assertEqual(pool.available(),
                         {"a": (2, 2), "b": (2, 2), "c": (2, 2), "d": (2, 2)})
        # 跳过开工点后未领用：凭据失效且不再约束后来申请。
        pool.advance(8)
        self.assertIsNone(pool.activate_group(ticket))
        self.assertEqual(pool.book("late", DD, 8, 9).worker, "d")

    def test_forged_foreign_and_non_ticket_credentials_are_fenced(self):
        pool = _four_worker_pool()
        ticket = pool.book_group((("x", DX), ("y", DY)), 2, 4)
        forged = GroupTicket(("x", "y"))
        other = _four_worker_pool()
        foreign = other.book_group((("x", DX), ("y", DY)), 2, 4)
        self.assertIsNone(pool.activate_group(forged))
        self.assertIsNone(pool.activate_group(foreign))
        with self.assertRaises(ValueError):
            pool.activate_group(object())

        pool.advance(2)
        leases = pool.activate_group(ticket)
        self.assertIsNotNone(leases)
        # 任一副本提前归还后，整批不得重新开工。
        self.assertTrue(pool.release(leases[0]))
        self.assertIsNone(pool.activate_group(ticket))

    def test_three_replicas_need_three_distinct_domains(self):
        pool = Pool([
            Worker("a", 1, 1, frozenset({"p"})),
            Worker("b", 1, 1, frozenset({"p"})),
            Worker("c", 1, 1, frozenset({"p"})),
            Worker("d", 1, 1, frozenset({"p"})),
        ], fault_domains={"a": "A", "b": "B", "c": "C", "d": "C"})
        replicas = tuple((f"j{i}", Demand(1, 1, frozenset({"p"}))) for i in range(3))
        # 只有三个互异域，三副本可成组。
        ticket = pool.book_group(replicas, 2, 5)
        self.assertIsNotNone(ticket)
        pool.advance(2)
        leases = pool.activate_group(ticket)
        domains = [pool._fault_domains[lease.worker] for lease in leases]
        self.assertEqual(len(set(domains)), 3)

        # 四个副本超出每批 2-3 个的口径。
        with self.assertRaises(ValueError):
            pool.book_group(
                tuple((f"k{i}", Demand(1, 1, frozenset({"p"}))) for i in range(4)),
                6, 7,
            )

    def test_group_count_cap_includes_activated_batches_and_recovers_after_expiry(self):
        pool = _four_worker_pool()
        first = pool.book_group((("x1", DX), ("y1", DY)), 2, 4)
        second = pool.book_group((("x2", DX), ("y2", DY)), 3, 5)
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        # 两批有效成组预约（含未开工）已满，第三批拒绝。
        self.assertIsNone(pool.book_group((("x3", DX), ("y3", DY)), 2, 4))
        pool.advance(3)
        self.assertIsNotNone(pool.activate_group(first))  # 已开工仍计数。
        self.assertIsNone(pool.book_group((("x3", DX), ("y3", DY)), 4, 5))
        pool.advance(5)  # 两批全部结束，名额释放。
        self.assertIsNotNone(pool.book_group((("x3", DX), ("y3", DY)), 6, 8))

    def test_validation_and_scale_limits_have_no_side_effects(self):
        # 未配置故障域时，成组预约是非法用法；停接不依赖故障域。
        plain = Pool([Worker("a", 2, 2), Worker("b", 2, 2)])
        with self.assertRaises(ValueError):
            plain.book_group((("x", DX), ("y", DY)), 2, 4)
        self.assertEqual(plain.schedule_stop("a", 2, 3, 1), (2, 3))

        pool = _four_worker_pool()
        good = (("x", DX), ("y", DY))
        with self.assertRaises(ValueError):
            pool.book_group(good, True, 4)          # bool 不是 int
        with self.assertRaises(ValueError):
            pool.book_group(good, 2, False)
        with self.assertRaises(ValueError):
            pool.book_group(good, 0, 2)             # start 必须严格晚于 now
        with self.assertRaises(ValueError):
            pool.book_group(good, 2, 2)             # start < end
        with self.assertRaises(ValueError):
            pool.book_group(good, 2, 33)            # 结束点晚于 now+32
        with self.assertRaises(ValueError):
            pool.book_group((("x", DX),), 2, 4)    # 每批 2-3 个
        with self.assertRaises(ValueError):
            pool.book_group([("x", DX), ("y", DY)], 2, 4)  # 必须是 tuple
        with self.assertRaises(ValueError):
            pool.book_group((("x", DX), ("x", DY)), 2, 4)  # 批内重名
        occupied = pool.book_group((("dup", DX), ("y", DY)), 2, 4)
        self.assertIsNotNone(occupied)
        with self.assertRaises(ValueError):
            pool.book_group((("x", DX), ("dup", DY)), 5, 6)  # 命名空间重名
        self.assertEqual(len(pool._groups), 1)

        # 超过四台工位时新入口返回 None，旧接口照常工作。
        five = Pool([Worker(f"w{i}", 1, 1) for i in range(5)],
                    fault_domains={f"w{i}": f"D{i}" for i in range(5)})
        self.assertIsNone(five.book_group(
            (("a", Demand(1, 1)), ("b", Demand(1, 1))), 2, 4))
        self.assertIsNone(five.schedule_stop("w0", 2, 3, 1))
        self.assertIsNotNone(five.reserve("ordinary", Demand(1, 1)))

        # 停接参数非法抛 ValueError 且无副作用。
        with self.assertRaises(ValueError):
            pool.schedule_stop("unknown", 2, 3, 1)
        with self.assertRaises(ValueError):
            pool.schedule_stop("c", True, 3, 1)
        with self.assertRaises(ValueError):
            pool.schedule_stop("c", 2, 3, 0)
        with self.assertRaises(ValueError):
            pool.schedule_stop("c", 3, 2, 1)
        with self.assertRaises(ValueError):
            pool.schedule_stop("c", 2, 3, True)
        with self.assertRaises(ValueError):
            pool.schedule_stop("c", 2, 2, 31)   # 结束点越过 now+32
        self.assertEqual(pool._stops, [])


class StopWindowMechanicsTest(TestCase):
    def test_same_worker_windows_cannot_overlap_but_may_touch(self):
        pool = _four_worker_pool()
        self.assertEqual(pool.schedule_stop("c", 2, 6, 2), (2, 4))
        self.assertEqual(pool.schedule_stop("c", 4, 6, 2), (4, 6))  # 端点相接
        # 固定开始点与 [4,6) 重叠：范围内没有合法安排，必须拒绝。
        self.assertIsNone(pool.schedule_stop("c", 5, 5, 1))
        # 四个未结束停接窗口已满，第五个即使不重叠也返回 None。
        self.assertEqual(pool.schedule_stop("c", 6, 6, 1), (6, 7))
        self.assertEqual(pool.schedule_stop("c", 7, 7, 1), (7, 8))
        self.assertIsNone(pool.schedule_stop("c", 8, 8, 1))

    def test_stop_blocks_new_work_but_other_workers_continue(self):
        pool = _four_worker_pool()
        self.assertEqual(pool.schedule_stop("c", 3, 5, 2), (3, 5))
        # 无限期普通租约不能跨进停接窗口：c 被排除，需求落到 d。
        lease = pool.reserve("ordinary", Demand(1, 1, frozenset({"y"})))
        self.assertEqual(lease.worker, "d")
        # 有截止时刻的短作业仍按原规则选取停接之前的可交付连续片段。
        backfill_lease, until = pool.backfill("short", DC, 1, 6)
        self.assertEqual(backfill_lease.worker, "c")
        self.assertEqual(until, 3)
        pool.advance(3)
        self.assertIsNone(pool.book("blocked", DC, 3, 4))
        # 其他工位在同一时刻照常接单（d 带独占标签；普通租约仅占 (1,1)，仍有余量）。
        elsewhere = pool.book(
            "elsewhere", Demand(1, 1, frozenset({"old-d"})), 3, 4,
        )
        self.assertEqual(elsewhere.worker, "d")
        self.assertEqual(pool.available()["d"], (0, 0))
        pool.advance(5)
        self.assertEqual(pool.book("reopened", DC, 5, 6).worker, "c")

    def test_indefinite_lease_already_running_blocks_stop_commitment(self):
        pool = _four_worker_pool()
        lease = pool.reserve("resident", DC)  # c 上的无截止普通租约
        self.assertEqual(lease.worker, "c")
        self.assertIsNone(pool.schedule_stop("c", 2, 4, 1))
        self.assertTrue(pool.release(lease))
        self.assertEqual(pool.schedule_stop("c", 2, 4, 1), (2, 3))

    def test_confirmed_window_never_moves_for_later_requests(self):
        pool = _four_worker_pool()
        self.assertEqual(pool.schedule_stop("c", 2, 8, 2), (2, 4))
        # 后来的申请无法挪动已承诺窗口：与 [2,4) 冲突的安排只能另选时刻或拒绝。
        self.assertEqual(pool.schedule_stop("c", 2, 8, 2), (4, 6))
        self.assertEqual(
            [(s.start, s.end) for s in pool._stops if s.worker == "c"],
            [(2, 4), (4, 6)],
        )


class TimedCompatibilityBoundaryTest(TestCase):
    def test_permits_and_drain_block_new_entries_and_vice_versa(self):
        pool = _four_worker_pool()
        permit = pool.admit("choice", (Demand(1, 1),))
        self.assertIsInstance(permit, Permit)
        with self.assertRaises(RuntimeError):
            pool.book_group((("x", DX), ("y", DY)), 2, 4)
        with self.assertRaises(RuntimeError):
            pool.schedule_stop("c", 2, 4, 1)
        self.assertTrue(pool.cancel_permit(permit))

        # 有效成组预约存在时，admit 不能发新许可，drain 返回 busy 且不闭源。
        ticket = pool.book_group((("x", DX), ("y", DY)), 2, 4)
        self.assertIsNotNone(ticket)
        self.assertIsNone(pool.admit("later", (Demand(1, 1),)))
        result = pool.drain("a", lambda *_: True)
        self.assertEqual(result, {"status": "busy", "moved": {}})
        self.assertIsNotNone(pool.reserve("still-open", Demand(1, 1, frozenset({"x"}))))

        # 停接承诺同样封锁 admit 与 drain。
        pool.advance(4)
        self.assertEqual(pool.schedule_stop("c", 5, 6, 1), (5, 6))
        self.assertIsNone(pool.admit("later", (Demand(1, 1),)))
        self.assertEqual(pool.drain("c", lambda *_: True)["status"], "busy")

    def test_duplicate_job_names_are_rejected_in_shared_namespace(self):
        pool = _four_worker_pool()
        self.assertIsNotNone(pool.book_group((("shared", DX), ("y", DY)), 2, 4))
        with self.assertRaises(ValueError):
            pool.reserve("shared", Demand(1, 1))
        with self.assertRaises(ValueError):
            pool.book("shared", DD, 2, 3)
        with self.assertRaises(ValueError):
            pool.book_group((("shared", DX), ("z", DY)), 5, 6)

    def test_without_new_promises_legacy_behaviour_is_unchanged(self):
        # 不提供 fault_domains 的旧构造方式保持原接口。
        pool = Pool([Worker("a", 2, 2), Worker("b", 2, 2)])
        lease = pool.reserve("job", Demand(1, 1))
        self.assertEqual(lease.worker, "a")
        booking = pool.book("timed", Demand(1, 1), 2, 4)
        self.assertIsInstance(booking, Booking)
        pool.advance(2)
        self.assertIs(pool.activate(booking), pool.current("timed"))
        pool.advance(4)
        self.assertEqual(pool.available(), {"a": (1, 1), "b": (2, 2)})
