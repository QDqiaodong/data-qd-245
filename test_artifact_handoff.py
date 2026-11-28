"""批处理中间结果交接的离线验收测试。

所有场景只用进程内簿记和显式逻辑时刻，不依赖真实等待；关键用例用简短中文打印
实际读者工位、成品位置、交换占用和普通接单结果，便于人工核对真实执行而非期望值。
"""
from unittest import TestCase

from placement import Artifact, Demand, Lease, Pool, Worker


def _show(title, pool, *extra):
    print(f"[{title}] 工位余额={pool.available()} 交换={pool.exchange_used()} "
          + " ".join(str(item) for item in extra))


class ScenarioOneSharedResultTest(TestCase):
    def test_two_readers_and_one_ordinary_service(self):
        """验收 1：a 产出四块，j/r 登记；中止、重试、旧租约围栏、本地 vs 交换。"""
        pool = Pool([Worker("a", 4, 8, frozenset({"a-only"})),
                     Worker("b", 4, 8)], exchange_blocks=4)
        producer = pool.reserve("prod", Demand(1, 4, frozenset({"a-only"})))
        self.assertEqual(producer.worker, "a")
        blocks = (b"one", b"two", b"three", b"four")
        artifact = pool.publish(producer, blocks, ("j", "r"))
        self.assertIsInstance(artifact, Artifact)
        # 发布后 a 只剩真实未用内存，旧租约失效。
        self.assertEqual(pool.available(), {"a": (4, 4), "b": (4, 8)})
        self.assertFalse(pool.release(producer))
        self.assertIsNone(pool.current("prod"))
        self.assertEqual(pool.artifact_location(artifact), ("local", "a"))

        # j 可在 a 本地读取，也可走交换区；两种安排都合法。
        reader, inputs = pool.start_reader("j", Demand(3, 4), (artifact,))
        self.assertIsNotNone(reader)
        self.assertEqual(inputs, (blocks,))
        j_worker = reader.worker
        j_location = pool.artifact_location(artifact)
        _show("验收1-j启动", pool, f"j->{j_worker} 位置={j_location}")
        if j_location[0] == "local":
            self.assertEqual(j_worker, "a")

        # 用旧 release 中止 j：归还运行占用，但不确认结果。
        self.assertTrue(pool.release(reader))
        self.assertIsNone(pool.current("j"))
        self.assertEqual(pool.available(), {"a": (4, 4), "b": (4, 8)})
        # 结果仍在原位置可读。
        self.assertEqual(pool.artifact_location(artifact), j_location)

        # 同名重试 j 后成功确认；迟到的旧 j 租约不能确认新运行。
        retry, retry_inputs = pool.start_reader("j", Demand(3, 4), (artifact,))
        self.assertIsNotNone(retry)
        self.assertEqual(retry_inputs, (blocks,))
        self.assertFalse(pool.finish_reader(reader))
        self.assertTrue(pool.finish_reader(retry))
        _show("验收1-j确认", pool, f"位置={pool.artifact_location(artifact)}")

        # 只剩 r 待确认：结果在 a 时 a 专用的 (1,5) 必须失败；
        # 结果在交换区时 a 上的同类服务必须真领到租约。
        location_now = pool.artifact_location(artifact)
        service = pool.reserve("svc", Demand(1, 5, frozenset({"a-only"})))
        if location_now == ("local", "a"):
            self.assertIsNone(service)
        else:
            self.assertIsNotNone(service)
            self.assertEqual(service.worker, "a")
            self.assertTrue(pool.release(service))

        r_reader, r_inputs = pool.start_reader("r", Demand(3, 4), (artifact,))
        self.assertIsNotNone(r_reader)
        self.assertEqual(r_inputs, (blocks,))
        self.assertTrue(pool.finish_reader(r_reader))
        self.assertEqual(pool.available(), {"a": (4, 8), "b": (4, 8)})
        self.assertEqual(pool.exchange_used(), 0)
        self.assertIsNone(pool.artifact_location(artifact))
        _show("验收1-全部归还", pool)

    def test_service_sees_local_blocks_and_freed_exchange(self):
        """验收 1 续：a 专用服务在本地成品前失败，转走后成功。"""
        # 交换上限 3：四块放不下交换区，j 只能走 a 本地。
        pool = Pool([Worker("a", 4, 8, frozenset({"a-only"})),
                     Worker("b", 4, 8)], exchange_blocks=3)
        producer = pool.reserve("prod", Demand(1, 4, frozenset({"a-only"})))
        blocks = (b"1", b"2", b"3", b"4")
        artifact = pool.publish(producer, blocks, ("j", "r"))
        reader, _inputs = pool.start_reader("j", Demand(3, 4, frozenset({"a-only"})),
                                            (artifact,))
        self.assertEqual(reader.worker, "a")
        self.assertTrue(pool.finish_reader(reader))
        self.assertEqual(pool.artifact_location(artifact), ("local", "a"))
        # 四块成品仍占 a 的内存，(1,5) 无法装入。
        self.assertIsNone(pool.reserve("svc", Demand(1, 5, frozenset({"a-only"}))))
        # r 仍在 a 读本地；服务此时无法插队。
        r_reader, _ = pool.start_reader("r", Demand(3, 4, frozenset({"a-only"})),
                                        (artifact,))
        self.assertEqual(r_reader.worker, "a")
        self.assertTrue(pool.finish_reader(r_reader))
        self.assertEqual(pool.available(), {"a": (4, 8), "b": (4, 8)})
        self.assertIsNone(pool.artifact_location(artifact))

    def test_exchange_capacity_and_future_booking_change_j_path(self):
        """验收 1 对照：上限 3 走本地；上限 4 且有 a 的 [5,7) 预约时经交换区到 b。"""
        # 分支 A：上限 3，j 必须走本地 a。
        local_pool = Pool([Worker("a", 4, 8), Worker("b", 4, 8)], exchange_blocks=3)
        producer = local_pool.reserve("prod", Demand(1, 4))
        artifact = local_pool.publish(producer, (b"1", b"2", b"3", b"4"), ("j", "r"))
        reader, _ = local_pool.start_reader("j", Demand(3, 4), (artifact,))
        self.assertEqual(reader.worker, "a")
        self.assertEqual(local_pool.artifact_location(artifact), ("local", "a"))
        _show("验收1-上限3", local_pool, f"j->{reader.worker}")

        # 分支 B：上限 4，a 上另有一条未来预约 [5,7) 占 (2,1)，使 a 无法承载
        # 无限期 j；j 必须把结果整体转入交换区并在 b 启动。
        exchange_pool = Pool([Worker("a", 4, 8), Worker("b", 4, 8)],
                             exchange_blocks=4)
        producer = exchange_pool.reserve("prod", Demand(1, 4))
        artifact = exchange_pool.publish(producer, (b"1", b"2", b"3", b"4"),
                                         ("j", "r"))
        booking = exchange_pool.book("resv", Demand(2, 1), 5, 7)
        self.assertEqual(booking.worker, "a")
        reader, _ = exchange_pool.start_reader("j", Demand(3, 4), (artifact,))
        self.assertIsNotNone(reader)
        self.assertEqual(reader.worker, "b")
        self.assertEqual(exchange_pool.artifact_location(artifact),
                         ("exchange", None))
        self.assertEqual(exchange_pool.exchange_used(), 4)
        _show("验收1-上限4+预约", exchange_pool, f"j->{reader.worker}")

        # 推进到 5：预约真实开工，j 仍有效，两维用量同时体现。
        exchange_pool.advance(5)
        self.assertIs(exchange_pool.current("j"), reader)
        leased = exchange_pool.activate(booking)
        self.assertIsNotNone(leased)
        self.assertEqual(exchange_pool.available(), {"a": (2, 7), "b": (1, 4)})
        _show("验收1-tick5", exchange_pool, f"j={exchange_pool.current('j').worker}")


class ScenarioTwoJointExchangeTest(TestCase):
    def _build(self, capacity):
        pool = Pool([
            Worker("a", 4, 8, frozenset({"pa"})),
            Worker("b", 4, 8, frozenset({"pb"})),
            Worker("c", 4, 8, frozenset({"only-c"})),
        ], exchange_blocks=capacity)
        first = pool.reserve("prod-a", Demand(1, 4, frozenset({"pa"})))
        second = pool.reserve("prod-b", Demand(1, 3, frozenset({"pb"})))
        art_a = pool.publish(first, (b"1", b"2", b"3", b"4"), ("j",))
        art_b = pool.publish(second, (b"x", b"y", b"z"), ("j",))
        return pool, art_a, art_b

    def test_two_inputs_cannot_be_paid_for_separately(self):
        """验收 2：单份都放得下，合计越界则整次拒绝；只加上限即成功。"""
        demand = Demand(1, 2, frozenset({"only-c"}))
        pool, art_a, art_b = self._build(6)
        before = pool.available()
        self.assertIsNone(pool.start_reader("j", demand, (art_a, art_b)))
        # 位置、余额、交换占用、绑定均不变。
        self.assertEqual(pool.available(), before)
        self.assertEqual(pool.exchange_used(), 0)
        self.assertEqual(pool.artifact_location(art_a), ("local", "a"))
        self.assertEqual(pool.artifact_location(art_b), ("local", "b"))
        self.assertIsNone(pool.current("j"))
        _show("验收2-拒绝", pool, "单份 4/3 均<=6，合计 7>6")

        pool, art_a, art_b = self._build(7)
        reader, inputs = pool.start_reader("j", demand, (art_a, art_b))
        self.assertIsNotNone(reader)
        self.assertEqual(reader.worker, "c")
        self.assertEqual(inputs, ((b"1", b"2", b"3", b"4"), (b"x", b"y", b"z")))
        self.assertEqual(pool.exchange_used(), 7)
        self.assertEqual(pool.artifact_location(art_a), ("exchange", None))
        self.assertEqual(pool.artifact_location(art_b), ("exchange", None))
        _show("验收2-成功", pool, f"j->{reader.worker}")

        self.assertTrue(pool.finish_reader(reader))
        self.assertEqual(pool.exchange_used(), 0)
        self.assertEqual(pool.available(), {"a": (4, 8), "b": (4, 8), "c": (4, 8)})
        self.assertIsNone(pool.artifact_location(art_a))


class ScenarioThreeLocalHoldTest(TestCase):
    def test_locally_read_result_cannot_be_moved_away(self):
        """验收 3：r 本地借读时 j 不能挪走；中止 r 后 j 经交换区；r 仍可重试。"""
        pool = Pool([
            Worker("a", 4, 8, frozenset({"a"})),
            Worker("b", 4, 8, frozenset({"b"})),
        ], exchange_blocks=4)
        producer = pool.reserve("prod", Demand(1, 4, frozenset({"a"})))
        blocks = (b"1", b"2", b"3", b"4")
        artifact = pool.publish(producer, blocks, ("r", "j"))

        # 先让一个临时结果占满交换区，确保 r 在无其他压力下确实本地读。
        filler = pool.reserve("filler", Demand(1, 4, frozenset({"b"})))
        filler_art = pool.publish(filler, (b"f", b"g", b"h", b"i"), ("fill",))
        fill_reader, _ = pool.start_reader(
            "fill", Demand(1, 1, frozenset({"a"})), (filler_art,))
        self.assertEqual(fill_reader.worker, "a")
        self.assertEqual(pool.exchange_used(), 4)

        r_reader, r_inputs = pool.start_reader(
            "r", Demand(1, 1, frozenset({"a"})), (artifact,))
        self.assertEqual(r_reader.worker, "a")
        self.assertEqual(pool.artifact_location(artifact), ("local", "a"))
        self.assertEqual(r_inputs, (blocks,))
        # 唯一读者确认，腾空交换区。
        self.assertTrue(pool.finish_reader(fill_reader))
        self.assertEqual(pool.exchange_used(), 0)
        _show("验收3-r本地读", pool, "交换已腾空")

        # 交换容量足够，但 r 正本地借读，只可用 b 的 j 不能挪走这份结果。
        before = pool.available()
        self.assertIsNone(pool.start_reader("j", Demand(1, 1, frozenset({"b"})),
                                            (artifact,)))
        self.assertEqual(pool.available(), before)
        self.assertEqual(pool.artifact_location(artifact), ("local", "a"))
        self.assertEqual(pool.current("r"), r_reader)
        _show("验收3-j被阻", pool, "r 仍本地读取")

        # 中止 r（不确认），j 才能把结果转入交换区并在 b 运行。
        self.assertTrue(pool.release(r_reader))
        j_reader, j_inputs = pool.start_reader(
            "j", Demand(1, 1, frozenset({"b"})), (artifact,))
        self.assertIsNotNone(j_reader)
        self.assertEqual(j_reader.worker, "b")
        self.assertEqual(pool.artifact_location(artifact), ("exchange", None))
        self.assertEqual(j_inputs, (blocks,))
        # j 确认后仍不能注销：r 尚未确认。
        self.assertTrue(pool.finish_reader(j_reader))
        self.assertEqual(pool.artifact_location(artifact), ("exchange", None))
        self.assertEqual(pool.exchange_used(), 4)
        _show("验收3-j确认", pool, "r 仍待确认")

        # r 重试读到相同块并确认，随后空间全部回收。
        r_retry, r_retry_inputs = pool.start_reader(
            "r", Demand(1, 1, frozenset({"a"})), (artifact,))
        self.assertEqual(r_retry_inputs, (blocks,))
        self.assertTrue(pool.finish_reader(r_retry))
        self.assertEqual(pool.exchange_used(), 0)
        self.assertEqual(pool.available(), {"a": (4, 8), "b": (4, 8)})
        self.assertIsNone(pool.artifact_location(artifact))
        _show("验收3-全部回收", pool)


class ArtifactValidationTest(TestCase):
    def test_disabled_pool_new_entries_raise_and_queries_are_empty(self):
        pool = Pool([Worker("a", 4, 8)])
        lease = pool.reserve("prod", Demand(1, 4))
        with self.assertRaises(RuntimeError):
            pool.publish(lease, (b"a",), ("j",))
        with self.assertRaises(RuntimeError):
            pool.start_reader("j", Demand(1, 1), (Artifact("prod"),))
        with self.assertRaises(RuntimeError):
            pool.finish_reader(lease)
        self.assertIsNone(pool.artifact_location(Artifact("prod")))
        self.assertEqual(pool.exchange_used(), 0)
        # 旧的 reserve 不受影响。
        self.assertIs(pool.current("prod"), lease)

    def test_exchange_blocks_type_and_range(self):
        with self.assertRaises(ValueError):
            Pool([Worker("a", 1, 1)], exchange_blocks=True)
        with self.assertRaises(ValueError):
            Pool([Worker("a", 1, 1)], exchange_blocks=-1)
        with self.assertRaises(ValueError):
            Pool([Worker("a", 1, 1)], exchange_blocks=65)
        # 0 是启用但无交换容量。
        zero = Pool([Worker("a", 4, 4)], exchange_blocks=0)
        lease = zero.reserve("prod", Demand(1, 4))
        art = zero.publish(lease, (b"abcd",), ("j",))
        self.assertEqual(zero.artifact_location(art), ("local", "a"))

    def test_publish_format_and_limits(self):
        pool = Pool([Worker("a", 4, 8)], exchange_blocks=4)
        lease = pool.reserve("prod", Demand(4, 8))
        bad_cases = [
            ((), ("j",)),                       # 空 blocks
            ((b"x" * 65,), ("j",)),             # 块超过 64 字节
            ((b"",), ("j",)),                   # 空块
            (("str",), ("j",)),                 # 非 bytes
            (([b"x"],), ("j",)),                # blocks 非 tuple
            ((b"x",), ()),                      # 空 readers
            ((b"x",), ("j", "j")),              # 重复读者
            ((b"x",), ("",)),                   # 空读者名
            ((b"x",) * 9, ("j",)),              # 超过 8 块
            ((b"x",), "j"),                     # readers 非 tuple
        ]
        for blocks, readers in bad_cases:
            with self.assertRaises(ValueError):
                pool.publish(lease, blocks, readers)
        # 超过生产租约内存。
        small = Pool([Worker("a", 4, 4)], exchange_blocks=4)
        small_lease = small.reserve("prod", Demand(1, 2))
        with self.assertRaises(ValueError):
            small.publish(small_lease, (b"1", b"2", b"3"), ("j",))
        # 合法的失败发布都没有改状态。
        self.assertIs(pool.current("prod"), lease)
        self.assertEqual(pool.exchange_used(), 0)

    def test_publish_only_accepts_current_raw_reserve_lease(self):
        pool = Pool([Worker("a", 4, 8), Worker("b", 4, 8)], exchange_blocks=4)
        # 定时租约不可发布。
        booking = pool.book("timed", Demand(1, 2), 0, 5)
        timed_lease = pool.activate(booking)
        self.assertIsNone(pool.publish(timed_lease, (b"x",), ("j",)))
        # 许可领用租约不可发布（单独一池，避免定时记录挡住 admit）。
        permit_pool = Pool([Worker("a", 4, 8)], exchange_blocks=4)
        permit = permit_pool.admit("permit-job", (Demand(1, 1),))
        claimed = permit_pool.claim(permit, 0)
        self.assertIsNone(permit_pool.publish(claimed, (b"x",), ("j",)))
        # 迁移后的新租约不可发布。
        moved_pool = Pool([Worker("a", 4, 8, frozenset({"k"})),
                           Worker("b", 4, 8, frozenset({"k"}))], exchange_blocks=4)
        original = moved_pool.reserve("mig", Demand(1, 1, frozenset({"k"})))
        result = moved_pool.drain("a", lambda *_args: True)
        new_lease = result["moved"]["mig"]
        self.assertIsNone(moved_pool.publish(new_lease, (b"x",), ("j",)))
        # 已失效的旧租约不可发布。
        stale_pool = Pool([Worker("a", 4, 8)], exchange_blocks=4)
        old = stale_pool.reserve("job", Demand(1, 1))
        stale_pool.release(old)
        fresh = stale_pool.reserve("job", Demand(1, 1))
        self.assertIsNone(stale_pool.publish(old, (b"x",), ("j",)))
        self.assertIs(stale_pool.current("job"), fresh)

    def test_start_reader_format_authorization_and_completeness(self):
        pool = Pool([Worker("a", 4, 8), Worker("b", 4, 8)], exchange_blocks=8)
        lease = pool.reserve("prod", Demand(1, 4))
        artifact = pool.publish(lease, (b"1", b"2", b"3", b"4"), ("j", "r"))
        lease2 = pool.reserve("prod2", Demand(1, 4))
        artifact2 = pool.publish(lease2, (b"a", b"b"), ("j",))

        with self.assertRaises(ValueError):
            pool.start_reader("bad", Demand(1, 1), ())
        with self.assertRaises(ValueError):
            pool.start_reader("bad", Demand(1, 1), (artifact, artifact))
        with self.assertRaises(ValueError):
            pool.start_reader("bad", Demand(1, 1), (object(),))
        with self.assertRaises(ValueError):
            pool.start_reader("bad", object(), (artifact,))

        # 未传齐 j 的两份待确认结果。
        self.assertIsNone(pool.start_reader("j", Demand(1, 1), (artifact,)))
        # 无权的读者名。
        self.assertIsNone(pool.start_reader("zz", Demand(1, 1),
                                            (artifact, artifact2)))
        # 伪造凭据。
        self.assertIsNone(pool.start_reader("j", Demand(1, 1),
                                            (Artifact("prod"), artifact2)))
        # 外池凭据。
        other = Pool([Worker("a", 4, 8)], exchange_blocks=4)
        foreign = other.publish(other.reserve("prod", Demand(1, 1)), (b"z",), ("j",))
        self.assertIsNone(pool.start_reader("j", Demand(1, 1), (foreign, artifact2)))

        # 正常启动后，同名作业再领输入被拒绝。
        reader, _ = pool.start_reader("j", Demand(1, 1), (artifact, artifact2))
        self.assertIsNotNone(reader)
        with self.assertRaises(ValueError):
            pool.start_reader("j", Demand(1, 1), (artifact, artifact2))

    def test_fabricated_confirmation_is_fenced(self):
        pool = Pool([Worker("a", 4, 8), Worker("b", 4, 8)], exchange_blocks=4)
        lease = pool.reserve("prod", Demand(1, 4))
        artifact = pool.publish(lease, (b"1", b"2", b"3", b"4"), ("j", "r"))
        reader, _ = pool.start_reader("j", Demand(1, 1), (artifact,))
        self.assertFalse(pool.finish_reader(lease))          # 生产租约
        self.assertFalse(pool.finish_reader(Lease("j", reader.worker)))
        self.assertFalse(pool.finish_reader(Lease("prod", "a")))
        # 释放后旧读者租约也不能再确认或归还新占用。
        self.assertTrue(pool.release(reader))
        retry, _ = pool.start_reader("j", Demand(1, 1), (artifact,))
        self.assertFalse(pool.finish_reader(reader))
        self.assertFalse(pool.release(reader))
        self.assertIs(pool.current("j"), retry)

    def test_artifact_slot_cap_returns_none(self):
        pool = Pool([Worker("a", 64, 64)], exchange_blocks=64)
        artifacts = []
        for index in range(8):
            lease = pool.reserve(f"p{index}", Demand(1, 1))
            artifact = pool.publish(lease, (b"x",), (f"r{index}",))
            self.assertIsNotNone(artifact)
            artifacts.append(artifact)
        ninth = pool.reserve("p8", Demand(1, 1))
        self.assertIsNone(pool.publish(ninth, (b"x",), ("r8",)))
        # 拒绝时生产租约仍是当前绑定。
        self.assertIs(pool.current("p8"), ninth)

    def test_drain_is_busy_while_results_exist(self):
        pool = Pool([Worker("a", 4, 8, frozenset({"k"})),
                     Worker("b", 4, 8, frozenset({"k"}))], exchange_blocks=4)
        lease = pool.reserve("prod", Demand(1, 4, frozenset({"k"})))
        pool.publish(lease, (b"1", b"2", b"3", b"4"), ("j",))
        result = pool.drain("a", lambda *_args: True)
        self.assertEqual(result, {"status": "busy", "moved": {}})

    def test_scale_limits_raise_value_error(self):
        many = Pool([Worker(f"w{i}", 1, 1) for i in range(9)], exchange_blocks=4)
        lease = many.reserve("prod", Demand(1, 1))
        with self.assertRaises(ValueError):
            many.publish(lease, (b"x",), ("j",))
        with self.assertRaises(ValueError):
            many.start_reader("j", Demand(1, 1), (Artifact("prod"),))


class ArtifactLifecycleTest(TestCase):
    def test_reader_work_memory_does_not_include_artifact_blocks(self):
        pool = Pool([Worker("a", 4, 8)], exchange_blocks=4)
        lease = pool.reserve("prod", Demand(1, 4))
        artifact = pool.publish(lease, (b"1", b"2", b"3", b"4"), ("j",))
        # 发布后 a 可用 (4,4)：四块成品占内存。读者 demand 的 CPU/内存照旧，
        # 但工作内存不额外包含成品块。
        self.assertEqual(pool.available(), {"a": (4, 4)})
        reader, _ = pool.start_reader("j", Demand(1, 4), (artifact,))
        self.assertIsNotNone(reader)
        self.assertEqual(pool.available(), {"a": (3, 0)})
        self.assertTrue(pool.finish_reader(reader))
        # j 是唯一登记读者，确认后成品注销、整块内存归还。
        self.assertEqual(pool.available(), {"a": (4, 8)})
        self.assertIsNone(pool.artifact_location(artifact))

    def test_aborted_reader_can_retry_without_confirming(self):
        pool = Pool([Worker("a", 4, 8)], exchange_blocks=4)
        lease = pool.reserve("prod", Demand(1, 4))
        artifact = pool.publish(lease, (b"1", b"2", b"3", b"4"), ("j", "r"))
        reader, _ = pool.start_reader("j", Demand(1, 1), (artifact,))
        self.assertTrue(pool.release(reader))
        # 结果未确认，r 仍可读；j 也能重试。
        self.assertEqual(pool.artifact_location(artifact), ("local", "a"))
        retry, _ = pool.start_reader("j", Demand(1, 1), (artifact,))
        self.assertIsNotNone(retry)
        self.assertTrue(pool.finish_reader(retry))
        # j 已确认，r 仍未确认：结果保留。
        self.assertEqual(pool.artifact_location(artifact), ("local", "a"))
        r_reader, _ = pool.start_reader("r", Demand(1, 1), (artifact,))
        self.assertTrue(pool.finish_reader(r_reader))
        self.assertIsNone(pool.artifact_location(artifact))
