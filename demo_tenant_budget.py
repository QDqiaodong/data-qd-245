"""一个可运行的滚动租户预算示例：工位空着，预约仍可能被拒绝。

运行：python3 -B demo_tenant_budget.py
"""
from placement import Demand, Pool, Worker


def run():
    pool = Pool(
        [
            Worker("a", 4, 6, frozenset({"a"})),
            Worker("b", 4, 6, frozenset({"b"})),
        ],
        tenant_budgets={"batch-team": (4, 8, 11)},
        metered_jobs={"x": "batch-team", "y": "batch-team"},
    )

    # 已登记的作业只能走定时入口。x 在 a 上预约 [0,4)，实际只跑到 tick 2。
    booking = pool.book("x", Demand(2, 3, frozenset({"a"})), 0, 4)
    assert booking is not None
    lease = pool.activate(booking)
    assert lease is not None
    pool.advance(2)
    assert pool.release(lease)

    # 两台工位此刻都没有任何占用，但租户最近 4 tick 已计费 (4, 6)。
    print("可用工位:", pool.available())
    print("租户已计费:", pool.tenant_usage("batch-team"))

    # 新预约 [2,5) 会让窗口 [0,4) 累计到 8 CPU·tick / 12 内存·tick，
    # 超过 11 的内存预算，因此返回 None；工位和租户额度都不变。
    rejected = pool.book("y", Demand(2, 3, frozenset({"b"})), 2, 5)
    print("首次预约:", rejected)
    assert rejected is None
    assert pool.available() == {"a": (4, 6), "b": (4, 6)}

    # 调用方的重试方式：把开始时刻后移到让旧用量滑出窗口的位置。
    retried = pool.book("y", Demand(2, 3, frozenset({"b"})), 3, 6)
    print("后移后重试:", retried)
    assert retried is not None
    pool.advance(3)
    lease = pool.activate(retried)
    print("实际开工:", lease, "可用工位:", pool.available())
    return rejected, retried, lease, pool.available()


if __name__ == "__main__":
    run()
