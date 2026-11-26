# placement：进程内资源池与维护疏散

`placement.Pool` 在单进程内管理若干工位（`Worker`）的 CPU / 内存两类资源，
按标签为作业（job）匹配工位并发放 `Lease`。本模块只做进程内簿记，不接触
容器、网络或持久化；作业的实际准备由调用方完成。

## 数据类型

- `Worker(name, cpu, memory, labels=frozenset())`：工位名、两维容量、标签集合。
- `Demand(cpu, memory, labels=frozenset())`：作业一次申请的两维需求与必需标签。
- `Lease(job, worker)`：绑定凭据。冻结且不参与等值比较，释放时按**对象身份**
  校验，因此同字段的伪造对象、过期的旧凭据都不能归还当前占用。
- `Booking(job, worker, start, end)`：固定窗口预约成功后返回的不可变凭据；
  激活时同样只接受池当时发出的**原对象**，不接受同字段伪造品。

## 基础入口

```python
pool = Pool([Worker("a", 4, 8, frozenset({"gpu"})), Worker("b", 2, 4)])

lease = pool.reserve("job-1", Demand(1, 2, frozenset({"gpu"})))
# 两维同时满足、标签为超集才成功；按 (剩余cpu, 剩余mem, 工位名) 取最小候选。
# 失败返回 None 且不消耗任何资源；同名重复作业抛 ValueError。

pool.current("job-1")          # 当前生效的原始 Lease 对象；未绑定返回 None
pool.available()               # {工位名: (空闲cpu, 空闲mem)}，含已停止接单的工位
pool.release(lease)            # 仅当前绑定的同一对象能释放，返回 True/False
```

## 逻辑时刻与定时入口

每个池的逻辑时刻从 `0` 开始。调度只使用显式推进的逻辑时刻，不读取系统时钟，
也不会启动真实定时器：

```python
pool.advance(now)
# now 必须是真正的非负 int（bool 不算 int）；允许重复传入当前时刻，
# 传入更小值抛 ValueError。推进是原子的：到达 end 的区间在同一次调用中
# 回收资源、解绑当前 Lease 并释放作业名。跳过窗口不会补发租约。
```

所有预约和回填区间都是左闭右开的 `[start, end)`。

### 固定窗口预约

```python
booking = pool.book(
    "train-night",
    Demand(4, 2, frozenset({"accel"})),
    start=10,
    end=14,
)
# 成功返回不可变、带对象身份语义的 Booking(job, worker, start, end)；
# 整段窗口绑定同一台工位。没有可行工位或记录数达到上限时返回 None。

pool.current("train-night")     # None：预约本身还不是生效租约
pool.available()                # 时刻 < start 时不预扣未来预约

pool.advance(10)
lease = pool.activate(booking)  # 仅原 Booking 对象且 10 <= now < 14 时成功
pool.activate(booking)          # 同一原预约返回同一个 Lease
```

- 选工位时逐刻度检查整个 `[start, end)`，只把同一时刻重叠的占用相加；
  先后衔接、不相交的满容量作业不会被错误地累加到同一时刻。
- 多台工位都能兑现整窗时，取工位名最小者。
- 进入 `[start, end)` 后，即使尚未调用 `activate`，容量也已经预留并体现在
  `available()` 中；激活不会再次扣减。
- 早到、已过 `end`、已提前释放、属于其他 `Pool`、字段相同但不是原对象的
  `Booking`，`activate` 均返回 `None`；传入非 `Booking` 对象抛 `ValueError`。

### 立即回填

```python
lease, until = pool.backfill(
    "lint-batch",
    Demand(2, 1),
    min_ticks=3,
    max_ticks=6,
)
# 成功时立即持有 [now, until)，并返回标准 Lease 与截止时刻 until。
```

`backfill` 从当前时刻寻找连续前缀：

1. 标签必须匹配，工位不能已停止接单；
2. 所选片段至少能连续容纳 `min_ticks`，否则继续找其他合格工位；
3. 在选中的工位上把片段延长到不超过 `max_ticks` 的最长安全位置，绝不破坏
   已有普通租约的长期安全性或任何定时承诺；
4. 多台工位都满足最短时长时，不规定工位偏好；当前实现选择可给片段最长的工位，
   长度相同则取工位名最小，即采用“优先多跑一段”的合法策略。

`until` 一经返回就固定：后续请求只能看到该占用，不能把它提前收回或延后。
调用 `pool.release(lease)` 会立即归还当前容量并放弃剩余片段；否则由之后的
`pool.advance(until)` 到点回收。

### 三类作业共用账本

- 普通 `reserve` 仍没有结束时刻。准入时除了检查当前真实余量，还会确认把这份
  无限期占用加入后，所有未来预约/回填窗口的每个时刻都不超过 CPU、内存容量。
- 因此，当前余量足够不代表长期租约一定可发放；未来承诺会在某一维度造成峰值
  不足的工位被排除。在剩余可选工位中，仍按原来的 `(当前剩余 CPU, 当前剩余内存,
  工位名)` 规则排序。
- 同一个作业名同时只能有一条普通租约、预约或回填记录；尚未激活的未来预约也
  占用作业名。重名抛 `ValueError`。
- `available()` 始终只给当前逻辑时刻的真实剩余量：包含当前已生效占用、已进入
  窗口但未激活的预约以及疏散留位，但不预扣未来才开始的预约。
- `release(lease)` 仍按 `Lease` 对象身份围栏。提前释放已激活的预约或回填会同时
  删除剩余时段承诺；旧 `Booking` 不能重新激活，旧 `Lease` 也不能释放同名重领后的
  新占用。

### 定时入口的规模和参数边界

以下边界只约束 `book` 与 `backfill`，不改变旧 `reserve` / `drain` 之外的旧入口：

- 池内至多 **8** 台工位；
- 每台工位的 CPU、内存容量均至多 **64** 单位；
- `book` 必须满足 `当前时刻 <= start < end <= 当前时刻 + 64`；
- `backfill` 必须满足 `1 <= min_ticks <= max_ticks <= 64`；
- 每池至多 **16** 条未结束的预约和回填记录；已激活预约仍只算一条。达到上限且
  请求在参数和重名规则方面合法时，新请求返回 `None`。
- 作业名、`Demand`、标签规则与 `reserve` 相同；时刻和时长必须是真正的 `int`，
  `bool` 不会被当作整数。类型错误、重名或越界抛 `ValueError`，且不改变池状态。

## 维护疏散：pool.drain(worker, prepare)

在工位 `worker` 下线前，把开始疏散时仍在它上面的活跃作业整批交接给其他工位。

```python
def prepare(old_lease: Lease, target_name: str) -> bool:
    # 同步回调：调用方在此完成目标侧的实际准备（预热、分发文件等）。
    # 回调执行期间不持有池锁，其他线程可照常 reserve/release/current。
    # 返回 True 表示准备完成；返回非 True 或抛出普通 Exception 视为准备失败。
    return True

result = pool.drain("a", prepare)
# {"status": "drained" | "no_capacity" | "prepare_failed" | "busy",
#  "moved": {作业名: 新 Lease}}
```

### 状态语义

| status | 含义 |
| --- | --- |
| `drained` | 没有准备失败，且返回时源上已无活跃绑定（源本来为空也算成功，不调用回调）。 |
| `no_capacity` | 开始时不存在一组完整可行的去处。不调用任何回调，绑定与资源数不变。 |
| `prepare_failed` | 某个回调返回非 `True` 或抛出 `Exception`。已发布的迁移不回滚，未发布的作业保留原绑定。 |
| `busy` | 已有一次疏散在进行中，或池中仍有未结束的定时记录。本次调用直接返回，不会把 `worker` 关闭。 |

`moved` 只记录**本次尝试实际发布过**的新绑定；即使调用方随后释放了该绑定，
记录仍保留。

### 定时承诺与疏散边界

- 只要池中仍有未结束的预约或回填记录，合法的 `drain` 直接返回
  `{"status": "busy", "moved": {}}`；不会关闭源工位，也不会调用 `prepare`。
  定时承诺不到处迁移。
- 一次旧疏散正在执行（`_draining=True`）期间，参数合法的新 `book` /
  `backfill` 请求会抛 `RuntimeError`，且不写入任何记录；参数本身不合法时仍先抛
  `ValueError`。疏散结束后定时入口恢复可用。
- 定时记录到点失效或被提前释放后，池恢复原有普通租约疏散行为。

### 疏散的关键保证

1. **开始即闭源，失败不重开。** 合法尝试一开始就让源停止接收新作业；之后
   无论 `no_capacity` 还是 `prepare_failed`，源都保持关闭。已关闭的工位也
   不能成为其他疏散的目标。
2. **整批留位先于回调。** 先通过回溯搜索找出一组完整可行方案（不存在时才
   返回 `no_capacity`，不会因逐个贪心选错而拒绝可行方案），并在任何回调
   开始前从目标余量中扣住整批份额。普通 `reserve` 只能使用其余合格容量，
   抢不走留位；源在交接生效或原绑定释放前继续占用原资源。
3. **串行回调、锁外执行。** 每个原绑定最多回调一次；轮到它时若原绑定已失效
   则跳过。回调等待不阻塞其他线程的 `reserve` / `release` / `current`。
4. **发布围栏。** 仅当回调返回恰好 `True` **且**原 Lease 仍是当前绑定时，
   才在一个锁步骤内完成：源归还两维资源 → 目标留位转为占用 → 新 Lease 替换
   旧对象。没有"先摘旧绑定"的空窗；迟到的旧凭据无法归还新占用。
5. **并发释放安全。** 回调期间旧绑定被主动释放、甚至同名作业已重新绑定后，
   该回调的任何结论都不会覆盖新绑定、复活旧作业或拖垮整批；它的目标留位
   保留到回调退出，随后立即归还，不泄漏。
6. **失败即收尾。** `prepare_failed` 返回前，所有未转为占用的目标留位都会
   归还。对未排空的源重试时，只处理当时仍在该源上的绑定；已迁走的作业不会
   被再次准备。

### drain 的规模限制

除上文定时入口的限制外，`drain` 还要求尝试开始时源上至多 **6** 个活跃作业；
`worker` 名称未知、`prepare` 不可调用，或超出 drain 规模限制时抛 `ValueError`，
且不改变池状态。

旧的 `reserve` / `release` / `current` / `available` 入口不引入上文的 8 工位、
64 单位或 16 条定时记录限制。

## 典型重试模式

```python
while True:
    result = pool.drain("a", prepare)
    if result["status"] in ("drained", "no_capacity"):
        break
    if result["status"] == "busy":
        continue            # 另一次疏散在跑，稍后重试
    # prepare_failed：修复准备侧问题后重试；已发布的 moved 不会重复准备
```

## 测试

离线运行（90 秒内完成，仅使用 Python 标准库，并发场景用 `Event` / `Barrier`
/ `Thread` 同步，不依赖 sleep 碰运气）：

```bash
python3 -B -m unittest -v
```
