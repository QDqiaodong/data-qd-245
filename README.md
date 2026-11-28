# placement：进程内资源池与维护疏散

`placement.Pool` 在单进程内管理若干工位（`Worker`）的 CPU / 内存两类资源，
按标签为作业（job）匹配工位并发放 `Lease`。本模块只做进程内簿记，不接触
容器、网络或持久化；作业的实际准备由调用方完成。

## 数据类型

- `Worker(name, cpu, memory, labels=frozenset())`：工位名、两维容量、标签集合。
- `Demand(cpu, memory, labels=frozenset())`：作业一次申请的两维需求与必需标签。
- `Lease(job, worker)`：绑定凭据。冻结且不参与等值比较，释放时按**对象身份**
  校验，因此同字段的伪造对象、过期的旧凭据都不能归还当前占用。
- `Permit(job)`：一至两种配置的不占工位准入许可。冻结且只接受池当时发出的
  **原对象**领用或取消；详见下文“不指定工位的准入许可”。
- `Booking(job, worker, start, end)`：固定窗口预约成功后返回的不可变凭据；
  激活时同样只接受池当时发出的**原对象**，不接受同字段伪造品。
- `GroupTicket(jobs)`：跨故障域成组预约返回的不可变凭据。只含按副本顺序排列的
  作业名元组，没有工位或时刻；激活时同样按**对象身份**围栏，伪造品、外池凭据和
  已失效凭据都不能领用。
- `Artifact(job)`：发布批处理中间结果返回的不可变凭据。只记生产作业名，真实位置
  与块内容由池内部跟踪；只接受本池当时发出的原对象。

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

## 不指定工位的准入许可

有时作业名已经确定，但具体配置要等上游输入。`admit` 可以先发出一张**不占工位**
的 `Permit`，把一份或两份互斥配置作为选项保留下来：

```python
pool = Pool([
    Worker("a", 4, 4, frozenset({"a-label"})),
    Worker("b", 4, 4, frozenset({"b-label"})),
])
options = (
    Demand(2, 1, frozenset({"a-label"})),  # 选项 0
    Demand(1, 2, frozenset({"b-label"})),  # 选项 1
)
permit = pool.admit("job", options)       # 不扣 available()，current("job") is None

lease = pool.claim(permit, 1)             # 原子选择选项 1，并立即得到真实 Lease
pool.release(lease)                        # 仍用原 release 归还
```

- `options` 必须是长度 1 或 2 的 `tuple[Demand, ...]`，下标从 `0` 开始；
  选项不会被当成两个同时运行的作业，也不会合并各维最大值。
- `Permit` 是只含 `job` 的冻结对象，没有工位或时刻。发证不改变
  `available()` / `current(job)`；只有 `claim` 成功时才把所选配置**完整扣在
  一台工位**上。
- `claim(permit, option)` 只接受本池发出、当前仍有效的**原对象**。伪造、外池、
  已领用或已取消的许可返回 `None`；`cancel_permit(permit)` 对同类失效凭据返回
  `False`，成功取消返回 `True` 并释放作业名及准入约束。
- 对当前有效许可，选项编号必须是真正的 `int` 且在范围内；`True`/`False` 等
  `bool` 编号以及越界编号抛 `ValueError`。

### “可兑现”的准确含义

发证和普通 `reserve` 都会执行精确的递归可行性检查：从受保护的状态开始，
**无论下一位领用人是谁、选择哪个合法选项**，池都必须能立刻选择一台整工位落下；
扣减后，余下许可仍满足同样的保证。检查不假设选型公布顺序、领用顺序，也不假设
在运行作业会先退出。

因此：

- 若只有在“所有选项事先公布”后才存在的静态安排，但某位持证人可能先领用并堵死
  其他人的选项，`admit` 会返回 `None`。
- `reserve` 会逐台试扣普通需求，只在扣后仍通过上述递归保证的工位中，按原
  `(剩余 CPU, 剩余内存, 工位名)` 规则选择；没有安全工位时返回 `None`。
- `claim` 可在多个安全工位中任选；当前实现也沿用相同的剩余量排序。
- `available()` 仍是当下真实余量，未领用许可不预扣。

许可入口的额外规模边界为：池内最多 **6** 台工位，工位 CPU/内存均至多 **64**，
每个选项的每一维也至多 **64**；每池最多 **5** 张未领用有效许可，满额的合法
申请返回 `None`。这些边界不施加给旧 `reserve`。

许可、普通租约和定时条目共用作业名。已有定时条目或 drain 准备过程正在进行时，
参数合法的 `admit` 返回 `None`；存在有效许可时，合法的 `book` / `backfill`
抛 `RuntimeError`，`drain` 返回既有 `busy` 且不会关闭工位。许可没有自动过期
或提前收回规则。所有准入、领用、取消、直接接单和归还都在同一把池锁内原子完成；
两个线程并发领用同一原许可时，只有一个调用会得到真实 `Lease`。

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

## 跨故障域成组预约与工位停接

这两类入口共享一套更小的离线规模边界：池内至多 **4** 台工位，结束点不晚于
`当前时刻 + 32`；至多 **2** 批有效成组预约（含已开工但未结束的批次）和 **4** 个
未结束的停接窗口。超出数量上限的合法请求返回 `None`；参数非法抛 `ValueError`
且无副作用。所有时刻必须是真正的 `int`，`bool` 不被接受。

### 故障域配置

```python
pool = Pool(
    [Worker("a", 4, 4), Worker("b", 4, 4)],
    fault_domains={"a": "rack-A", "b": "rack-B"},
)
```

- `fault_domains` 是工位名到故障域名的**完整映射**：必须恰好覆盖每台工位，域名
  为非空字符串，否则构造抛 `ValueError`。
- 不提供该参数时旧接口原样工作；`book_group` 属于需要故障域的新用法，在未配置的
  池上调用抛 `ValueError`。`schedule_stop` 不依赖故障域。

### 成组预约：book_group / activate_group

```python
ticket = pool.book_group(
    (
        ("replica-x", Demand(2, 2, frozenset({"x-label"}))),
        ("replica-y", Demand(2, 2, frozenset({"y-label"}))),
    ),
    start=5,
    end=8,
)
# 每批 2-3 个 (作业名, Demand) 元组，按返回顺序排列；整批在同一个 [start, end)
# 内运行，每个副本各占一台固定工位，且工位的故障域两两互异。
# 成功返回本池凭据 GroupTicket；不存在整组联合安排时返回 None。

pool.activate_group(ticket)     # now < start：提前领用返回 None 且不消费
pool.advance(5)
leases = pool.activate_group(ticket)
# (Lease("replica-x", ...), Lease("replica-y", ...))，按副本顺序一次性发放
```

- **逐副本都有位置不等于整组可运行。** 选工位是带回溯的联合搜索：每个副本匹配
  标签和两维容量、整段窗口逐刻度可容纳，同时工位互不相同且故障域两两互异；任一
  条件找不到完整联合方案就整组拒绝，不写入任何记录、余额与作业名均不变。
- `start` 必须严格晚于当前时刻（`start > now`），`start < end <= now + 32`。
  未到开工点时未来允诺**不预扣** `available()`；进入窗口后即使尚未领用也已预留
  容量，与旧 `book` 的到窗预留一致。
- 到准确开工 tick 调用 `activate_group(ticket)` 才一次发放整批原 `Lease` 元组；
  任一副本开不了工就一份都不发。提前领用返回 `None` 且不消费；跳过开工点后未
  领用的凭据失效，不再约束后来的申请并释放作业名与批次名额。
- 有效凭据重复领用返回仍有效的**同一批租约对象**；任一副本被 `release` 提前
  归还或到期结束后，整批不再重新开工。伪造、外池或非 `GroupTicket` 对象分别
  返回 `None` 或抛 `ValueError`。
- 成组副本与普通租约、旧 `book`、`backfill`、`Permit` 共用作业名命名空间；
  副本记录同时计入旧的 16 条定时记录上限。

### 工位停接：schedule_stop

```python
window = pool.schedule_stop("c", earliest=5, latest=8, duration=2)
# 在闭区间 [earliest, latest] 内选择一个整 tick 作为开始；
# 成功返回不变元组 (实际开始, 实际结束)（半开区间），否则返回 None。
```

- 承诺窗口内该工位不得有任何尚存的执行或预约占用，也不接新单（`reserve`、
  `book`、`backfill`、成组副本都会绕开或拒绝），但**别的工位照常工作**。
- 同一工位的两个停接窗口不能重叠；区间端点相接允许。已确认的窗口不会因后来的
  申请挪动：范围内存在多个合法开始时刻时任选其一（当前实现取最早可行时刻），
  不考虑尚未提出的请求。
- 普通 `reserve` 没有截止时刻，不能被放到一台已承诺停接的工位上（即使当前
  空闲）；旧的有截止时刻短作业（`book` / `backfill`）仍按原规则选取停接之前
  可交付的连续片段。
- 停接请求会被工位上**无截止时刻的普通租约**阻止（返回 `None`），因为这类租约
  没有可等待的结束点；定时作业到点结束后即可安排。`earliest >= now`、时长为正，
  结束点不晚于 `now + 32`。

### 与旧定时入口的兼容边界

- 尚有未领用 `Permit` 或正在疏散（`drain`）时，不能创建成组预约或停接：沿用旧
  定时入口的口径抛 `RuntimeError`（参数本身非法时仍先抛 `ValueError`）。
- 存在有效成组预约或停接承诺时，`admit` 不能发新许可（返回 `None`），`drain`
  按旧定时工作返回 `busy` 且不关闭源工位。
- 这些新承诺全部结束后，旧预约、回填、许可、疏散行为完全不变。所有新入口都在
  原有池锁内原子完成，不接外部调度器，也不搬动已开工的作业。

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

## 批处理中间结果交接

生产算子可把成品留在原工位供下游本地读取，或送入容量有限的共享交换区供任意
工位读取。启用方式是在构造时给出 `exchange_blocks`：

```python
pool = Pool(workers, exchange_blocks=4)   # 4 块交换容量；0 表示可交接但无交换区
# 不传 exchange_blocks 则交接功能关闭：publish/start_reader/finish_reader 抛
# RuntimeError，artifact_location 返回 None，exchange_used 返回 0。
```

一个块计一单位内存或一单位交换空间，数据是长度 1–64 字节的不可变 `bytes`。

```python
lease = pool.reserve("prod", Demand(1, 4))          # 普通原始租约
artifact = pool.publish(lease, (b"a", b"bb", b"ccc", b"dddd"), ("j", "r"))
# 返回不可伪造的 Artifact；随后原作业解绑、CPU 与未用内存归还，
# 成品块继续占用生产工位内存，旧租约不能再归还它。
pool.artifact_location(artifact)                    # ("local", "prod所在工位") 或 ("exchange", None)

reader, inputs = pool.start_reader("j", Demand(3, 4), (artifact,))
# inputs 是按 artifacts 顺序的块元组，每份内容与发布时完全一致。
pool.finish_reader(reader)                          # True：确认本次借用的全部结果
```

### 发布

- `publish(lease, blocks, readers)` 只接受 `reserve` 发出的**当前原始普通租约**；
  定时、许可领用、读者、迁移产生的租约或已失效租约返回 `None`。
- `blocks`、`readers` 都是元组；读者是 1–4 个互不相同的非空作业名，发布时不占用
  这些名字。单份结果 1–8 块，且不超过生产租约的内存；否则抛 `ValueError`。
- 已存续结果达到 8 份时，合法发布返回 `None`。成品块所在工位的真实内存一直被
  占用，直到被整体送入交换区或所有登记读者确认成功。

### 启动下游

`start_reader(job, demand, artifacts)` 成功返回 `(lease, inputs)`，失败返回
`None` 且不改动任何输入位置、用量或待确认集合。调用必须一次传入该作业目前**全部**
待确认结果（非空、无重复、无伪造/外池/已确认结果），否则返回 `None`；已有同名
有效作业或预约/许可时抛 `ValueError`。租约的两维需求和标签照旧，工作内存额外
扣减，不包含成品块。

要在所选工位上同时读到全部输入：留在该工位本地的结果可直接读取，其余本地的
结果可**整体**转入交换区后读取，交换区的结果可在任意工位读取。规划会用转移与
分配共同生效后的真实状态比较安排，既算上成品占用的内存，也算上转走后真正空出
的容量；多种合法安排可任选，不为尚未提出的请求提前迁移。以下情况返回 `None`：

- 单份结果放得下、但整次启动的交换需求放不下（不逐份单独买单）；
- 某份本地结果正被其他运行中的读者读取，不能挪走，也不在其他工位可读；
- 没有工位能同时容纳作业并读到全部输入。

### 确认或中止

- `finish_reader(lease)` 对当前原始读者租约返回 `True`：归还本次运行占用并确认
  它借用的全部结果。只有最后一个登记读者确认后，结果才注销并归还其所在位置的
  空间。其他租约（含旧运行、伪造物）返回 `False`。
- 旧 `release(lease)` 对读者租约只中止本次运行：归还运行占用、解除本地读取保护，
  不确认结果，原作业名可以重试。同名重试后，迟到的旧租约既不能确认也不能归还
  新的占用。
- `artifact_location` 对已注销或非本池原凭据返回 `None`；`exchange_used()` 返回
  实际占用的交换块数。

### 兼容边界

- 普通 `reserve` 仍用原排序，但会看到本地成品占用的内存，也能使用成品转走后
  空出的容量。新读者属于无结束时刻的普通运行：避开停止接单的工位，也不挤掉
  已发的定时承诺或选型许可；旧入口的新承诺同样不能挤掉现行读者或本地成品。
- 有存续结果时合法的 `drain` 返回旧的 `busy` 且不关源；疏散过程中合法的
  发布和启动返回 `None`。
- 交接入口的规模边界：1–8 台工位、每维至多 64、交换容量 0–64。超出时
  `publish` / `start_reader` 抛 `ValueError`；未启用时两个查询无害地返回空值。

## 滚动租户预算

同一个池可服务多个批处理租户。工位排得下只说明机器有余量；登记作业还要在
任意连续 `width` 个 tick 内控制自己消耗的 CPU·tick 与内存·tick，避免一连串
短作业绕开预算。

```python
pool = Pool(
    [Worker("a", 4, 6, frozenset({"a"})), Worker("b", 4, 6, frozenset({"b"}))],
    tenant_budgets={"batch-team": (4, 8, 11)},   # width, cpu_ticks, memory_ticks
    metered_jobs={"x": "batch-team", "y": "batch-team"},
)

booking = pool.book("x", Demand(2, 3, frozenset({"a"})), 0, 4)
pool.advance(2)
pool.release(pool.activate(booking))
# 两台工位此刻都空着，但最近 4 tick 已被计费 (4, 6)。
```

计费按定时条目**实际占住的区间**累计：对任一整数右端点 `t`，窗口
`[t - width, t)` 内每个被占用的 tick 都计入该作业完整的 `Demand.cpu` 与
`Demand.memory`。未激活的预约同样占住整个承诺窗口；提前 `release` 只放弃从
当前 tick 起的未用部分，已经走过的 tick 继续影响预算直到滑出窗口。
`pool.tenant_usage(tenant)` 返回截至当前逻辑时刻的 `[now - width, now)` 用量，
不把未来承诺算进去；未知租户抛 `ValueError`。

准入口径与上报口径一致：候选区间与同租户**已承诺的作业**落入同一窗口时，两者
的 tick 会一起计费；窗口因此越过预算就拒绝。只有一个候选、窗口里没有其它承诺
时，这一份区间的最后一个 tick 不反过来阻止它自己被接纳——单次区间本身是调用方
明确要求的单位，预算约束的是多个区间在窗口内的堆积。区间一旦准入，之后的
`tenant_usage` 仍按上面的完整窗口口径如实累计。

### 为什么工位空着却会拒绝

```python
pool.book("y", Demand(2, 3, frozenset({"b"})), 2, 5)
# → None：窗口 [0, 4) 内 x 的 tick 0-1 与 y 的 tick 2-3 合计
#   8 CPU·tick、12 内存·tick，超过 11 的内存预算。
#   工位余量与租户已计费量都不变，合法的 book 不会占用作业名。
```

预算按租户独立核算、两维不可互换，已占用额度横跨所有工位；`available()` 仍是
工位真实余量，不会因为租户额度不足而变小。未登记的旧作业不计费，也不受任何
租户预算限制，但仍与登记作业共享工位容量和作业名。

### 调用方怎样重试

`book` 只因为预算超限拒绝时返回 `None`，调用方可以：

1. **后移开始时刻**，让旧用量滑出滚动窗口：

   ```python
   retried = pool.book("y", Demand(2, 3, frozenset({"b"})), 3, 6)
   pool.advance(3)
   lease = pool.activate(retried)      # 到窗口后正常开工
   ```

2. **改用回填**：`backfill` 会在旧约束和预算内交付最长的可行前缀；如果最短
   时长都放不下则返回 `None`，放得下但更长的片段超预算时自动缩短：

   ```python
   lease, until = pool.backfill("y", Demand(2, 2, frozenset({"a"})), 1, 4)
   # 只拿到能在预算内兑现的最长片段，until 是实际截止 tick。
   ```

3. **换一个租户或降低需求**，或推进逻辑时刻后重新尝试。预算拒绝不会写入任何
   预约、租约或计费记录，因此重试是安全的。

具体可运行示例见 `demo_tenant_budget.py`。

### 规模与参数边界

- 每池至多 **8** 个租户、**32** 个登记作业名；租户名、作业名都必须是非空字符串，
  `metered_jobs` 只能指向已配置的租户。
- `width` 是 1–65536 的整数 tick；两维预算各为 0–67108864 的整数。布尔值不算
  整数，越界或类型非法在构造时抛 `ValueError`。省略或传 `None` 等价于空字典，
  构造时会复制字典，之后修改原字典不影响池。
- 登记作业只能通过带租户预算的 `book` / `backfill` 获得运行权。把登记名交给
  `reserve`、`admit`、`book_group`（组内任一登记名也算）或 `start_reader`
  会抛 `ValueError` 且不改变池；未启用预算时这些入口行为完全不变。
- 逻辑时钟可以跳到任意大的非负整数；配额层按事件时间线摘要计算，不会按空闲
  跨度逐 tick 追赶，连续短作业也不会反复扫描整个宽窗口。

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
# 只运行跨故障域成组预约与工位停接的验收场景（含简短中文执行摘要）：
python3 -B -m unittest -v test_group_maintenance
# 只运行批处理中间结果交接的验收场景：
python3 -B -m unittest -v test_artifact_handoff
# 只运行滚动租户预算的验收场景（含简短中文执行摘要）：
python3 -B -m unittest -v test_tenant_budget
# 可运行的预算拒绝与重试示例：
python3 -B demo_tenant_budget.py
```
