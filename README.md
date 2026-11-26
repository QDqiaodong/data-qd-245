# placement：进程内资源池与维护疏散

`placement.Pool` 在单进程内管理若干工位（`Worker`）的 CPU / 内存两类资源，
按标签为作业（job）匹配工位并发放 `Lease`。本模块只做进程内簿记，不接触
容器、网络或持久化；作业的实际准备由调用方完成。

## 数据类型

- `Worker(name, cpu, memory, labels=frozenset())`：工位名、两维容量、标签集合。
- `Demand(cpu, memory, labels=frozenset())`：作业一次申请的两维需求与必需标签。
- `Lease(job, worker)`：绑定凭据。冻结且不参与等值比较，释放时按**对象身份**
  校验，因此同字段的伪造对象、过期的旧凭据都不能归还当前占用。

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
| `busy` | 已有一次疏散在进行中。本次调用直接返回，不会把 `worker` 关闭。 |

`moved` 只记录**本次尝试实际发布过**的新绑定；即使调用方随后释放了该绑定，
记录仍保留。

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

### 规模限制（仅作用于 drain 入口）

- 池内最多 **8** 台工位；
- 每台工位每个资源维度至多 **64** 单位；
- 尝试开始时源上至多 **6** 个活跃作业；
- `worker` 名称未知、`prepare` 不可调用，或超出上述限制时抛 `ValueError`，
  且不改变池状态。

`reserve` / `release` / `current` / `available` 不受这些规模限制约束。

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
