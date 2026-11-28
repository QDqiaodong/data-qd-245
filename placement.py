"""Resource placement and fenced release of in-process worker reservations."""
from dataclasses import dataclass
from threading import Lock


DRAIN_WORKER_LIMIT = 8
DRAIN_DIMENSION_LIMIT = 64
DRAIN_SOURCE_JOB_LIMIT = 6

# Timed admission entries have deliberately small, offline-testable bounds.
TIMED_WORKER_LIMIT = 8
TIMED_DIMENSION_LIMIT = 64
TIMED_HORIZON_LIMIT = 64
MAX_TIMED_RECORDS = 16

# Flexible permits have an additional, smaller admission envelope.
PERMIT_WORKER_LIMIT = 6
PERMIT_DIMENSION_LIMIT = 64
MAX_PERMITS = 5

# Fault-domain group bookings and maintenance stop windows share one small,
# offline-testable envelope.
GROUP_WORKER_LIMIT = 4
GROUP_REPLICA_MIN = 2
GROUP_REPLICA_MAX = 3
MAX_GROUP_BOOKINGS = 2
MAX_STOP_WINDOWS = 4
GROUP_HORIZON_LIMIT = 32

# Batch-result handoff has its own small, offline-testable envelope.
ARTIFACT_WORKER_LIMIT = 8
ARTIFACT_DIMENSION_LIMIT = 64
ARTIFACT_EXCHANGE_LIMIT = 64
MAX_ARTIFACTS = 8
MAX_ARTIFACT_READERS = 4
MAX_READER_INPUTS = 4
BLOCKS_PER_ARTIFACT_MIN = 1
BLOCKS_PER_ARTIFACT_MAX = 8
BLOCK_BYTES_MIN = 1
BLOCK_BYTES_MAX = 64

# Rolling per-tenant budgets cover registered batch jobs only.
TENANT_LIMIT = 8
METERED_JOB_LIMIT = 32
TENANT_WINDOW_MIN = 1
TENANT_WINDOW_MAX = 65536
TENANT_BUDGET_MAX = 67108864


def _labels(labels):
    return type(labels) is frozenset and all(type(label) is str and label for label in labels)


@dataclass(frozen=True)
class Worker:
    name: str
    cpu: int
    memory: int
    labels: frozenset = frozenset()

    def __post_init__(self):
        if (type(self.name) is not str or not self.name or not _labels(self.labels)
                or any(type(n) is not int or n <= 0 for n in (self.cpu, self.memory))):
            raise ValueError("invalid worker")


@dataclass(frozen=True)
class Demand:
    cpu: int
    memory: int
    labels: frozenset = frozenset()

    def __post_init__(self):
        if (not _labels(self.labels)
                or any(type(n) is not int or n <= 0 for n in (self.cpu, self.memory))):
            raise ValueError("invalid demand")


@dataclass(frozen=True, eq=False)
class Lease:
    job: str
    worker: str


@dataclass(frozen=True)
class Permit:
    """Immutable, worker-less admission choice.

    A permit reserves no real capacity.  The pool accepts a claim only for the
    exact original object; an equal-valued or forged ``Permit`` is not enough.
    """

    job: str


@dataclass(frozen=True)
class Booking:
    """Immutable credential for a future fixed-window placement."""

    job: str
    worker: str
    start: int
    end: int


@dataclass(frozen=True)
class GroupTicket:
    """Immutable credential for one fault-domain-separated replica batch.

    Only the exact object issued by the pool can be activated; an equal-valued
    or forged ticket never matches a live group.
    """

    jobs: tuple


@dataclass(frozen=True, eq=False)
class Artifact:
    """Immutable credential for one published batch result.

    A pool accepts an artifact only as the exact original object it issued.
    Equal-valued or forged artifacts never match a live result, and the fields
    carry no mutable location: the pool tracks where the blocks really live.
    """

    job: str


class _ArtifactRecord:
    """One live published result and its pending reader confirmations."""

    __slots__ = ("artifact", "job", "blocks", "readers", "pending",
                 "worker", "in_exchange")

    def __init__(self, artifact, job, blocks, readers, worker):
        self.artifact = artifact
        self.job = job
        self.blocks = blocks
        # Ordered, unique reader job names that may still confirm this result.
        self.readers = readers
        # Reader names that have not yet confirmed successfully.
        self.pending = set(readers)
        # Worker whose memory currently carries the blocks, or None when the
        # result has been moved into the shared exchange area.
        self.worker = worker
        self.in_exchange = worker is None


class _ReaderRecord:
    """One running downstream run borrowing artifacts plus local read holds."""

    __slots__ = ("lease", "inputs", "local_holds", "artifact_records")

    def __init__(self, lease, inputs, local_holds, artifact_records):
        self.lease = lease
        self.inputs = inputs
        # artifact -> producing worker name whose local blocks are read-held.
        self.local_holds = local_holds
        # The live artifact records borrowed by this run, in request order.
        self.artifact_records = artifact_records


class _TimedRecord:
    """One live booking, backfill or group-replica interval.

    The pool lock guards every instance and the ``holding`` flag records whether
    the interval's demand is currently deducted from ``Pool._free``.
    """

    __slots__ = (
        "kind", "job", "demand", "worker", "start", "end",
        "booking", "lease", "holding", "group", "tenant",
    )

    def __init__(self, kind, job, demand, worker, start, end,
                 booking=None, lease=None, holding=False, group=None,
                 tenant=None):
        self.kind = kind
        self.job = job
        self.demand = demand
        self.worker = worker
        self.start = start
        self.end = end
        self.booking = booking
        self.lease = lease
        self.holding = holding
        self.group = group
        # Tenant name when this interval is a registered metered job.
        self.tenant = tenant


class _TimelineNode:
    """One change point in a tenant's per-tick billing rate."""

    __slots__ = ("key", "priority", "delta_cpu", "delta_memory",
                 "left", "right", "sum_cpu", "sum_memory",
                 "weighted_cpu", "weighted_memory")

    def __init__(self, key, delta_cpu, delta_memory, priority):
        self.key = key
        self.priority = priority
        self.delta_cpu = delta_cpu
        self.delta_memory = delta_memory
        self.left = None
        self.right = None
        self.sum_cpu = delta_cpu
        self.sum_memory = delta_memory
        self.weighted_cpu = key * delta_cpu
        self.weighted_memory = key * delta_memory


def _node_refresh(node):
    left = node.left
    right = node.right
    sum_cpu = node.delta_cpu
    sum_memory = node.delta_memory
    weighted_cpu = node.key * node.delta_cpu
    weighted_memory = node.key * node.delta_memory
    if left is not None:
        sum_cpu += left.sum_cpu
        sum_memory += left.sum_memory
        weighted_cpu += left.weighted_cpu
        weighted_memory += left.weighted_memory
    if right is not None:
        sum_cpu += right.sum_cpu
        sum_memory += right.sum_memory
        weighted_cpu += right.weighted_cpu
        weighted_memory += right.weighted_memory
    node.sum_cpu = sum_cpu
    node.sum_memory = sum_memory
    node.weighted_cpu = weighted_cpu
    node.weighted_memory = weighted_memory


def _rotate_right(node):
    pivot = node.left
    node.left = pivot.right
    pivot.right = node
    _node_refresh(node)
    _node_refresh(pivot)
    return pivot


def _rotate_left(node):
    pivot = node.right
    node.right = pivot.left
    pivot.left = node
    _node_refresh(node)
    _node_refresh(pivot)
    return pivot


def _timeline_add(node, key, delta_cpu, delta_memory, priority):
    if node is None:
        return _TimelineNode(key, delta_cpu, delta_memory, priority)
    if key == node.key:
        node.delta_cpu += delta_cpu
        node.delta_memory += delta_memory
    elif key < node.key:
        node.left = _timeline_add(
            node.left, key, delta_cpu, delta_memory, priority)
        if node.left.priority < node.priority:
            node = _rotate_right(node)
            return node
    else:
        node.right = _timeline_add(
            node.right, key, delta_cpu, delta_memory, priority)
        if node.right.priority < node.priority:
            node = _rotate_left(node)
            return node
    _node_refresh(node)
    return node


def _timeline_prefix(node, key):
    """Return aggregate deltas at strictly smaller keys.

    The result is ``(sum_cpu, sum_memory, weighted_cpu, weighted_memory)``
    where weighted values are the corresponding sums multiplied by the key.
    """
    sum_cpu = sum_memory = weighted_cpu = weighted_memory = 0
    while node is not None:
        if node.key < key:
            left = node.left
            if left is not None:
                sum_cpu += left.sum_cpu
                sum_memory += left.sum_memory
                weighted_cpu += left.weighted_cpu
                weighted_memory += left.weighted_memory
            sum_cpu += node.delta_cpu
            sum_memory += node.delta_memory
            weighted_cpu += node.key * node.delta_cpu
            weighted_memory += node.key * node.delta_memory
            node = node.right
        else:
            node = node.left
    return sum_cpu, sum_memory, weighted_cpu, weighted_memory


class _TenantRecord:
    """One tenant's rolling billing profile.

    The timeline holds rate-change events: a committed box ``[start, end)``
    with per-tick ``(cpu, memory)`` contributes ``+d`` at ``start`` and
    ``-d`` at ``end``.  The cumulative area up to tick ``x`` is then
    ``x * sum(delta) - sum(key * delta)`` over events with ``key < x``; the
    rolling window ``[x - width, x)`` is the difference of two such queries.
    An event tree gives logarithmic updates and lookups for any clock value
    and width, with no per-tick catch-up.
    """

    __slots__ = ("width", "cpu_budget", "memory_budget", "root", "rng")

    def __init__(self, width, cpu_budget, memory_budget):
        self.width = width
        self.cpu_budget = cpu_budget
        self.memory_budget = memory_budget
        self.root = None
        self.rng = (0x9E3779B97F4A7C15 + width * 2654435761 + cpu_budget
                    + (memory_budget << 7)) & ((1 << 64) - 1)

    def _next_priority(self):
        self.rng = (self.rng * 6364136223846793005 + 1442695040888963407) \
            & ((1 << 64) - 1)
        return self.rng

    def add_box(self, start, end, cpu, memory):
        """Bill ``cpu``/``memory`` per tick over the half-open ``[start,end)``."""
        if start >= end:
            return
        self.root = _timeline_add(
            self.root, start, cpu, memory, self._next_priority())
        self.root = _timeline_add(
            self.root, end, -cpu, -memory, self._next_priority())

    def billed(self, x):
        """CPU·tick and memory·tick consumed in ``[0, x)``."""
        if x <= 0:
            return (0, 0)
        sum_cpu, sum_memory, weighted_cpu, weighted_memory = \
            _timeline_prefix(self.root, x)
        return (x * sum_cpu - weighted_cpu, x * sum_memory - weighted_memory)

    def window(self, now):
        """CPU·tick and memory·tick consumed in ``[now - width, now)``."""
        high_cpu, high_memory = self.billed(now)
        low_cpu, low_memory = self.billed(now - self.width)
        return (high_cpu - low_cpu, high_memory - low_memory)


class _GroupRecord:
    """One accepted replica batch and its live per-replica timed records."""

    __slots__ = ("ticket", "replicas", "start", "end", "leases")

    def __init__(self, ticket, replicas, start, end):
        self.ticket = ticket
        # Replica records in the same order as the book_group argument.
        self.replicas = replicas
        self.start = start
        self.end = end
        # Lease objects in replica order, filled at the first activation.
        self.leases = None


class _StopRecord:
    """One committed worker maintenance window ``[start, end)``."""

    __slots__ = ("worker", "start", "end")

    def __init__(self, worker, start, end):
        self.worker = worker
        self.start = start
        self.end = end


class _PermitRecord:
    """One unclaimed permit and its one or two whole-demand options."""

    __slots__ = ("permit", "job", "options")

    def __init__(self, permit, job, options):
        self.permit = permit
        self.job = job
        self.options = options


class Pool:
    def __init__(self, workers, *, fault_domains=None, exchange_blocks=None,
                 tenant_budgets=None, metered_jobs=None):
        self._workers = {}
        self._free = {}
        for worker in workers:
            if worker.name in self._workers:
                raise ValueError("duplicate worker")
            self._workers[worker.name] = worker
            self._free[worker.name] = (worker.cpu, worker.memory)
        # Batch-result handoff is opt-in.  ``None`` disables it entirely;
        # a real integer (including 0) enables it with that exchange capacity.
        self._handoff_enabled = exchange_blocks is not None
        self._exchange_capacity = 0
        if exchange_blocks is not None:
            if type(exchange_blocks) is not int:
                raise ValueError("exchange_blocks must be an integer")
            if not 0 <= exchange_blocks <= ARTIFACT_EXCHANGE_LIMIT:
                raise ValueError("exchange_blocks is outside the supported range")
            self._exchange_capacity = exchange_blocks
        # Optional complete worker-name -> fault-domain-name mapping.  When
        # present it enables group bookings; every name must be a non-empty
        # string and every worker must be covered exactly once.
        self._fault_domains = None
        if fault_domains is not None:
            if not isinstance(fault_domains, dict):
                raise ValueError("fault_domains must be a mapping")
            if set(fault_domains) != set(self._workers):
                raise ValueError("fault_domains must cover every worker exactly once")
            if any(type(domain) is not str or not domain
                   for domain in fault_domains.values()):
                raise ValueError("fault domain names must be non-empty strings")
            self._fault_domains = dict(fault_domains)
        # Optional rolling tenant budgets.  Copies are taken here so later
        # caller mutation of the original mappings cannot change billing.
        self._tenants = self._build_tenants_locked(tenant_budgets)
        self._metered = self._build_metered_locked(metered_jobs, self._tenants)
        self._active = {}
        self._lock = Lock()
        # Workers that no longer accept new jobs (sources of drain attempts).
        self._closed = set()
        # Exactly one drain attempt may run at a time.
        self._draining = False
        # Logical time, half-open interval scheduling state, and aggregate
        # ordinary/drain-hold usage used by future feasibility checks.
        self._now = 0
        self._ordinary_use = {name: [0, 0] for name in self._workers}
        self._hold_use = {name: [0, 0] for name in self._workers}
        self._timed = []
        self._timed_by_job = {}
        # Unclaimed flexible permits.  _permits is the authoritative live set;
        # _permit_by_job enforces the shared job-name namespace.
        self._permits = []
        self._permit_by_job = {}
        # Live replica batches (including activated ones until every replica
        # interval ends) and committed maintenance windows.
        self._groups = []
        self._stops = []
        # Live published artifacts by original credential.  Running readers
        # are keyed by their current original lease object.
        self._artifacts = {}
        self._readers = {}
        # Leases issued by ``reserve`` and still current; only these may
        # publish a batch result.
        self._reserve_leases = set()
        # Blocks currently occupying the shared exchange area.
        self._exchange_used = 0

    # ------------------------------------------------------------------
    # Rolling tenant budgets: configuration and billing primitives.
    # ------------------------------------------------------------------

    @staticmethod
    def _build_tenants_locked(tenant_budgets):
        if tenant_budgets is None:
            return {}
        if not isinstance(tenant_budgets, dict):
            raise ValueError("tenant_budgets must be a mapping")
        if len(tenant_budgets) > TENANT_LIMIT:
            raise ValueError("too many tenants")
        tenants = {}
        for tenant, profile in tenant_budgets.items():
            if type(tenant) is not str or not tenant:
                raise ValueError("tenant names must be non-empty strings")
            if not isinstance(profile, tuple) or len(profile) != 3:
                raise ValueError("each tenant budget is (width, cpu_ticks, memory_ticks)")
            width, cpu_ticks, memory_ticks = profile
            if (any(type(value) is not int
                    for value in (width, cpu_ticks, memory_ticks))
                    or not TENANT_WINDOW_MIN <= width <= TENANT_WINDOW_MAX
                    or not 0 <= cpu_ticks <= TENANT_BUDGET_MAX
                    or not 0 <= memory_ticks <= TENANT_BUDGET_MAX):
                raise ValueError("tenant budget values are out of range")
            tenants[tenant] = _TenantRecord(width, cpu_ticks, memory_ticks)
        return tenants

    @staticmethod
    def _build_metered_locked(metered_jobs, tenants):
        if metered_jobs is None:
            return {}
        if not isinstance(metered_jobs, dict):
            raise ValueError("metered_jobs must be a mapping")
        if len(metered_jobs) > METERED_JOB_LIMIT:
            raise ValueError("too many metered jobs")
        metered = {}
        for job, tenant in metered_jobs.items():
            if type(job) is not str or not job:
                raise ValueError("metered job names must be non-empty strings")
            if type(tenant) is not str or not tenant or tenant not in tenants:
                raise ValueError("metered job names an unknown tenant")
            metered[job] = tenant
        return metered

    def tenant_usage(self, tenant):
        """Billed (cpu_ticks, memory_ticks) inside [now - width, now), or raise."""
        with self._lock:
            if type(tenant) is not str or tenant not in self._tenants:
                raise ValueError("unknown tenant")
            return self._tenants[tenant].window(self._now)

    def _fits_budget_locked(self, tenant, start, end, demand):
        """Whether one candidate interval fits the tenant's rolling windows.

        Windows are examined at each tick ``t`` after the candidate's start
        through the committed horizon.  At such a ``t`` the window
        ``[t - width, t)`` holds the tenant's already committed usage plus the
        candidate's overlap with it.  The horizon reaches the later of the
        candidate's end and every live promise of the same tenant, so a future
        promise is protected even when the candidate itself ends first.

        A candidate's own terminal window (one ending exactly at its end, with
        no other committed work inside) is not re-charged against itself: one
        admitted interval is the unit the caller explicitly asked for.  As
        soon as that window contains other committed work — for example the
        preceding run of short jobs — it is checked in full.  Every other
        shared window is always checked.

        Live promise spans stay within the 64-tick timed envelope, so this
        stays small at any logical clock value.
        """
        profile = self._tenants[tenant]
        # One tick of the candidate alone must already be affordable.
        if (demand.cpu > profile.cpu_budget
                or demand.memory > profile.memory_budget):
            return False
        width = profile.width
        last = end
        for record in self._timed:
            if record.tenant == tenant and record.end > last:
                last = record.end
        for tick in range(start + 1, last + 1):
            used_cpu, used_memory = profile.window(tick)
            if tick == end == last and not used_cpu and not used_memory:
                continue
            span = min(tick, end) - max(tick - width, start)
            if span > 0:
                used_cpu += span * demand.cpu
                used_memory += span * demand.memory
            if (used_cpu > profile.cpu_budget
                    or used_memory > profile.memory_budget):
                return False
        return True

    def _budget_prefix_length_locked(self, tenant, demand, max_ticks, floor):
        """Longest prefix ``[now, now+length)`` the budget admits, or 0.

        Feasibility is monotone in the prefix length, so a binary search finds
        the longest budget-allowed segment at ``floor .. max_ticks``.
        """
        if not self._fits_budget_locked(
                tenant, self._now, self._now + floor, demand):
            return 0
        low, high = floor, max_ticks
        while low < high:
            middle = (low + high + 1) // 2
            if self._fits_budget_locked(
                    tenant, self._now, self._now + middle, demand):
                low = middle
            else:
                high = middle - 1
        return low

    def _commit_budget_locked(self, tenant, start, end, demand):
        self._tenants[tenant].add_box(
            start, end, demand.cpu, demand.memory)

    def _reject_metered_entry(self, names):
        """Registered metered jobs may run only through budgeted timed entries."""
        if any(name in self._metered for name in names):
            raise ValueError("metered job cannot run without a timed budget")

    def reserve(self, job, demand):
        """Atomically reserve both dimensions, or return None without consuming either."""
        if type(job) is not str or not job or not isinstance(demand, Demand):
            raise ValueError("invalid job or demand")
        self._reject_metered_entry((job,))
        with self._lock:
            if (job in self._active or job in self._timed_by_job
                    or job in self._permit_by_job):
                raise ValueError("job already has an active reservation")
            safety_check = self._permit_safety_checker_locked(self._permits)
            free_state = self._flat_free_locked()
            candidates = []
            for index, (name, (cpu, memory)) in enumerate(self._free.items()):
                if name in self._closed:
                    continue
                if not (demand.labels <= self._workers[name].labels):
                    continue
                if demand.cpu > cpu or demand.memory > memory:
                    continue
                # An ordinary lease has no end, so it must remain safe beside
                # every already-promised timed interval.
                if not self._indefinite_fits_locked(name, demand):
                    continue
                if self._permits:
                    trial_free = self._trial_free_locked(free_state, index, demand)
                    if not safety_check(trial_free):
                        continue
                candidates.append((cpu - demand.cpu, memory - demand.memory, name))
            if not candidates:
                return None
            cpu_after, memory_after, name = min(candidates)
            lease = Lease(job, name)
            self._free[name] = (cpu_after, memory_after)
            used = self._ordinary_use[name]
            used[0] += demand.cpu
            used[1] += demand.memory
            self._active[job] = lease, demand
            self._reserve_leases.add(lease)
            return lease

    def admit(self, job, options):
        """Issue a worker-less permit for one or two whole-demand choices.

        The permit is granted only when the pool can remain safe after *any*
        outstanding permit is claimed next with *any* of its legal options.
        No real worker capacity is deducted here.
        """
        if type(job) is not str or not job:
            raise ValueError("invalid job")
        if not isinstance(options, tuple) or not 1 <= len(options) <= 2:
            raise ValueError("options must be a tuple of one or two Demands")
        if any(not isinstance(option, Demand) for option in options):
            raise ValueError("every option must be a Demand")
        if len(self._workers) > PERMIT_WORKER_LIMIT:
            raise ValueError("permits support at most 6 workers")
        if any(worker.cpu > PERMIT_DIMENSION_LIMIT
               or worker.memory > PERMIT_DIMENSION_LIMIT
               for worker in self._workers.values()):
            raise ValueError("permit pools support at most 64 units per dimension")
        if any(option.cpu > PERMIT_DIMENSION_LIMIT
               or option.memory > PERMIT_DIMENSION_LIMIT
               for option in options):
            raise ValueError("each permit option may request at most 64 units per dimension")
        self._reject_metered_entry((job,))

        permit = Permit(job)
        with self._lock:
            if (job in self._active or job in self._timed_by_job
                    or job in self._permit_by_job):
                raise ValueError("job already has an active reservation or permit")
            # Permits are only for non-timed work and do not participate in a
            # drain's prepare phase.  Validation/duplicate errors win first.
            if self._timed or self._groups or self._stops or self._draining:
                return None
            if len(self._permits) >= MAX_PERMITS:
                return None

            record = _PermitRecord(permit, job, options)
            if not self._permits_fulfillable_locked(self._permits + [record]):
                return None

            self._permits.append(record)
            self._permit_by_job[job] = record
            return permit

    def claim(self, permit, option):
        """Claim the exact current permit and one legal option as a Lease."""
        if not isinstance(permit, Permit):
            raise ValueError("expected Permit")
        with self._lock:
            record = self._find_permit_record_locked(permit)
            if record is None:
                return None
            # Option validation applies only to a live original permit and must
            # reject bool even though bool is a subclass of int.
            if type(option) is not int or not 0 <= option < len(record.options):
                raise ValueError("invalid permit option")

            demand = record.options[option]
            remaining = [item for item in self._permits if item is not record]
            safety_check = self._permit_safety_checker_locked(remaining)
            free_state = self._flat_free_locked()
            candidates = []
            for index, (name, (cpu, memory)) in enumerate(self._free.items()):
                if name in self._closed:
                    continue
                if not (demand.labels <= self._workers[name].labels):
                    continue
                if demand.cpu > cpu or demand.memory > memory:
                    continue
                if remaining:
                    trial_free = self._trial_free_locked(free_state, index, demand)
                    if not safety_check(trial_free):
                        continue
                candidates.append((
                    cpu - demand.cpu,
                    memory - demand.memory,
                    name,
                ))

            # Admission maintains the recursive invariant, so every legal
            # option has at least one such worker when its claim acquires the
            # lock.  No state has been changed by the search above.
            if not candidates:
                raise RuntimeError("permit guarantee could not be fulfilled")
            cpu_after, memory_after, name = min(candidates)

            lease = Lease(record.job, name)
            self._free[name] = (cpu_after, memory_after)
            used = self._ordinary_use[name]
            used[0] += demand.cpu
            used[1] += demand.memory
            self._active[record.job] = (lease, demand)
            self._permits.remove(record)
            if self._permit_by_job.get(record.job) is record:
                del self._permit_by_job[record.job]
            return lease

    def cancel_permit(self, permit):
        """Cancel the exact current unclaimed permit; return whether it was live."""
        if not isinstance(permit, Permit):
            raise ValueError("expected Permit")
        with self._lock:
            record = self._find_permit_record_locked(permit)
            if record is None:
                return False
            self._permits.remove(record)
            if self._permit_by_job.get(record.job) is record:
                del self._permit_by_job[record.job]
            return True

    def _find_permit_record_locked(self, permit):
        # Exact object identity is mandatory; same-field forged, foreign and
        # stale permits never match a current record.
        for record in self._permits:
            if record.permit is permit:
                return record
        return None

    def _flat_free_locked(self):
        return tuple(
            value
            for cpu_memory in self._free.values()
            for value in cpu_memory
        )

    @staticmethod
    def _trial_free_locked(free, index, demand):
        trial = list(free)
        at = 2 * index
        trial[at] -= demand.cpu
        trial[at + 1] -= demand.memory
        return tuple(trial)

    def _permits_fulfillable_locked(self, records, free=None):
        if not records:
            return True
        if free is None:
            free = self._flat_free_locked()
        check = self._permit_safety_checker_locked(records)
        return check(free)

    def _permit_safety_checker_locked(self, records):
        """Build an exact recursive online-fulfillment predicate.

        A state is safe iff, for every permit that could act next and every
        option it could choose, the pool has *some* whole-worker placement
        after which the remaining permits are still safe in the reduced state.
        """
        if not records:
            return lambda _free: True

        option_sets = []
        for record in records:
            options = []
            for demand in record.options:
                eligible = tuple(
                    index
                    for index, name in enumerate(self._workers)
                    if name not in self._closed
                    and demand.labels <= self._workers[name].labels
                )
                options.append((demand.cpu, demand.memory, eligible))
            option_sets.append(tuple(options))

        count = len(records)
        memo = {}

        def feasible(mask, free):
            if mask == 0:
                return True
            cache_key = (mask, free)
            cached = memo.get(cache_key)
            if cached is not None:
                return cached

            remaining_mask = mask
            while remaining_mask:
                bit = remaining_mask & -remaining_mask
                index = bit.bit_length() - 1

                # Universal over the next holder's legal option.
                for cpu_need, memory_need, eligible in option_sets[index]:
                    found_worker = False
                    # Existential over the pool's immediate whole-worker choice.
                    for worker_index in eligible:
                        at = 2 * worker_index
                        free_cpu = free[at]
                        free_memory = free[at + 1]
                        if cpu_need > free_cpu or memory_need > free_memory:
                            continue
                        trial = list(free)
                        trial[at] = free_cpu - cpu_need
                        trial[at + 1] = free_memory - memory_need
                        if feasible(mask ^ bit, tuple(trial)):
                            found_worker = True
                            break
                    if not found_worker:
                        memo[cache_key] = False
                        return False

                remaining_mask ^= bit

            memo[cache_key] = True
            return True

        full_mask = (1 << count) - 1
        return lambda free: feasible(full_mask, free)

    def advance(self, now):
        """Advance logical time and atomically settle reached interval ends."""
        if type(now) is not int or now < 0:
            raise ValueError("now must be a non-negative integer")
        with self._lock:
            if now < self._now:
                raise ValueError("logical time cannot move backwards")

            # Settle all endings first. A boundary tick belongs to the later
            # interval because intervals are [start, end).
            expired = [record for record in list(self._timed)
                       if record.end <= now]
            for record in expired:
                if record.holding:
                    cpu, memory = self._free[record.worker]
                    self._free[record.worker] = (
                        cpu + record.demand.cpu,
                        memory + record.demand.memory,
                    )
                    record.holding = False
                current = self._active.get(record.job)
                if (record.lease is not None and current is not None
                        and current[0] is record.lease):
                    del self._active[record.job]
                self._remove_timed_locked(record)

            # Surviving intervals that have now started reserve current capacity
            # even before their Booking is activated.
            for record in self._timed:
                if not record.holding and record.start <= now:
                    self._deduct_current_locked(record.worker, record.demand)
                    record.holding = True

            # Maintenance windows hold no capacity of their own; reaching the
            # end simply releases the worker back to ordinary scheduling.
            self._stops = [stop for stop in self._stops if stop.end > now]

            # A group stops constraining the pool once every replica interval
            # has ended or been released.
            self._sweep_groups_locked()

            self._now = now

    def book(self, job, demand, start, end):
        """Reserve one whole fixed window on a single labelled worker."""
        if type(job) is not str or not job or not isinstance(demand, Demand):
            raise ValueError("invalid job or demand")
        if type(start) is not int or type(end) is not int:
            raise ValueError("start and end must be integers")
        self._check_timed_pool_limits()
        with self._lock:
            if not (self._now <= start < end):
                raise ValueError("booking window must satisfy now <= start < end")
            if end > self._now + TIMED_HORIZON_LIMIT:
                raise ValueError("booking end is beyond the supported horizon")
            if (job in self._active or job in self._timed_by_job
                    or job in self._permit_by_job):
                raise ValueError("job already has a reservation")
            if self._permits:
                raise RuntimeError("cannot create timed work while permits are outstanding")
            if self._draining:
                raise RuntimeError("cannot create timed work while drain is running")
            if len(self._timed) >= MAX_TIMED_RECORDS:
                return None

            tenant = self._metered.get(job)
            # A registered job must fit its tenant's rolling budget before any
            # capacity is chosen; a budget-only refusal consumes nothing.
            if tenant is not None and not self._fits_budget_locked(
                    tenant, start, end, demand):
                return None

            chosen = None
            for name in sorted(self._workers):
                if name in self._closed:
                    continue
                worker = self._workers[name]
                if not (demand.labels <= worker.labels):
                    continue
                if self._interval_fits_locked(name, demand, start, end):
                    chosen = name
                    break
            if chosen is None:
                return None

            booking = Booking(job, chosen, start, end)
            record = _TimedRecord(
                "booking", job, demand, chosen, start, end, booking=booking,
                tenant=tenant,
            )
            if tenant is not None:
                self._commit_budget_locked(tenant, start, end, demand)
            if start == self._now:
                self._deduct_current_locked(chosen, demand)
                record.holding = True
            self._timed.append(record)
            self._timed_by_job[job] = record
            return booking

    def activate(self, booking):
        """Activate the exact original Booking while logical time is in its window."""
        if not isinstance(booking, Booking):
            raise ValueError("expected Booking")
        with self._lock:
            record = self._find_booking_record_locked(booking)
            if record is None:
                return None
            if not (record.start <= self._now < record.end):
                return None
            if record.lease is not None:
                current = self._active.get(record.job)
                if current is not None and current[0] is record.lease:
                    return record.lease
                return None

            # A valid live record in its window is already holding capacity,
            # including after a time jump into its start. Defensively reconcile
            # only if an internal state transition left it not holding.
            if not record.holding:
                self._deduct_current_locked(record.worker, record.demand)
                record.holding = True
            lease = Lease(record.job, record.worker)
            record.lease = lease
            self._active[record.job] = (lease, record.demand)
            return lease

    def book_group(self, replicas, start, end):
        """Book one batch of 2-3 same-window replicas on distinct fault domains.

        ``replicas`` is a tuple of ``(job, Demand)`` pairs in return order.
        Every replica occupies one fixed, labelled worker for the whole
        half-open ``[start, end)`` window, and the chosen workers must carry
        pairwise different fault domains.  A per-replica placement is not
        enough: the whole batch is accepted only when one joint assignment
        exists.  Returns the pool's original ``GroupTicket`` or ``None``.
        """
        if self._fault_domains is None:
            raise ValueError("group bookings require fault_domains configuration")
        if not isinstance(replicas, tuple) or not (
            GROUP_REPLICA_MIN <= len(replicas) <= GROUP_REPLICA_MAX
        ):
            raise ValueError("replicas must be a tuple of 2 or 3 (job, Demand) pairs")
        jobs = []
        demands = []
        for pair in replicas:
            if not isinstance(pair, tuple) or len(pair) != 2:
                raise ValueError("each replica must be a (job, Demand) pair")
            job, demand = pair
            if type(job) is not str or not job or not isinstance(demand, Demand):
                raise ValueError("invalid replica job or demand")
            jobs.append(job)
            demands.append(demand)
        self._reject_metered_entry(jobs)
        if type(start) is not int or type(end) is not int:
            raise ValueError("start and end must be integers")
        if not (start < end):
            raise ValueError("booking window must satisfy start < end")
        if start <= self._now:
            raise ValueError("group bookings must start strictly after now")
        if end > self._now + GROUP_HORIZON_LIMIT:
            raise ValueError("booking end is beyond the supported horizon")

        with self._lock:
            if len(self._workers) > GROUP_WORKER_LIMIT:
                return None
            if len(self._groups) >= MAX_GROUP_BOOKINGS:
                return None
            if self._permits:
                raise RuntimeError("cannot create timed work while permits are outstanding")
            if self._draining:
                raise RuntimeError("cannot create timed work while drain is running")
            if len(self._timed) + len(replicas) > MAX_TIMED_RECORDS:
                return None
            if len(set(jobs)) != len(jobs):
                raise ValueError("duplicate replica job in group")
            if any(job in self._active or job in self._timed_by_job
                   or job in self._permit_by_job for job in jobs):
                raise ValueError("job already has a reservation")

            assignment = self._plan_group_locked(demands, start, end)
            if assignment is None:
                return None

            ticket = GroupTicket(tuple(jobs))
            group = _GroupRecord(ticket, [], start, end)
            records = []
            for (job, demand), name in zip(replicas, assignment):
                record = _TimedRecord(
                    "group", job, demand, name, start, end, group=group,
                )
                # start > now by validation, so nothing is deducted yet.
                self._timed.append(record)
                self._timed_by_job[job] = record
                records.append(record)
            group.replicas = records
            self._groups.append(group)
            return ticket

    def _plan_group_locked(self, demands, start, end):
        """Backtracking joint assignment: never reject a feasible batch."""
        assignment = []
        used_workers = set()
        used_domains = set()

        def search(index):
            if index == len(demands):
                return True
            demand = demands[index]
            for name in sorted(self._workers):
                if name in self._closed or name in used_workers:
                    continue
                worker = self._workers[name]
                if not (demand.labels <= worker.labels):
                    continue
                domain = self._fault_domains[name]
                if domain in used_domains:
                    continue
                if not self._interval_fits_locked(name, demand, start, end):
                    continue
                used_workers.add(name)
                used_domains.add(domain)
                assignment.append(name)
                if search(index + 1):
                    return True
                assignment.pop()
                used_domains.remove(domain)
                used_workers.remove(name)
            return False

        return tuple(assignment) if search(0) else None

    def activate_group(self, ticket):
        """Issue the whole batch's original Leases at exactly its start tick.

        Returns a tuple of ``Lease`` objects in replica order, or ``None`` for
        early, late, forged, foreign or already-finished tickets.  Repeating a
        valid activation while every member is live returns the same leases.
        """
        if not isinstance(ticket, GroupTicket):
            raise ValueError("expected GroupTicket")
        with self._lock:
            group = self._find_group_locked(ticket)
            if group is None:
                return None
            if not (group.start <= self._now < group.end):
                return None
            if group.leases is not None:
                # A member that was released or expired means the batch can
                # never be started again as a whole.
                for record, lease in zip(group.replicas, group.leases):
                    current = self._active.get(record.job)
                    if current is None or current[0] is not lease:
                        return None
                return tuple(group.leases)

            leases = []
            for record in group.replicas:
                if not record.holding:
                    self._deduct_current_locked(record.worker, record.demand)
                    record.holding = True
                lease = Lease(record.job, record.worker)
                record.lease = lease
                self._active[record.job] = (lease, record.demand)
                leases.append(lease)
            group.leases = leases
            return tuple(leases)

    def _find_group_locked(self, ticket):
        # Object identity, exactly like Booking/Permit fencing.
        for group in self._groups:
            if group.ticket is ticket:
                return group
        return None

    def _sweep_groups_locked(self):
        self._groups = [
            group for group in self._groups
            if any(record in self._timed for record in group.replicas)
        ]

    def schedule_stop(self, worker, earliest, latest, duration):
        """Commit one maintenance window for ``worker`` inside [earliest, latest].

        Returns the immutable tuple ``(actual_start, actual_end)`` for the
        earliest feasible whole-tick start, or ``None`` when no window can be
        honored.  Confirmed windows never move for later requests.
        """
        if type(worker) is not str or worker not in self._workers:
            raise ValueError("unknown worker")
        if (type(earliest) is not int or type(latest) is not int
                or type(duration) is not int):
            raise ValueError("stop bounds and duration must be integers")
        if duration <= 0:
            raise ValueError("stop duration must be positive")
        if earliest < self._now:
            raise ValueError("stop window cannot start before now")
        if latest < earliest:
            raise ValueError("latest must not precede earliest")
        if earliest + duration > self._now + GROUP_HORIZON_LIMIT:
            raise ValueError("stop window end is beyond the supported horizon")

        with self._lock:
            if len(self._workers) > GROUP_WORKER_LIMIT:
                return None
            if len(self._stops) >= MAX_STOP_WINDOWS:
                return None
            if self._permits:
                raise RuntimeError("cannot schedule stops while permits are outstanding")
            if self._draining:
                raise RuntimeError("cannot schedule stops while drain is running")
            # An indefinite ordinary lease has no end tick to wait out; it
            # cannot coexist with a promised stop on the same worker.
            for job, (lease, _demand) in self._active.items():
                if lease.worker != worker:
                    continue
                if self._timed_by_job.get(job) is None:
                    return None
            latest_start = min(latest, self._now + GROUP_HORIZON_LIMIT - duration)
            for start in range(earliest, latest_start + 1):
                end = start + duration
                if self._stop_fits_locked(worker, start, end):
                    self._stops.append(_StopRecord(worker, start, end))
                    return (start, end)
            return None

    def _stop_fits_locked(self, worker, start, end):
        # Same-worker stop windows may not overlap; touching endpoints are fine.
        for stop in self._stops:
            if stop.worker == worker and start < stop.end and stop.start < end:
                return False
        # No surviving execution or reservation may occupy the worker inside
        # the window.  Timed records include bookings, backfills and group
        # replicas; ordinary leases were rejected separately in schedule_stop.
        for record in self._timed:
            if record.worker == worker and start < record.end and record.start < end:
                return False
        return True

    def backfill(self, job, demand, min_ticks, max_ticks):
        """Grant an immediate finite lease with the longest safe prefix found."""
        if type(job) is not str or not job or not isinstance(demand, Demand):
            raise ValueError("invalid job or demand")
        if type(min_ticks) is not int or type(max_ticks) is not int:
            raise ValueError("tick bounds must be integers")
        if not (1 <= min_ticks <= max_ticks <= TIMED_HORIZON_LIMIT):
            raise ValueError("invalid backfill tick bounds")
        self._check_timed_pool_limits()
        with self._lock:
            if (job in self._active or job in self._timed_by_job
                    or job in self._permit_by_job):
                raise ValueError("job already has a reservation")
            if self._permits:
                raise RuntimeError("cannot create timed work while permits are outstanding")
            if self._draining:
                raise RuntimeError("cannot create timed work while drain is running")
            if len(self._timed) >= MAX_TIMED_RECORDS:
                return None

            tenant = self._metered.get(job)
            # The budget is tenant-wide, so its longest admissible prefix is
            # independent of the worker; each worker then delivers the shorter
            # of that prefix and its physically safe prefix.
            budget_length = max_ticks
            if tenant is not None:
                budget_length = self._budget_prefix_length_locked(
                    tenant, demand, max_ticks, min_ticks)
                if budget_length < min_ticks:
                    return None
            best = None  # (length, name, until)
            for name in sorted(self._workers):
                if name in self._closed:
                    continue
                if not (demand.labels <= self._workers[name].labels):
                    continue
                until = self._backfill_until_locked(
                    name, demand, min_ticks, max_ticks,
                )
                if until is None:
                    continue
                length = min(until - self._now, budget_length)
                if length < min_ticks:
                    continue
                until = self._now + length
                # Sorted iteration plus strict > keeps the smallest worker name
                # among equal maximum lengths. The spec permits any worker rule.
                if best is None or length > best[0]:
                    best = (length, name, until)
            if best is None:
                return None

            _length, name, until = best
            lease = Lease(job, name)
            record = _TimedRecord(
                "backfill", job, demand, name, self._now, until,
                lease=lease, holding=True, tenant=tenant,
            )
            if tenant is not None:
                self._commit_budget_locked(tenant, self._now, until, demand)
            self._deduct_current_locked(name, demand)
            self._active[job] = (lease, demand)
            self._timed.append(record)
            self._timed_by_job[job] = record
            return lease, until

    def _check_timed_pool_limits(self):
        if (len(self._workers) > TIMED_WORKER_LIMIT
                or any(worker.cpu > TIMED_DIMENSION_LIMIT
                       or worker.memory > TIMED_DIMENSION_LIMIT
                       for worker in self._workers.values())):
            raise ValueError(
                "timed scheduling supports at most 8 workers and 64 units per dimension"
            )

    def _deduct_current_locked(self, name, demand):
        cpu, memory = self._free[name]
        self._free[name] = (cpu - demand.cpu, memory - demand.memory)

    def _remove_timed_locked(self, record):
        try:
            self._timed.remove(record)
        except ValueError:
            pass
        if self._timed_by_job.get(record.job) is record:
            del self._timed_by_job[record.job]

    def _find_booking_record_locked(self, booking):
        # Object identity is mandatory: an equal-valued forged Booking is not
        # the credential issued by this pool.
        for record in self._timed:
            if record.booking is booking:
                return record
        return None

    def _tick_fits_locked(self, name, demand, tick):
        # A committed stop window rejects every new occupancy on that worker,
        # regardless of remaining capacity; touching endpoints are allowed.
        for stop in self._stops:
            if stop.worker == name and stop.start <= tick < stop.end:
                return False
        worker = self._workers[name]
        base_cpu = self._ordinary_use[name][0] + self._hold_use[name][0]
        base_memory = self._ordinary_use[name][1] + self._hold_use[name][1]
        cpu = base_cpu + demand.cpu
        memory = base_memory + demand.memory
        if cpu > worker.cpu or memory > worker.memory:
            return False
        for record in self._timed:
            if (record.worker == name
                    and record.start <= tick < record.end):
                cpu += record.demand.cpu
                memory += record.demand.memory
                if cpu > worker.cpu or memory > worker.memory:
                    return False
        return True

    def _interval_fits_locked(self, name, demand, start, end):
        for tick in range(start, end):
            if not self._tick_fits_locked(name, demand, tick):
                return False
        return True

    def _indefinite_fits_locked(self, name, demand):
        worker = self._workers[name]
        base_cpu = (self._ordinary_use[name][0]
                    + self._hold_use[name][0] + demand.cpu)
        base_memory = (self._ordinary_use[name][1]
                       + self._hold_use[name][1] + demand.memory)
        if base_cpu > worker.cpu or base_memory > worker.memory:
            return False
        horizon = self._now
        for record in self._timed:
            if record.end > horizon:
                horizon = record.end
        # An indefinite lease cannot outlive a promised stop on its worker.
        for stop in self._stops:
            if stop.worker == name and stop.end > horizon:
                horizon = stop.end
        for tick in range(self._now, horizon):
            if not self._tick_fits_locked(name, demand, tick):
                return False
        return True

    def _backfill_until_locked(self, name, demand, min_ticks, max_ticks):
        until = self._now
        last = self._now + max_ticks
        for tick in range(self._now, last):
            if self._tick_fits_locked(name, demand, tick):
                until = tick + 1
            else:
                # The lease occupies one continuous prefix beginning at now.
                break
        if until - self._now < min_ticks:
            return None
        return until

    def release(self, lease):
        """Only the exact current lease can return units."""
        if not isinstance(lease, Lease):
            raise ValueError("expected Lease")
        with self._lock:
            record = self._active.get(lease.job)
            if record is None or record[0] is not lease:
                return False
            current_lease, demand = record
            cpu, memory = self._free[lease.worker]
            self._free[lease.worker] = (cpu + demand.cpu, memory + demand.memory)

            # A reader lease aborts only its own run: units return, its local
            # read holds are dropped, but no borrowed result is confirmed.
            reader = self._readers.pop(lease, None)
            if reader is not None:
                used = self._ordinary_use[lease.worker]
                used[0] -= demand.cpu
                used[1] -= demand.memory
                del self._active[lease.job]
                return True

            self._reserve_leases.discard(lease)
            timed = self._timed_by_job.get(lease.job)
            if timed is not None and timed.lease is current_lease:
                # Early release abandons the remainder of the fixed interval.
                # Metered time already walked stays billed, and a partial
                # prefix of a backfill is billed only through this tick.
                self._abandon_billed_locked(timed)
                self._remove_timed_locked(timed)
            else:
                used = self._ordinary_use[lease.worker]
                used[0] -= demand.cpu
                used[1] -= demand.memory
            del self._active[lease.job]
            # A replica release may be the last live member of its batch.
            self._sweep_groups_locked()
            return True

    def _abandon_billed_locked(self, record):
        """Stop billing the never-walked remainder of a released interval.

        A successful early return gives up only the commitment from the
        current tick onward; every tick already walked keeps counting against
        later rolling windows until it slides out.
        """
        if record.tenant is None:
            return
        self._tenants[record.tenant].add_box(
            self._now, record.end, -record.demand.cpu, -record.demand.memory)

    def available(self):
        """Real unoccupied and unreserved units, including workers that reject new jobs."""
        with self._lock:
            return dict(self._free)

    def current(self, job):
        """Return the Lease currently bound to *job*, or None when the job is unbound."""
        with self._lock:
            record = self._active.get(job)
            return None if record is None else record[0]

    # ------------------------------------------------------------------
    # Batch-result handoff: publish, start_reader, finish_reader.
    # ------------------------------------------------------------------

    def publish(self, lease, blocks, readers):
        """Publish one batch result from a current raw ordinary lease.

        ``blocks`` is a tuple of 1-8 immutable ``bytes`` objects (1-64 bytes
        each); ``readers`` is a tuple of 1-4 distinct non-empty job names that
        will confirm the result later.  Returns the pool's original
        ``Artifact`` credential, or ``None`` for a legal request that cannot be
        honored (stale/inapplicable lease, no free artifact slot, evacuation in
        progress).  The producer is unbound: all CPU and the memory not used by
        the finished blocks return to its worker, while the blocks stay in that
        worker's memory until moved to the exchange or fully confirmed.
        """
        if not self._handoff_enabled:
            raise RuntimeError("batch-result handoff is not enabled")
        self._check_artifact_pool_limits()
        if not isinstance(lease, Lease):
            raise ValueError("expected Lease")
        if not isinstance(blocks, tuple) or not (
                BLOCKS_PER_ARTIFACT_MIN <= len(blocks) <= BLOCKS_PER_ARTIFACT_MAX):
            raise ValueError("blocks must be a tuple of 1 to 8 bytes objects")
        if any(type(block) is not bytes or not (
                BLOCK_BYTES_MIN <= len(block) <= BLOCK_BYTES_MAX)
                for block in blocks):
            raise ValueError("every block must be immutable bytes of 1 to 64 bytes")
        if not isinstance(readers, tuple) or not (
                1 <= len(readers) <= MAX_ARTIFACT_READERS):
            raise ValueError("readers must be a tuple of 1 to 4 job names")
        if any(type(reader) is not str or not reader for reader in readers):
            raise ValueError("reader names must be non-empty strings")
        if len(set(readers)) != len(readers):
            raise ValueError("reader names must be distinct")

        with self._lock:
            # Only the exact current lease issued by ``reserve`` may publish;
            # timed, permit-claimed, reader and migrated leases are excluded.
            record = self._active.get(lease.job)
            if (record is None or record[0] is not lease
                    or lease not in self._reserve_leases
                    or lease in self._readers
                    or self._timed_by_job.get(lease.job) is not None):
                return None
            _current_lease, demand = record
            if len(blocks) > demand.memory:
                raise ValueError("one result cannot exceed the producer's memory")
            # A reader may wait on at most four results in total.  Reaching that
            # wait cap is a capacity refusal, not a malformed request.
            for reader in readers:
                waiting = sum(1 for rec in self._artifacts.values()
                              if reader in rec.pending)
                if waiting + 1 > MAX_READER_INPUTS:
                    return None
            if self._draining:
                return None
            if len(self._artifacts) >= MAX_ARTIFACTS:
                return None

            worker = lease.worker
            free_cpu, free_memory = self._free[worker]
            self._free[worker] = (
                free_cpu + demand.cpu,
                free_memory + demand.memory - len(blocks),
            )
            used = self._ordinary_use[worker]
            used[0] -= demand.cpu
            used[1] -= demand.memory
            used[1] += len(blocks)

            del self._active[lease.job]
            self._reserve_leases.discard(lease)
            artifact = Artifact(lease.job)
            artifact_record = _ArtifactRecord(
                artifact, lease.job, tuple(blocks), tuple(readers), worker,
            )
            self._artifacts[artifact] = artifact_record
            return artifact

    def start_reader(self, job, demand, artifacts):
        """Start a downstream reader over every currently pending result.

        Returns ``(lease, inputs)`` where ``inputs`` are the block tuples in
        ``artifacts`` order, or ``None`` when no legal arrangement exists.  To
        make all inputs simultaneously readable on the chosen worker, required
        local results may be moved wholesale into the shared exchange area (as
        many as the exchange capacity allows); such a move is committed
        atomically with the placement.
        """
        if not self._handoff_enabled:
            raise RuntimeError("batch-result handoff is not enabled")
        if type(job) is not str or not job or not isinstance(demand, Demand):
            raise ValueError("invalid job or demand")
        self._check_artifact_pool_limits()
        if not isinstance(artifacts, tuple) or not artifacts:
            raise ValueError("artifacts must be a non-empty tuple")
        if any(not isinstance(artifact, Artifact) for artifact in artifacts):
            raise ValueError("every input must be an Artifact")
        if len(set(artifacts)) != len(artifacts):
            raise ValueError("duplicate input artifact")
        if len(artifacts) > MAX_READER_INPUTS:
            raise ValueError("one reader may wait on at most four results")
        self._reject_metered_entry((job,))

        with self._lock:
            # The job name is exclusive with every other live binding; adding
            # inputs to an already-reading job is therefore rejected here too.
            if (job in self._active or job in self._timed_by_job
                    or job in self._permit_by_job):
                raise ValueError("job already has an active reservation")
            if self._draining:
                return None
            # Every credential must be this pool's live original object.
            records = []
            for artifact in artifacts:
                record = self._artifacts.get(artifact)
                if record is None:
                    return None
                records.append(record)
            # The call must carry exactly the job's whole pending set, and the
            # job must be an authorized reader of every passed result.
            if any(job not in rec.pending for rec in records):
                return None
            required = {rec.artifact for rec in self._artifacts.values()
                        if job in rec.pending}
            if required != set(artifacts):
                return None

            arrangement = self._plan_reader_locked(demand, records)
            if arrangement is None:
                return None
            worker, plan = arrangement

            # Commit: move this request's required local results wholesale,
            # then place.  No other result is touched.
            local_holds = {}
            for record in records:
                if record.in_exchange:
                    continue
                if plan[record.artifact] == "exchange":
                    self._move_artifact_to_exchange_locked(record)
                else:
                    local_holds[record.artifact] = record.worker

            # After the moves the real free/ordinary figures already match the
            # simulated arrangement, so a plain deduction finalizes it.
            self._deduct_current_locked(worker, demand)
            used = self._ordinary_use[worker]
            used[0] += demand.cpu
            used[1] += demand.memory
            lease = Lease(job, worker)
            self._active[job] = (lease, demand)
            self._readers[lease] = _ReaderRecord(
                lease, tuple(rec.blocks for rec in records), local_holds,
                tuple(records),
            )
            return lease, tuple(rec.blocks for rec in records)

    def finish_reader(self, lease):
        """Confirm one current original reader lease.

        Returns ``True`` after returning the run's occupancy and confirming
        every result it borrowed.  A result is deregistered and its space
        returned only once its last registered reader confirms.  Any lease that
        is not the current original run of this pool returns ``False``.
        """
        if not self._handoff_enabled:
            raise RuntimeError("batch-result handoff is not enabled")
        if not isinstance(lease, Lease):
            raise ValueError("expected Lease")
        with self._lock:
            reader = self._readers.get(lease)
            record = self._active.get(lease.job)
            if (reader is None or record is None or record[0] is not lease):
                return False
            demand = record[1]
            self._free[lease.worker] = (
                self._free[lease.worker][0] + demand.cpu,
                self._free[lease.worker][1] + demand.memory,
            )
            used = self._ordinary_use[lease.worker]
            used[0] -= demand.cpu
            used[1] -= demand.memory
            del self._active[lease.job]
            del self._readers[lease]
            for artifact in reader.artifact_records:
                artifact.pending.discard(lease.job)
                if not artifact.pending:
                    self._deregister_artifact_locked(artifact)
            return True

    def artifact_location(self, artifact):
        """Where a live result's blocks currently live, or None if unknown."""
        if not self._handoff_enabled:
            return None
        with self._lock:
            record = self._artifacts.get(artifact) if isinstance(
                artifact, Artifact) else None
            if record is None:
                return None
            if record.in_exchange:
                return ("exchange", None)
            return ("local", record.worker)

    def exchange_used(self):
        """Number of exchange blocks actually occupied (0 when disabled)."""
        if not self._handoff_enabled:
            return 0
        with self._lock:
            return self._exchange_used

    def _check_artifact_pool_limits(self):
        if (len(self._workers) > ARTIFACT_WORKER_LIMIT
                or any(worker.cpu > ARTIFACT_DIMENSION_LIMIT
                       or worker.memory > ARTIFACT_DIMENSION_LIMIT
                       for worker in self._workers.values())):
            raise ValueError(
                "handoff supports at most 8 workers and 64 units per dimension"
            )

    def _move_artifact_to_exchange_locked(self, record):
        """Move all of one result's local blocks into the exchange area."""
        worker = record.worker
        blocks = len(record.blocks)
        free_cpu, free_memory = self._free[worker]
        self._free[worker] = (free_cpu, free_memory + blocks)
        self._ordinary_use[worker][1] -= blocks
        record.worker = None
        record.in_exchange = True
        self._exchange_used += blocks

    def _deregister_artifact_locked(self, record):
        """Release a confirmed result's space and forget the credential."""
        if record.in_exchange:
            self._exchange_used -= len(record.blocks)
        else:
            worker = record.worker
            free_cpu, free_memory = self._free[worker]
            self._free[worker] = (free_cpu, free_memory + len(record.blocks))
            self._ordinary_use[worker][1] -= len(record.blocks)
        self._artifacts.pop(record.artifact, None)

    def _artifact_read_held_locked(self, artifact):
        """Whether any running reader is locally reading *artifact* right now."""
        return any(artifact in reader.local_holds
                   for reader in self._readers.values())

    def _plan_reader_locked(self, demand, records):
        """Find one worker plus a legal local/exchange arrangement, or None.

        The search uses the real post-transfer state: moving a result frees its
        producer's memory and consumes exchange blocks.  Only results in this
        request may move; no other result is relocated, no result is split and
        no already-exchange result is moved back.
        """
        for name in sorted(self._workers):
            if name in self._closed:
                continue
            if not (demand.labels <= self._workers[name].labels):
                continue
            plan = self._plan_reader_on_locked(name, demand, records)
            if plan is not None:
                return plan
        return None

    def _plan_reader_on_locked(self, name, demand, records):
        # Mandatory decisions first.
        mandatory_moved = []
        optional_local = []
        plan = {}
        for record in records:
            if record.in_exchange:
                plan[record.artifact] = "exchange"
                continue
            if record.worker == name:
                optional_local.append(record)
            else:
                plan[record.artifact] = "exchange"
                mandatory_moved.append(record)

        # Enumerate which local-on-target results (if any) also move over.
        count = len(optional_local)
        for mask in range(1 << count):
            trial = dict(plan)
            freed = {}
            exchange_added = 0
            read_held = False
            for record in mandatory_moved:
                if self._artifact_read_held_locked(record.artifact):
                    read_held = True
                    break
                exchange_added += len(record.blocks)
                freed[record.worker] = (
                    freed.get(record.worker, 0) + len(record.blocks)
                )
            if read_held:
                continue
            for index, record in enumerate(optional_local):
                if mask & (1 << index):
                    if self._artifact_read_held_locked(record.artifact):
                        read_held = True
                        break
                    trial[record.artifact] = "exchange"
                    exchange_added += len(record.blocks)
                    freed[name] = freed.get(name, 0) + len(record.blocks)
                else:
                    trial[record.artifact] = "local"
            if read_held:
                continue
            if self._exchange_used + exchange_added > self._exchange_capacity:
                continue

            simulated_free = {
                worker_name: [free_cpu, free_memory]
                for worker_name, (free_cpu, free_memory) in self._free.items()
            }
            for worker_name, amount in freed.items():
                simulated_free[worker_name][1] += amount

            if (demand.cpu > simulated_free[name][0]
                    or demand.memory > simulated_free[name][1]):
                continue

            simulated_ordinary = (
                self._ordinary_use[name][0],
                # Local blocks that move out are dropped from ordinary usage;
                # blocks that stay local remain in that figure.
                self._ordinary_use[name][1] - freed.get(name, 0),
            )
            if not self._indefinite_fits_sim_locked(
                    name, demand, simulated_ordinary):
                continue
            if self._permits:
                flat = tuple(
                    value
                    for worker_name in self._workers
                    for value in simulated_free[worker_name]
                )
                trial_flat = list(flat)
                at = 2 * list(self._workers).index(name)
                trial_flat[at] -= demand.cpu
                trial_flat[at + 1] -= demand.memory
                if not self._permit_safety_checker_locked(
                        self._permits)(tuple(trial_flat)):
                    continue
            return name, trial
        return None

    def _indefinite_fits_sim_locked(self, name, demand, simulated_ordinary):
        """Indefinite-fit test with an adjusted ordinary-usage baseline."""
        worker = self._workers[name]
        base_cpu = (simulated_ordinary[0] + self._hold_use[name][0]
                    + demand.cpu)
        base_memory = (simulated_ordinary[1] + self._hold_use[name][1]
                       + demand.memory)
        if base_cpu > worker.cpu or base_memory > worker.memory:
            return False
        horizon = self._now
        for record in self._timed:
            if record.end > horizon:
                horizon = record.end
        for stop in self._stops:
            if stop.worker == name and stop.end > horizon:
                horizon = stop.end
        for tick in range(self._now, horizon):
            if not self._tick_fits_sim_locked(
                    name, demand, simulated_ordinary, tick):
                return False
        return True

    def _tick_fits_sim_locked(self, name, demand, simulated_ordinary, tick):
        for stop in self._stops:
            if stop.worker == name and stop.start <= tick < stop.end:
                return False
        worker = self._workers[name]
        cpu = simulated_ordinary[0] + self._hold_use[name][0] + demand.cpu
        memory = simulated_ordinary[1] + self._hold_use[name][1] + demand.memory
        if cpu > worker.cpu or memory > worker.memory:
            return False
        for record in self._timed:
            if record.worker == name and record.start <= tick < record.end:
                cpu += record.demand.cpu
                memory += record.demand.memory
                if cpu > worker.cpu or memory > worker.memory:
                    return False
        return True

    def drain(self, worker, prepare):
        """Move every active job off *worker* before maintenance.

        ``prepare(old_lease, target_name)`` is called synchronously, once per
        original binding, without holding the pool lock so other threads can
        keep using reserve/release/current while a callback waits. A migration
        is published only when the callback returns exactly ``True`` and the
        original lease is still current. Published migrations never roll back;
        a failed attempt returns ``prepare_failed`` with the unpublished jobs
        still on their original bindings. The source never reopens automatically.

        Returns ``{"status": ..., "moved": {job: new_lease}}`` where status is
        one of ``drained``, ``no_capacity``, ``prepare_failed`` or ``busy``.
        """
        with self._lock:
            # Validation first; any failure raises before touching pool state.
            if type(worker) is not str or worker not in self._workers:
                raise ValueError("unknown worker")
            if not callable(prepare):
                raise ValueError("prepare must be callable")
            if len(self._workers) > DRAIN_WORKER_LIMIT:
                raise ValueError("drain supports at most 8 workers")
            if any(w.cpu > DRAIN_DIMENSION_LIMIT
                   or w.memory > DRAIN_DIMENSION_LIMIT
                   for w in self._workers.values()):
                raise ValueError("drain supports at most 64 units per resource dimension")
            source_jobs = sorted(
                job for job, (lease, _demand) in self._active.items()
                if lease.worker == worker
            )
            if len(source_jobs) > DRAIN_SOURCE_JOB_LIMIT:
                raise ValueError("drain source has more than 6 active jobs")
            if (self._draining or self._timed or self._permits
                    or self._groups or self._stops or self._artifacts):
                # Timed promises, group bookings, stop windows, outstanding
                # choice permits and live batch results are not migrated by
                # this evacuation. A legal request is busy and must not close
                # the source or run prepare.
                return {"status": "busy", "moved": {}}

            # The attempt starts here: the source stops accepting new jobs and
            # stays closed even if planning or preparation fails later.
            self._closed.add(worker)
            self._draining = True

        # job -> (target, cpu, memory) portion held for that job
        job_holds = {}
        # target -> [cpu, memory] still held by this attempt
        holds = {}
        moved = {}
        prepare_failed = False
        try:
            with self._lock:
                plan = self._plan_drain(source_jobs)
                if plan is None:
                    return {"status": "no_capacity", "moved": {}}
                old_leases = {}
                # Reserve the whole batch's target shares before any callback.
                for job in source_jobs:
                    target = plan[job]
                    old_lease, demand = self._active[job]
                    old_leases[job] = old_lease
                    cpu, memory = self._free[target]
                    self._free[target] = (cpu - demand.cpu, memory - demand.memory)
                    self._hold_use[target][0] += demand.cpu
                    self._hold_use[target][1] += demand.memory
                    job_holds[job] = (target, demand.cpu, demand.memory)
                    held = holds.setdefault(target, [0, 0])
                    held[0] += demand.cpu
                    held[1] += demand.memory

            # Serial callbacks, always invoked outside the pool lock.
            for job in source_jobs:
                target = job_holds[job][0]
                old_lease = old_leases[job]
                with self._lock:
                    record = self._active.get(job)
                    if record is None or record[0] is not old_lease:
                        # Already gone before the callback's turn: skip it.
                        self._drop_hold(job_holds, holds, job)
                        continue
                error = None
                result = None
                try:
                    result = prepare(old_lease, target)
                except Exception as exc:
                    error = exc
                with self._lock:
                    record = self._active.get(job)
                    if record is None or record[0] is not old_lease:
                        # The binding was released/replaced while the callback
                        # ran: its outcome can publish, resurrect or fail
                        # nothing. The hold is returned now that the callback
                        # has exited.
                        self._drop_hold(job_holds, holds, job)
                        continue
                    if error is not None or result is not True:
                        prepare_failed = True
                        break
                    demand = record[1]
                    # Atomic publish: source units return, the target hold
                    # converts to occupancy and the lease object is replaced in
                    # one locked step, so there is no unbound window and a stale
                    # lease can never release the new occupancy.
                    cpu, memory = self._free[worker]
                    self._free[worker] = (cpu + demand.cpu, memory + demand.memory)
                    source_used = self._ordinary_use[worker]
                    source_used[0] -= demand.cpu
                    source_used[1] -= demand.memory
                    self._convert_hold(job_holds, holds, job)
                    new_lease = Lease(job, target)
                    self._active[job] = (new_lease, demand)
                    self._reserve_leases.discard(old_lease)
                    moved[job] = new_lease
        finally:
            with self._lock:
                # Return every hold that never converted to occupancy.
                for target, (cpu, memory) in list(holds.items()):
                    free_cpu, free_memory = self._free[target]
                    self._free[target] = (free_cpu + cpu, free_memory + memory)
                    self._hold_use[target][0] -= cpu
                    self._hold_use[target][1] -= memory
                holds.clear()
                job_holds.clear()
                self._draining = False

        return {
            "status": "prepare_failed" if prepare_failed else "drained",
            "moved": moved,
        }

    def _plan_drain(self, jobs):
        """Backtracking search: never reject when a complete feasible plan exists."""
        free = {name: [cpu, memory] for name, (cpu, memory) in self._free.items()}
        plan = {}

        def search(index):
            if index == len(jobs):
                return True
            job = jobs[index]
            _lease, demand = self._active[job]
            targets = []
            for name, candidate in self._workers.items():
                # The source and any worker that stopped accepting jobs are
                # ineligible targets for this or any other drain.
                if name in self._closed:
                    continue
                if (demand.labels <= candidate.labels
                        and demand.cpu <= free[name][0]
                        and demand.memory <= free[name][1]):
                    targets.append(name)
            for name in sorted(targets):
                free[name][0] -= demand.cpu
                free[name][1] -= demand.memory
                plan[job] = name
                if search(index + 1):
                    return True
                free[name][0] += demand.cpu
                free[name][1] += demand.memory
                del plan[job]
            return False

        return dict(plan) if search(0) else None

    def _drop_hold(self, job_holds, holds, job):
        """Return one job's hold to the target's free units (caller holds lock)."""
        target, cpu, memory = job_holds.pop(job)
        held = holds[target]
        held[0] -= cpu
        held[1] -= memory
        if held == [0, 0]:
            del holds[target]
        free_cpu, free_memory = self._free[target]
        self._free[target] = (free_cpu + cpu, free_memory + memory)
        self._hold_use[target][0] -= cpu
        self._hold_use[target][1] -= memory

    def _convert_hold(self, job_holds, holds, job):
        """Turn one job's hold into occupancy without changing target free units."""
        target, cpu, memory = job_holds.pop(job)
        held = holds[target]
        held[0] -= cpu
        held[1] -= memory
        if held == [0, 0]:
            del holds[target]
        self._hold_use[target][0] -= cpu
        self._hold_use[target][1] -= memory
        target_used = self._ordinary_use[target]
        target_used[0] += cpu
        target_used[1] += memory
