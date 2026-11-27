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


class _TimedRecord:
    """One live booking, backfill or group-replica interval.

    The pool lock guards every instance and the ``holding`` flag records whether
    the interval's demand is currently deducted from ``Pool._free``.
    """

    __slots__ = (
        "kind", "job", "demand", "worker", "start", "end",
        "booking", "lease", "holding", "group",
    )

    def __init__(self, kind, job, demand, worker, start, end,
                 booking=None, lease=None, holding=False, group=None):
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
    def __init__(self, workers, *, fault_domains=None):
        self._workers = {}
        self._free = {}
        for worker in workers:
            if worker.name in self._workers:
                raise ValueError("duplicate worker")
            self._workers[worker.name] = worker
            self._free[worker.name] = (worker.cpu, worker.memory)
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

    def reserve(self, job, demand):
        """Atomically reserve both dimensions, or return None without consuming either."""
        if type(job) is not str or not job or not isinstance(demand, Demand):
            raise ValueError("invalid job or demand")
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
            )
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
                length = until - self._now
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
                lease=lease, holding=True,
            )
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

            timed = self._timed_by_job.get(lease.job)
            if timed is not None and timed.lease is current_lease:
                # Early release abandons the remainder of the fixed interval.
                self._remove_timed_locked(timed)
            else:
                used = self._ordinary_use[lease.worker]
                used[0] -= demand.cpu
                used[1] -= demand.memory
            del self._active[lease.job]
            # A replica release may be the last live member of its batch.
            self._sweep_groups_locked()
            return True

    def available(self):
        """Real unoccupied and unreserved units, including workers that reject new jobs."""
        with self._lock:
            return dict(self._free)

    def current(self, job):
        """Return the Lease currently bound to *job*, or None when the job is unbound."""
        with self._lock:
            record = self._active.get(job)
            return None if record is None else record[0]

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
                    or self._groups or self._stops):
                # Timed promises, group bookings, stop windows and outstanding
                # choice permits are not migrated by this evacuation. A legal
                # request is busy and must not close the source or run prepare.
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
