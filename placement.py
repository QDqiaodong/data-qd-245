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
class Booking:
    """Immutable credential for a future fixed-window placement."""

    job: str
    worker: str
    start: int
    end: int


class _TimedRecord:
    """One live booking or backfill interval.

    The pool lock guards every instance and the ``holding`` flag records whether
    the interval's demand is currently deducted from ``Pool._free``.
    """

    __slots__ = (
        "kind", "job", "demand", "worker", "start", "end",
        "booking", "lease", "holding",
    )

    def __init__(self, kind, job, demand, worker, start, end,
                 booking=None, lease=None, holding=False):
        self.kind = kind
        self.job = job
        self.demand = demand
        self.worker = worker
        self.start = start
        self.end = end
        self.booking = booking
        self.lease = lease
        self.holding = holding


class Pool:
    def __init__(self, workers):
        self._workers = {}
        self._free = {}
        for worker in workers:
            if worker.name in self._workers:
                raise ValueError("duplicate worker")
            self._workers[worker.name] = worker
            self._free[worker.name] = (worker.cpu, worker.memory)
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

    def reserve(self, job, demand):
        """Atomically reserve both dimensions, or return None without consuming either."""
        if type(job) is not str or not job or not isinstance(demand, Demand):
            raise ValueError("invalid job or demand")
        with self._lock:
            if job in self._active or job in self._timed_by_job:
                raise ValueError("job already has an active reservation")
            candidates = []
            for name, (cpu, memory) in self._free.items():
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
            if job in self._active or job in self._timed_by_job:
                raise ValueError("job already has a reservation")
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
            if job in self._active or job in self._timed_by_job:
                raise ValueError("job already has a reservation")
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
            if self._draining or self._timed:
                # Timed promises are not migrated by this evacuation. A legal
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
