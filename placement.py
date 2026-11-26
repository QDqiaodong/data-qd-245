"""Resource placement and fenced release of in-process worker reservations."""
from dataclasses import dataclass
from threading import Lock


DRAIN_WORKER_LIMIT = 8
DRAIN_DIMENSION_LIMIT = 64
DRAIN_SOURCE_JOB_LIMIT = 6


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

    def reserve(self, job, demand):
        """Atomically reserve both dimensions, or return None without consuming either."""
        if type(job) is not str or not job or not isinstance(demand, Demand):
            raise ValueError("invalid job or demand")
        with self._lock:
            if job in self._active:
                raise ValueError("job already has an active reservation")
            candidates = []
            for name, (cpu, memory) in self._free.items():
                if name in self._closed:
                    continue
                if (demand.labels <= self._workers[name].labels
                        and demand.cpu <= cpu and demand.memory <= memory):
                    candidates.append((cpu - demand.cpu, memory - demand.memory, name))
            if not candidates:
                return None
            cpu_after, memory_after, name = min(candidates)
            lease = Lease(job, name)
            self._free[name] = (cpu_after, memory_after)
            self._active[job] = lease, demand
            return lease

    def release(self, lease):
        """Only the exact current lease can return units."""
        if not isinstance(lease, Lease):
            raise ValueError("expected Lease")
        with self._lock:
            record = self._active.get(lease.job)
            if record is None or record[0] is not lease:
                return False
            cpu, memory = self._free[lease.worker]
            self._free[lease.worker] = (cpu + record[1].cpu, memory + record[1].memory)
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
            if self._draining:
                # A legal overlapping attempt must not close this worker either.
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

    def _convert_hold(self, job_holds, holds, job):
        """Turn one job's hold into occupancy without changing target free units."""
        target, cpu, memory = job_holds.pop(job)
        held = holds[target]
        held[0] -= cpu
        held[1] -= memory
        if held == [0, 0]:
            del holds[target]
