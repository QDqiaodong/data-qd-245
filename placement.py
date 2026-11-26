"""Resource placement and fenced release of in-process worker reservations."""
from dataclasses import dataclass
from threading import Lock


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

    def reserve(self, job, demand):
        """Atomically reserve both dimensions, or return None without consuming either."""
        if type(job) is not str or not job or not isinstance(demand, Demand):
            raise ValueError("invalid job or demand")
        with self._lock:
            if job in self._active:
                raise ValueError("job already has an active reservation")
            candidates = []
            for name, (cpu, memory) in self._free.items():
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
        with self._lock:
            return dict(self._free)
