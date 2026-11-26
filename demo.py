from pathlib import Path
from tempfile import TemporaryDirectory

from placement import Demand, Pool, Worker


def run():
    with TemporaryDirectory() as tmp:
        roots = {name: Path(tmp) / name for name in ("small", "large")}
        for directory in roots.values():
            directory.mkdir()
            (directory / "identity").write_text(directory.name)
        (roots["small"] / "model.txt").write_text("local-model-v1")
        workers = [
            Worker(name, cpu, memory, frozenset({"local-model"})
                   if (roots[name] / "model.txt").is_file() else frozenset())
            for name, cpu, memory in (("small", 2, 4), ("large", 4, 8))
        ]
        pool = Pool(workers)
        batch = pool.reserve("batch", Demand(1, 3))
        inference = Demand(1, 2, frozenset({"local-model"}))
        blocked = pool.reserve("infer", inference)
        fallback = pool.reserve("other", Demand(1, 2))
        assert blocked is None and fallback.worker == "large"
        identity = (roots[fallback.worker] / "identity").read_text()
        assert pool.release(batch)
        landed = pool.reserve("infer", inference)
        model = (roots[landed.worker] / "model.txt").read_text()
        return blocked, identity, landed.worker, model, pool.available()


if __name__ == "__main__":
    blocked, fallback, worker, model, free = run()
    print(f"labelled job before release: {blocked}; unlabelled job: {fallback}")
    print(f"labelled job after release: {worker}, read {model}; free(cpu,memory): {free}")
