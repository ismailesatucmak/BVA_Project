"""Peak memory probe shared by S4 (ML) and S5 (processing benchmark).

Measures the resident set size (RSS) of a process *and its descendants* with a
background sampling thread. For PySpark the real work happens in the JVM, which is a
child process of the Python driver (python -> spark-submit -> java); measuring only the
Python process (``psutil.Process().memory_info()``) would miss it entirely.

Usage::

    with PeakRSS() as probe:
        model.fit(...)
    probe.peak_total_mb, probe.peak_jvm_mb, probe.start_total_mb
"""
from __future__ import annotations

import threading
import time

import psutil

MB = 1024 * 1024
DEFAULT_INTERVAL_S = 0.05


def _is_jvm(proc: psutil.Process) -> bool:
    try:
        return proc.name().lower().startswith("java")
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def tree_rss_mb(root: psutil.Process | None = None) -> tuple[float, float]:
    """Return (total RSS of root + all descendants, RSS of JVM descendants) in MB."""
    root = root or psutil.Process()
    total = 0.0
    jvm = 0.0
    procs = [root]
    try:
        procs += root.children(recursive=True)
    except psutil.NoSuchProcess:
        pass
    for proc in procs:
        try:
            rss = proc.memory_info().rss / MB
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        total += rss
        if _is_jvm(proc):
            jvm += rss
    return total, jvm


class PeakRSS:
    """Context manager that records the peak RSS of the current process tree."""

    def __init__(self, interval_s: float = DEFAULT_INTERVAL_S, root: psutil.Process | None = None):
        self.interval_s = interval_s
        self.root = root or psutil.Process()
        self.start_total_mb = 0.0
        self.start_jvm_mb = 0.0
        self.peak_total_mb = 0.0
        self.peak_jvm_mb = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _sample(self) -> None:
        total, jvm = tree_rss_mb(self.root)
        self.peak_total_mb = max(self.peak_total_mb, total)
        self.peak_jvm_mb = max(self.peak_jvm_mb, jvm)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._sample()
            time.sleep(self.interval_s)

    def __enter__(self) -> "PeakRSS":
        self.start_total_mb, self.start_jvm_mb = tree_rss_mb(self.root)
        self.peak_total_mb, self.peak_jvm_mb = self.start_total_mb, self.start_jvm_mb
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self._sample()  # final reading so very short blocks still get one sample
