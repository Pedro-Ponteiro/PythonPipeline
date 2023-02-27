import multiprocessing as mp
from typing import Any, Callable, Dict, List


def step(
    function: Callable[[Any], Any],
    kwargs: Dict[str, Any],
    parallel_method: str = "mp",  # "mp", "mt" or ""
    processes_qtd: int = mp.cpu_count,
    threads_qtd: int = 4,
    on_error: str = "stop",  # "stop", "silentlycontinue" or "continue"
    split_parameter: str = "",  # name of the parameter to split (must be an iterable)
    # (each piece will be passed to a thread/process)
    log_folder: str = "logs",
):
    ...


def pipe(
    steps: List[step], synchronous_steps: bool = True, background_process: bool = False
):
    ...
