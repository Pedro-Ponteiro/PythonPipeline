import multiprocessing as mp
import traceback
from typing import Any, Callable, Dict, List, Tuple


# Parent class for all steps
# function, kwargs and on_error
class Step:
    def __init__(
        self,
        function: Callable[[Any], Any],
        kwargs: Dict[str, Any],
        on_error: str = "stop",
        await_output: bool = True,
    ) -> None:
        self.function = function
        self.kwargs = kwargs.copy()
        self.on_error = on_error
        self.await_output = await_output

    def start_step(self) -> Any:
        try:
            return self.process()
        except Exception:
            err_header, err_trace = self.generate_error()
            err_msg = err_header + err_trace

            if self.on_error == "silentlycontinue":
                return err_msg
            if self.on_error == "continue":
                print(err_msg)
                return err_msg
            elif self.on_error == "stop":
                raise err_msg

    def generate_error(self) -> Tuple[str, str]:
        err_header = f"""function= {self.function.__name__}\n
                         kwargs= {self.kwargs.keys()}\n
                         on_error= {self.on_error}\n]"""
        err_trace = f"Traceback= \n{traceback.format_exc()}"
        return err_header, err_trace

    def process(self):
        # abstract
        ...


# function, kwargs and on_error
class SequentialStep(Step):
    def process(self):
        return self.function(**self.kwargs)


setp = SequentialStep(lambda x: print(x), {"x": 1}).start_step()
print(setp)
exit()


# abstract class for MT and MP Steps
class ParallelStep(Step):
    def __init__(self) -> None:
        pass


class MTStep(ParallelStep):
    def __init__(self) -> None:
        pass


class MPStep(ParallelStep):
    def __init__(self) -> None:
        pass


def step(
    function: Callable[[Any], Any],
    kwargs: Dict[str, Any],
    parallel_method: str = "mp",  # "mp", "mt" or ""
    processes_qtd: int = mp.cpu_count,
    threads_qtd: int = 4,
    on_error: str = "stop",  # "stop", "silentlycontinue" or "continue"
    split_parameter: str = "",  # name of the parameter to split (must be an iterable)
    # (each piece will be passed on to a thread/process)
):
    ...


def pipe(
    steps: List[step],
    synchronous_steps: bool = True,
    background_process: bool = False,
    log_folder: str = "logs",
):
    ...
