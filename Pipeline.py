import multiprocessing
import multiprocessing as mp
import os
import traceback
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Any, Callable, Dict, List, Tuple

# TODO: add support for containers and execution at other computers


class Step:
    def __init__(
        self,
        function: Callable[[Any], Any],
        func_kwargs: Dict[str, Any],
        on_error: str = "stop",
    ) -> None:
        self.function = function
        self.func_kwargs = func_kwargs.copy()
        self.on_error = on_error

    def start_step(self) -> Any:
        try:
            return self.process()
        except Exception:
            err_header, err_trace = self.generate_error()
            err_msg = err_header + err_trace

            if self.on_error == "silentlycontinue":
                return err_msg
            if self.on_error == "continue":
                print(err_msg)  # TODO: change to logging module
                return err_msg
            elif self.on_error == "stop":
                raise Exception(err_msg)

    def generate_error(self) -> Tuple[str, str]:
        err_header = f"""
        function= {self.function.__name__}\n
        {"-"*20}\n
        kwargs= {self.func_kwargs.keys()}\n
        {"-"*20}\n
        on_error= {self.on_error}\n
        {"-"*20}\n
        """
        err_trace = f"Traceback= \n{traceback.format_exc()}"
        return err_header, err_trace

    def process(self) -> Any:
        return self.function(**self.func_kwargs)


class Phase:
    def __init__(
        self,
        steps: List[Step],
        parallel_method_name: str = "none",
        phase_name: str = "phase",
        nr_processes: int = mp.cpu_count(),
        nr_threads: int = None,
    ):
        self.steps = steps
        self.phase_name = phase_name
        self.nr_processes = nr_processes
        self.parallel_method_name = parallel_method_name

        if nr_threads is None:
            self.nr_threads = len(steps)
        else:
            self.nr_threads = nr_threads
        # TODO: ADD asynchrounous parallel method

        self.parallel_method_map: Dict[str, Callable[[], Any]] = {
            "multiprocessing": self.execute_multiprocess,
            "multithreading": self.execute_multithread,
            "none": self.execute_sequential,
        }
        self.parallel_method = self.parallel_method_map.get(parallel_method_name)

        if self.parallel_method is None:
            raise ValueError(f"Unknown parallel method: {self.parallel_method}")

    def execute_sequential(self):
        step_result_list = []
        for idx, step in enumerate(self.steps):
            step_result = self.start_step_wrapper(
                step, self.previous_phase_result.copy(), idx
            )
            step_result_list.append(step_result)

        return dict(step_result_list)

    def execute_multiprocess(self):
        with multiprocessing.Pool(processes=self.nr_processes) as pool:
            # Create a list of tuples with each step and its previous_phase_result
            step_result_list = pool.starmap(
                self.start_step_wrapper,
                [
                    (step, self.previous_phase_result.copy(), idx)
                    for idx, step in enumerate(self.steps)
                ],
            )

        # Convert the list of tuples to a dictionary
        return dict(step_result_list)

    def execute_multithread(self):
        with ThreadPool(processes=self.nr_threads) as pool:
            step_result_list = pool.starmap(
                self.start_step_wrapper,
                [
                    (step, self.previous_phase_result.copy(), idx)
                    for idx, step in enumerate(self.steps)
                ],
            )

        return dict(step_result_list)

    def start_phase(
        self, previous_phase_result: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        if previous_phase_result is None:
            previous_phase_result = {}

        self.previous_phase_result = previous_phase_result

        return self.parallel_method()

    def start_step_wrapper(
        self, step: Step, previous_phase_result: Dict[str, Any], step_idx: int
    ) -> Tuple[str, Any]:
        if "previous_phase_result" in step.function.__annotations__.keys():
            step.func_kwargs["previous_phase_result"] = previous_phase_result
        result = step.start_step()
        return f"{step.function.__name__}{step_idx}", result


class PipeLine:
    def __init__(
        self,
        phases: List[Phase],
        log_folder: str = "logs",
        return_all_phase_results: bool = True,
        pipeline_name: str = "pipeline",
    ) -> None:
        """_summary_

        Args:
            phases (List[Phase]): _description_
            log_folder (str, optional): _description_. Defaults to "logs".
            return_all_phase_results (bool, optional): _description_. Defaults to True.
            pipeline_name (str, optional): _description_. Defaults to "pipeline".
        """
        self.phases = phases
        self.log_folder = log_folder
        self.return_all_phase_results = return_all_phase_results
        self.pipeline_name = pipeline_name

    def start_pipeline(self) -> Dict[str, Dict[str, Any]]:
        """Executes phases in for loop.
        Each phase result is passed as a parameter for the next phase

        Returns:
            Dict[str, Dict[str, Any]]:
            Dict[key=phase_name, value=Dict[key=func_id, value=func_results]].
            Defaults to True.
        """
        previous_phase_result = None
        pipeline_results = {}

        for idx, phase in enumerate(self.phases):
            phase_result = phase.start_phase(previous_phase_result)
            self.generate_phase_log(
                phase.phase_name, phase_result, phase.parallel_method_name
            )

            if self.return_all_phase_results or idx == len(self.phases) - 1:
                pipeline_results[f"{phase.phase_name}{idx}"] = phase_result

            previous_phase_result = phase_result

        return pipeline_results

    def generate_phase_log(
        self,
        phase_name: str,
        phase_result: Dict[str, Any],
        phase_parallel_method_name: str,
    ) -> None:
        """Writes logs with phase errors and outputs
        the pipeline will have an unique folder inside self.log_folder
        "{log_folder}/{pipeline_name}_{datetime}/{phase_name}{nr_files}.txt"
        phases will have an unique txt file inside that folder

        Args:
            phase_name (str): phase.phase_name attribute
            phase_result (Dict[str, Any]): Phase result
        """
        log_folder = (
            f"{self.log_folder}/{self.pipeline_name}"
            + f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        os.makedirs(log_folder, exist_ok=True)

        nr_files = len(os.listdir(log_folder))

        with open(f"{log_folder}/{phase_name}{nr_files}.txt", "w") as f:
            f.write("Parallel Execution Method\n")
            f.write(phase_parallel_method_name + "\n")
            f.write("-" * 20 + "\n")
            for step_name, step_result in phase_result.items():
                f.write(f"Step Name:\n{step_name}\n")
                f.write(f"Step Result:\n{step_result}\n\n")
                f.write("-" * 20 + "\n\n")
