import multiprocessing
import multiprocessing as mp
import os
import traceback
from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple


# Parent class for all steps
# function, kwargs, on_error and await_output
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
        # abstract (can return something or not
        ...


# function, kwargs and on_error
class SequentialStep(Step):
    def process(self):
        return self.function(**self.func_kwargs)

    def generate_error(self) -> Tuple[str, str]:
        header, err_trace = super().generate_error()

        header = f"SequentialStep\n{'-'*20}\n" + header

        return header, err_trace


# abstract class for MT and MP Steps
class ParallelStep(Step):
    def __init__(self, split_parameter: str = "", *args, **kwargs) -> None:
        """_summary_

        Args:
            split_parameter (str, optional): _description_. Defaults to "".
        """
        super().__init__(*args, **kwargs)
        self.split_parameter = split_parameter


class MPStep(ParallelStep):
    def __init__(self, processes_qtd: int = mp.cpu_count, *args, **kwargs) -> None:
        """_summary_

        Args:
            processes_qtd (int, optional): _description_. Defaults to mp.cpu_count.
        """

        super().__init__(*args, **kwargs)
        self.processes_qtd = processes_qtd

    def process(self) -> Any:
        if self.split_parameter:
            if self.split_parameter not in self.func_kwargs:
                raise ValueError(f"'{self.split_parameter}' not found in kwargs")

            param_chunks = self.func_kwargs[self.split_parameter]
            if not isinstance(param_chunks, list):  # check if its an iterable
                raise ValueError(f"'{self.split_parameter}' in kwargs is not iterable")

            param_chunks_dict = {
                {self.split_parameter: chunk_value} for chunk_value in param_chunks
            }

            with mp.Pool(processes=self.processes_qtd) as pool:
                results = pool.starmap(
                    self.start_step_wrapper,
                    [
                        (
                            self.function,
                            {**self.func_kwargs, **chunk_dict},
                            self.previous_phase_result,
                        )
                        for chunk_dict in param_chunks_dict
                    ],
                )
        else:
            with mp.Pool(processes=self.processes_qtd) as pool:
                results = pool.starmap(
                    self.start_step_wrapper,
                    [
                        (self.function, self.func_kwargs, self.previous_phase_result)
                        for _ in range(self.processes_qtd)
                    ],
                )

        return results

    def generate_error(self) -> Tuple[str, str]:
        super_header, err_trace = super().generate_error()

        mp_header = f"""
        SequentialStep\n
        {'-'*20}\n
        processes_qtd= {self.processes_qtd}\n
        split_parameter= {self.split_parameter}
        {"-"*20}\n
        """

        mp_header += super_header

        return mp_header, err_trace


class MTStep(ParallelStep):
    def __init__(self) -> None:
        pass


class Phase:
    def __init__(
        self,
        steps: List[Step],
        parallel_method: str = "none",
        phase_name: str = "phase",
    ):
        self.steps = steps
        self.parallel_method = parallel_method
        self.phase_name = phase_name

    def start_phase(
        self, previous_phase_result: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        if previous_phase_result is None:
            previous_phase_result = {}

        if self.parallel_method == "none":
            step_result_list = []
            for idx, step in enumerate(self.steps):
                step_result = self.start_step_wrapper(step, previous_phase_result, idx)
                step_result_list.append(step_result)

            return dict(step_result_list)

        elif self.parallel_method == "multiprocessing":
            nr_cpus = multiprocessing.cpu_count()
            if len(self.steps) < nr_cpus:
                nr_cpus = len(self.steps)

            with multiprocessing.Pool(processes=nr_cpus) as pool:
                # Create a list of tuples with each step and its previous_phase_result
                step_result_list = pool.starmap(
                    self.start_step_wrapper,
                    [
                        (step, previous_phase_result.copy(), idx)
                        for idx, step in enumerate(self.steps)
                    ],
                )

                # Convert the list of tuples to a dictionary
            return dict(step_result_list)

        else:
            raise ValueError(f"Unknown parallel method: {self.parallel_method}")

    def start_step_wrapper(
        self, step: Step, previous_phase_result: Dict[str, Any], step_idx: int
    ) -> Tuple[str, Any]:
        if "previous_phase_result" in step.func_kwargs.keys():
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
            self.generate_phase_log(phase.phase_name, phase_result)

            if self.return_all_phase_results or idx == len(self.phases) - 1:
                pipeline_results[f"{phase.phase_name}{idx}"] = phase_result

            previous_phase_result = phase_result

        return pipeline_results

    def generate_phase_log(self, phase_name: str, phase_result: Dict[str, Any]) -> None:
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
            for step_name, step_result in phase_result.items():
                f.write(f"{step_name}\n")
                f.write(f"{step_result}\n\n")


def main():
    # TODO: fix documentation and add tests
    def my_other_func(previous_phase_result: Dict[str, Any]):
        if previous_phase_result.get("my_other_func1"):
            return previous_phase_result["my_other_func1"] * 3
        return previous_phase_result["my_func0"] * 3

    def my_func(x: int) -> int:
        return x * 3

    step1 = SequentialStep(my_func, func_kwargs={"x": 3}, on_error="stop")
    step2 = SequentialStep(
        my_other_func, func_kwargs={"previous_phase_result": 3}, on_error="stop"
    )

    phase1 = Phase([step1], parallel_method="multiprocessing")
    phase2 = Phase([step1, step2, step1], parallel_method="multiprocessing")
    phase3 = Phase([step2], parallel_method="multiprocessing")

    from pprint import pprint

    pprint(PipeLine(phases=[phase1, phase2, phase3]).start_pipeline())


if __name__ == "__main__":
    main()
