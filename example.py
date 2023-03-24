from typing import Any, Dict

from Pipeline import Phase, PipeLine, Step


def my_other_func(previous_phase_result: Dict[str, Any]):
    if previous_phase_result.get("my_other_func1"):
        return previous_phase_result["my_other_func1"] * 3
    return previous_phase_result["my_func0"] * 3


def my_func(x: int) -> int:
    return print


def main():
    # TODO: fix documentation and add tests

    step1 = Step(my_func, func_kwargs={"x": 3}, on_error="stop")
    step2 = Step(my_other_func, func_kwargs={}, on_error="silentlycontinue")
    step3 = Step(my_func, func_kwargs={"x": 3}, on_error="stop")

    phase1 = Phase(
        [step1], parallel_method_name="multiprocessing", phase_name="teste_loco"
    )
    phase2 = Phase([step1, step2, step1], parallel_method_name="multiprocessing")
    phase3 = Phase([step2], parallel_method_name="multiprocessing")
    phase4 = Phase([step3, step3, step3, step3], parallel_method_name="multithreading")
    phase5 = Phase([step3], parallel_method_name="none")

    # from pprint import pprint

    # pprint(
    PipeLine(phases=[phase1, phase2, phase3, phase4, phase5]).start_pipeline()


if __name__ == "__main__":
    main()
