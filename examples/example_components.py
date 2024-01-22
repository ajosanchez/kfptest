# pytest --capture=no -s --log-cli-level=INFO test_component.py

from typing import NamedTuple

from kfp.dsl import component, Input, Output, Dataset

from kfptest import TestStep, ContainerTestStep


@component(base_image="python:3.10-slim")
def named_output_component(
    firstname: str, 
    lastname: str,
    input_dataset: Input[Dataset], 
    output_dataset: Output[Dataset]) -> NamedTuple(
    "Outputs",
    [
        ("full_name", str),
        ("squared_dataset", str),
    ]):
    from collections import namedtuple
    import pickle

    with open(input_dataset.path, "rb") as f:
        input_data = pickle.load(f)

    z = [x**2 for x in input_data]

    with open(output_dataset.path, "wb") as f:
        pickle.dump(z, f)

    outputs = namedtuple("Outputs", ["full_name", "squared_dataset"])
    return outputs(f"{firstname} {lastname}", z)

@component(base_image="python:3.10-slim")
def non_named_output_component(
    input_list: Input[Dataset], 
    squared_list: Output[Dataset]):
    import pickle

    with open(input_list.path, "rb") as f:
        input_data = pickle.load(f)

    z = [x**2 for x in input_data]

    with open(squared_list.path, "wb") as f:
        pickle.dump(z, f)

@component(base_image="python:3.10-slim")
def chained_component_1(
    input_list: Input[Dataset], 
    squared_list: Output[Dataset]):
    import pickle

    with open(input_list.path, "rb") as f:
        input_data = pickle.load(f)

    z = [x**2 for x in input_data]

    with open(squared_list.path, "wb") as f:
        pickle.dump(z, f)

@component(base_image="python:3.10-slim")
def chained_component_2(
    squared_list: Input[Dataset], 
    halved_list: Output[Dataset]):
    import pickle

    with open(squared_list.path, "rb") as f:
        input_data = pickle.load(f)

    z = [x/2 for x in input_data]

    with open(halved_list.path, "wb") as f:
        pickle.dump(z, f)
