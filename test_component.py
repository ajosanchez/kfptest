# pytest --capture=no -s --log-cli-level=INFO test_component.py

from typing import NamedTuple

from kfp.dsl import component, Input, Output, Dataset

from kfptest import TestStep, ContainerTestStep


@component(base_image="pipelines:latest")
def example_component(
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


def test_example_component(tmp_path):
  component_args = {
    "firstname": "Jeffrey",
    "lastname": "Lebowski",
    "input_dataset": [1,2,3,4,5]
  }

  test_step = ContainerTestStep(example_component, component_args, tmp_path)
  test_run = test_step.run()

  assert test_run.obj["full_name"] == "Jeffrey Lebowski"
  assert test_run.obj["squared_dataset"] == [1, 4, 9, 16, 25]

