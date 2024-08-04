"""
This is a boilerplate pipeline 'data_extracting_and_cleaning'
generated using Kedro 0.19.7
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import clean_containers_data, clean_operations_data

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
                func=clean_containers_data,
                inputs="containers",
                outputs="containers_raw",
            ),
        node(
                func=clean_operations_data,
                inputs="operations",
                outputs="operations_raw",
            )
    ])
