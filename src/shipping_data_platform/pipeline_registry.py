"""Project pipelines."""
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from shipping_data_platform.pipelines.data_extracting_and_cleaning import create_pipeline as data_extracting_and_cleaning

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    
    return {
        "__default__": data_extracting_and_cleaning(),
        "data_extracting_and_cleaning": data_extracting_and_cleaning()
    }
