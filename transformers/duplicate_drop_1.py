from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: ActionType.DROP_DUPLICATE

    Docs: https://docs.mage.ai/guides/transformer-blocks#drop-duplicates
    
    """
    df=df.sort_values('updated_at')
    action = build_transformer_action(
        df,
        action_type=ActionType.DROP_DUPLICATE,
        arguments=["_key"],  # Specify column names to use when comparing duplicates
        axis=Axis.ROW,
        options={'keep': 'last'},  # Specify whether to keep 'first' or 'last' duplicate
    )

    return BaseAction(action).execute(df)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
