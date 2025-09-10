import pickle, base64
from prefect.blocks.core import Block
from pydantic import Field


class PickleBlock(Block):
    """Block to save model in pickle format."""
    data_b64: str = Field(..., description="Pickled model base64")

    @staticmethod
    def save_model(model, name: str, overwrite: bool = True):
        # pickle -> base64 string
        data_b64 = base64.b64encode(pickle.dumps(model)).decode('ascii')
        block = PickleBlock(data_b64=data_b64)
        block.save(name, overwrite=overwrite)
        return block

    def load_model(self):
        # base64 string -> bytes -> unpickle
        return pickle.loads(base64.b64decode(self.data_b64))