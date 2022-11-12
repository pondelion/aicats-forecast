import pickle
import os


class PostProcess:

    def __init__(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self._y_scaler = pickle.load(
            open(os.path.join(base_dir, 'y_h48_scaler.pkl'), 'rb')
        )

    def post_process(self, pred):
        return self._y_scaler.inverse_transform(pred)[0][0]
