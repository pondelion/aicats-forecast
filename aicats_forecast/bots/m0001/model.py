import pickle
import os


class Model:

    def __init__(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self._clf = pickle.load(
            open(os.path.join(base_dir, 'rf_model_target_48h.pkl'), 'rb')
        )

    def predict(self, X):
        return self._clf.predict(X)

