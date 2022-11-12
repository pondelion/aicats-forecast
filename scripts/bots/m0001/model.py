import time

from aicats_forecast.bots.m0001.preprocess import FeatureExtractor
from aicats_forecast.bots.m0001.model import Model
from aicats_forecast.bots.m0001.postprocess import PostProcess


extractor = FeatureExtractor()
model = Model()

df_feat = extractor.extract()
print(df_feat)

pred = model.predict(df_feat)
print(f'pred : {pred}')

post_process = PostProcess()
pred_transformed = post_process.post_process(pred.reshape(1, -1))

print(pred_transformed)
