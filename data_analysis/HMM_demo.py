import numpy as np
from hmmlearn import hmm


seq = np.array([1, 0, 1, 0, 1, 1, 0, 1, 0, 1])
seq.shape = (len(seq), 1)

model = hmm.MultinomialHMM(n_components=3)
np.random.seed(1)
model.fit(0)
print model.emissionprob_
print model.transmat_

print model.predict(0)
print model.predict_proba(0)

