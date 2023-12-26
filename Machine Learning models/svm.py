import numpy as np
from sklearn.svm import SVC
from sklearn.datasets import make_blobs
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix

X, y = make_blobs(n_samples=200, centers=2, random_state=2)
train_X, test_X, train_y, test_y = train_test_split(X, y, test_size=0.2)

C = 1.0
svc = SVC(kernel='linear', C=C)
svc.fit(train_X, train_y)

plt.scatter(train_X[:,0], train_X[:,1], c=train_y)
ax = plt.gca()
xlim = ax.get_xlim()

w = svc.coef_[0]
slope = -w[0] / w[1]
xx = np.linspace(xlim[0], xlim[1])
yy = slope * xx - (svc.intercept_[0] / w[1])
plt.scatter(test_X[:,0], test_X[:,1], c=test_y, cmap='winter', marker='s')
plt.plot(xx, yy)
plt.show()

pred_y = svc.predict(test_X)
print(confusion_matrix(test_y, pred_y))