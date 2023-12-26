import numpy as np
import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.cluster import KMeans 

X, y = datasets.make_blobs(n_samples=200, centers=3,random_state=0)

kmeans = KMeans(n_clusters=3).fit(X)

plt.figure(1)
plt.scatter(X[kmeans.labels_==0, 0], X[kmeans.labels_==0, 1], c='red')
plt.scatter(X[kmeans.labels_==1, 0], X[kmeans.labels_==1, 1], c='blue')
plt.scatter(X[kmeans.labels_==2, 0], X[kmeans.labels_==2, 1], c='green')
plt.scatter(kmeans.cluster_centers_[:,0], kmeans.cluster_centers_[:,1], c='black')
plt.show()

plt.figure(2)
J = np.zeros((10, 1))
for i in range(0, 10):
    kmeans = KMeans(n_clusters=i+1).fit(X)
    J[i] = kmeans.inertia_
plt.plot(range(0, 10), J)
plt.show()