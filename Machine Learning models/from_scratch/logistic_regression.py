import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt
from sklearn import datasets

def sigmoid(z):
    return 1.0 / (1.0 + np.exp(-z))

def hypothesis(x, theta):
    return sigmoid(x @ theta)

def cost_function(X, Y, theta):
    cost = 0.0
    m = X.shape[0]
    for i in range(0, m):
        cost += -Y[i] * np.log(hypothesis(X[i], theta)) - (1-Y[i]) * np.log(1-hypothesis(X[i], theta))
    return cost/m


if __name__ == "__main__":

    samples = 200
    X, Y = datasets.make_blobs(n_samples=samples, centers=2, n_features=2, cluster_std=1.2, random_state=4)
    X = np.append(np.ones((samples, 1)), X, axis= 1)

    # plot data
    plt.figure(1)
    plt.scatter(X[:,1], X[:,2], c=Y)

    iterations = 10000
    learning_rate = 0.05
    theta_old = np.zeros((X.shape[1], 1))
    theta_new = np.zeros((X.shape[1], 1))
    J = np.zeros((iterations, 1))

    for iter in range(0, iterations):

        J[iter] = cost_function(X, Y, theta_old)

        temp = np.zeros((theta_new.shape[0], 1))

        for i in range(0, X.shape[1]):
            for j in range(0, X.shape[0]):
                temp[i] += ((hypothesis(X[j], theta_old) - Y[j]) * X[j][i])
        
        theta_new = theta_old - learning_rate * (temp/samples)
        theta_old = theta_new
    
    # plot decision boundary
    xp = np.array([X[:,1].min(), X[:,1].max()])
    yp0 = -theta_new[1]/theta_new[2] * xp[0] - theta_new[0]/theta_new[2]
    yp1 = -theta_new[1]/theta_new[2] * xp[1] - theta_new[0]/theta_new[2]
    yp = np.array([yp0, yp1])
    plt.plot(xp, yp, 'r')
    plt.show()

    # plot cost function
    plt.figure(2)
    plt.plot(J)
    plt.show()