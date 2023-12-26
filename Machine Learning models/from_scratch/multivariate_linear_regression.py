import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt
from sklearn import datasets

def hypothesis(x, theta):
    return x @ theta

def cost_function(X, Y, theta):
    J = 0.0
    m = X.shape[0]
    for i in range(0, m):
        J += (hypothesis(X[i], theta) - Y[i]) ** 2
    return J/(2*m)


if __name__ == "__main__":

    samples = 200
    X, Y = datasets.make_regression(n_samples=samples,n_features=10,
                                    n_informative=10, noise=10.0, coef=False,
                                    random_state=0)
    X = np.append(np.ones((samples, 1)), X, axis= 1)
    Y = np.reshape(Y, (samples, 1))

    iterations = 200
    learning_rate = 0.1
    
    J = np.zeros((iterations, 1))
    theta_old = np.zeros((X.shape[1], 1))
    theta_new = np.zeros((X.shape[1], 1))

    for iter in range(0, iterations):

        J[iter] = cost_function(X, Y, theta_old)

        temp = np.zeros((theta_new.shape[0], 1))
        for i in range(0, X.shape[1]):
            for j in range(0, X.shape[0]):
                temp[i] += ((hypothesis(X[j], theta_old) - Y[j]) * X[j][i])
        
        theta_new = theta_old - learning_rate * (temp/X.shape[0])

        theta_old = theta_new

    xp = np.array([X.max(), X.min()])
    yp = np.array([theta_new[0] + xp[0]* theta_new[1], theta_new[0] + xp[1]* theta_new[1]])
    plt.figure(1)
    plt.plot(xp, yp)
    plt.show()