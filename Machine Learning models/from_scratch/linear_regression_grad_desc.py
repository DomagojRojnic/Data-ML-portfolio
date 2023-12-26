import numpy as np
import matplotlib.pyplot as plt
from sklearn import datasets

def hypothesis(x, theta):
    return theta[0] + theta[1] * x

def cost_function(X, Y, theta):
    J = 0.0
    m = X.shape[0]
    for i in range(0, m):
        J += (hypothesis(X[i], theta) - Y[i]) ** 2
    return J/(2*m)


if __name__ == "__main__":

    X, Y = datasets.make_regression(n_samples=100,n_features=1,
                                    n_informative=1, noise=10.0, coef=False,
                                    random_state=0)
    iterations = 200
    learning_rate = 0.1
    theta_old = np.zeros((2,1))
    theta_new = np.zeros((2,1))
    J = np.zeros((iterations, 1))

    xp = np.array([X.min(), X.max()])
    yp = np.array([0,0])

    plt.figure(1)
    plt.title('Gradient descent')
    plt.scatter(X, Y, marker='.')

    for iter in range(0, iterations):

        J[iter] = cost_function(X, Y, theta_old)

        temp0 = 0.0
        temp1 = 0.0

        for i in range(0, X.shape[0]):
            temp0 += hypothesis(X[i], theta_old) - Y[i]
            temp1 += (hypothesis(X[i], theta_old) - Y[i]) * X[i]
        temp0 /= X.shape[0]
        temp1 /= X.shape[0]

        theta_new[0] = theta_old[0] - learning_rate * temp0
        theta_new[1] = theta_old[1] - learning_rate * temp1

        theta_old = theta_new

    yp[0] = hypothesis(xp[0],theta_new)
    yp[1] = hypothesis(xp[1],theta_new)

    plt.plot(xp, yp, 'r')
    plt.show()    
    
    plt.figure(2)
    plt.plot(J)
    plt.show()