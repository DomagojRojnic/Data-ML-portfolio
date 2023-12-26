import numpy as np
import matplotlib.pyplot as plt
from sklearn import datasets

def hypothesis(x, theta):
    return theta[0] + theta[1] * x


if __name__ == "__main__":

    x, Y = datasets.make_regression(n_samples=100,n_features=1,
                                    n_informative=1, noise=10.0, coef=False,
                                    random_state=0)
    plt.figure(1)
    plt.title('Analitical solution')
    plt.scatter(x, Y, marker='.')
    
    X = np.ones((100,1))
    X = np.append(X, x, axis=1)

    xp = np.array([X.min(), X.max()])
    yp = np.array([0,0])

    theta_direct = np.linalg.inv(np.transpose(X) @ X) @ np.transpose(X) @ Y
    yp[0] = hypothesis(xp[0],theta_direct)
    yp[1] = hypothesis(xp[1],theta_direct)

    plt.plot(xp,yp, c='r')
    plt.show()