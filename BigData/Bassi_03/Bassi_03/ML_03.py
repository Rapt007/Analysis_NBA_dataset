# Assignment-3(K-means clustering)
# Bassi, Aman
# 1001393217
# importing modules
from __future__ import division
import numpy as np
import random


# loading the iris data set in the form of the np array
data = np.loadtxt("irisdata.txt", delimiter=",")

# training data which are basically 4 columns
train_data = data[:, 0:4]

# labels
labels = data[:, 4]

# k in k-means algorithm
k = 3

# generating random k indexes in the range of train_data length
indexes = random.sample(range(0, 150), k)

# getting the points from train_data at those random indexes and taking it as initial centroid
initial_random_points = train_data[indexes, :]

# initialising an empty cluster
cluster = np.zeros((1, len(train_data)))
# print (cluster.shape)

# initialising an empty array for new centroid
centroid_new = np.zeros((k, 4))


# function for computing new centroid by taking the mean of every column
def computing_centroid_each_cluster(cluster):
    for row in range(len(centroid_new)):
        cluster_rows = np.where(cluster == row)
        centroid_new[row, :] = np.mean(train_data[cluster_rows[1][:], :], axis=0)
    return centroid_new


# creating the clusters
def getting_clusters(initial_random_points):
    for index, i in enumerate(train_data):
        list_dist = []
        for j in initial_random_points:
            dist = np.sqrt(sum(np.square(i - j)))
            list_dist.append(dist)
        # print(list_dist)
        minimum_index = list_dist.index(min(list_dist))
        cluster[0, index] = minimum_index
    return cluster


# assigning the old cluster
def assigning(cluster):
    cluster_old = np.empty_like(cluster)
    cluster_old[:] = cluster


# assigning the centroid
def assigning_centroid(centroid_new):
    initial_random_points[:] = centroid_new


# getting accuracy
def evaluation(cluster):
    count = 0
    for i in range(len(labels)):
        if labels[i] == cluster[0, i]:
            count += 1
    accuracy = (count/150)*100
    return accuracy


# main function
if __name__ == '__main__':
    while True:
        cluster_old = assigning(cluster)
        # checking for all the points and making clusters
        cluster_new = getting_clusters(initial_random_points)
        centroid_new = computing_centroid_each_cluster(cluster)
        if np.array_equal(initial_random_points,centroid_new)==True:
            break
        else:
            assigning_centroid(centroid_new)
    accuracy = evaluation(cluster_new)
    print(accuracy)







