# GoStream - A library for writing scalable data streaming workflows #

## Overview ##
GoStream is a library for writing data streaming workflows using the [Flow-based programming paradigm](https://en.wikipedia.org/wiki/Flow-based_programming). Although other libraries exist, GoStream provides the following advantages

* A set of standard and abstract operators for writing scalable workflows [see our [CVPR'14 Paper](http://www.cv-foundation.org/openaccess/content_cvpr_workshops_2014/W20/papers/Helala_A_Stream_Algebra_2014_CVPR_paper.pdf)].
* A simple programming interface for constructing workflow graphs [see our [ICDSC'16 Paper](--)].
* An automatic end-to-end throughput versus latency optimization of workflow performance.
* Enabling feedback loops for performance and parameter tuning [see our [UCCV'14 Paper](http://vclab.ca/wp-content/papercite-data/pdf/14-uccv-w.pdf)].
* Runtime switching between different data processing algorithms for the same operator [see our [ICDSC'16 Paper](--)].

We are also working on adding the following features:

* Adding support to distribute workflows using Docker Swarm.
* Integration with the [NATS High-Performance server](https://github.com/nats-io/gnatsd).
* Elastic scaling of workflows while enabling efficient utilization of cloud resources using our end-to-end throughput versus latency optimization module.

## GoStream Module for Computer Vision

We provide a module for programming online computer vision algorithms processing image and video streams. The module supports the following features,

* GUI windows for displaying images or controlling workflows using [Go-GTK](https://github.com/mattn/go-gtk)
* GNU Plots for displaying dynamic charts using [Go-Gnuplot](https://github.com/sbinet/go-gnuplot)
* OpenCV support using [Go-OpenCV](https://github.com/lazywei/go-opencv)

## GoStream Module for Machine Learning

Learning from data streams is called [data stream mining](https://en.wikipedia.org/wiki/Data_stream_mining) and it aims to extract information models and structures from continuous and possibly infinite streams. We are currently supporting the following algorithms:

* Online Hierarchical Clustering [Kanen et al., ICDM'09](http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=5360250&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D5360250)
* Bayesian Optimization [See [Snoek et al. paper](https://arxiv.org/pdf/1206.2944.pdf)]

## GoStream Module for Automatic Algorithm Configuration

As workflow streaming operators implements different algorithms, it becomes hard to manually identify the best parameter settings for the best performance of every algorithm. The algorithm configuration module implements automatic methods for adaptive parameters selection and tuning. In literature, the problem is refered to by [Self-tuning](https://en.wikipedia.org/wiki/Self-tuning) and [Autonomic computing](https://en.wikipedia.org/wiki/Autonomic_computing). The module provides the following features,

* An implementation of the the time-bounded sequential parameter optimization algorithm [see [Hutter et al., LION'10](http://www.cs.ubc.ca/labs/beta/Projects/SMAC/papers/10-LION-TB-SPO.pdf)].
* Examples showing the integration of parameter optimization with feedback control.




## Install ##

###Linux & Mac OS X

```
go get github.com/helala/gostream
cd $GOPATH/src/github.com/helala/gostream/samples
go run word-count.go
