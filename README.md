# mumu-flink 分布式数据计算平台  
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mumuhadoop/mumu-flink/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/mumuhadoop/mumu-flink.svg?branch=master)](https://travis-ci.org/mumuhadoop/mumu-flink)
[![codecov](https://codecov.io/gh/mumuhadoop/mumu-flink/branch/master/graph/badge.svg)](https://codecov.io/gh/mumuhadoop/mumu-flink)

Apache Flink是一个面向分布式数据流处理和批量数据处理的开源计算平台，它能够基于同一个Flink运行时（Flink Runtime），
提供支持流处理和批处理两种类型应用的功能。现有的开源计算方案，会把流处理和批处理作为两种不同的应用类型，因为他们它
们所提供的SLA是完全不相同的：流处理一般需要支持低延迟、Exactly-once保证，而批处理需要支持高吞吐、高效处理，所以在实
现的时候通常是分别给出两套实现方法，或者通过一个独立的开源框架来实现其中每一种处理方案。例如，实现批处理的开源方案有
MapReduce、Tez、Crunch、spark，实现流处理的开源方案有Samza、Storm。

Flink流处理特性：

## 相关阅读

[hadoop官网文档](http://hadoop.apache.org)

[Apache flink 官网](http://flink.apache.org/)

[Flink架构、原理与部署测试](http://www.cnblogs.com/aeexiaoqiang/p/6531754.html)

[Apache Flink：特性、概念、组件栈、架构及原理分析](http://shiyanjun.cn/archives/1508.html)

## 联系方式

以上观点纯属个人看法，如有不同，欢迎指正。

email:<babymm@aliyun.com>

github:[https://github.com/babymm](https://github.com/babymm)