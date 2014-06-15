# HazelcastMQ

HazelcastMQ provides a simple messaging layer on top of the basic Queue and Topic data 
structures provided by [Hazelcast](http://www.hazelcast.com/), an in-memory 
data grid. HazelcastMQ emphasizes simple configuration and reliable clustering 
while providing an understandable and flexible messaging API. HazelcastMQ builds
on the core features of Hazelcast such as scalability and resilience while
maintaining a small footprint and few dependencies. HazelcastMQ can be easily
embedded in a single JVM or clustered across a huge number of nodes.

HazelcastMQ is divided into multiple components:

* [hazelcastmq-core](hazelcastmq-core/README.md): The core MQ library that 
provides a JMS 2.0-like API for sending and receiving messages.
* [hazelcastmq-camel](hazelcastmq-camel/README.md): An 
[Apache Camel](http://camel.apache.org/) component implementation for 
HazelcastMQ supporting Camel's integration framework and Enterprise Integration 
Patterns (EIP).
* [hazelcastmq-jms](hazelcastmq-jms/README.md): A JMS 1.1 implementation which 
layers on top of HazelcastMQ Core.
* [hazelcastmq-stomp](hazelcastmq-stomp/README.md): A [STOMP](http://stomp.github.com) 
server which maps all SEND and SUBSCRIBE commands to HazelcastMQ Core
producers and consumers.
* [yeti](yeti/README.md): A [STOMP](http://stomp.github.com) server and client framework built 
on [Netty](http://netty.io/) to make it simple to build STOMP implementations for 
existing brokers.

## Rationale

Refer to my [initial blog post](http://mikepilone.blogspot.com/2013/01/hazelcast-jms-provider.html) for now.

## Examples

Refer to each module for code examples or browse through the 
[hazelcastmq-examples](hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example) 
module. 

## Getting Builds

The source, javadoc, and binaries are available in the 
[mpilone/mvn-repo](https://github.com/mpilone/mvn-repo) GitHub repository. You
can configure Maven or Ivy to directly grab the dependencies by adding the repository:

    <repositories>
         <repository>
             <id>mpilone-snapshots</id>
             <url>https://github.com/mpilone/mvn-repo/raw/master/snapshots</url>
         </repository>
         <repository>
             <id>mpilone-releases</id>
             <url>https://github.com/mpilone/mvn-repo/raw/master/releases</url>
         </repository>
     </repositories>

And then adding the dependency:

    <dependency>
        <groupId>org.mpilone</groupId>
        <artifactId>hazelcastmq-core</artifactId>
        <version>1.0-SNAPSHOT</version>
    </dependency>
    
If there is enough interest I could look at moving it to a standard public 
Maven Repository.

