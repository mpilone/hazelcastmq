# Yeti

A [STOMP](http://stomp.github.com) server and client framework built 
on [Netty](http://netty.io/) to make it simple to build STOMP implementations for 
existing brokers. Yeti borrows the best ideas from 
[Stampy](https://github.com/mrstampy/Stampy) and 
[Stilts](http://stilts.projectodd.org/stilts-stomp/) to provide fast, Netty based 
STOMP frame codecs and frame handlers. Yeti is the core STOMP server 
implementation used by HazelcastMQ STOMP however it has no direct dependency 
on Hazelcast/HazelcastMQ and may be split out into a separate project in the 
future.

## Features
* Protocol negotiation (Stomplet implementations can define supported protocols)
* Simple Stomplet framework for easily handling frame commands
* Heartbeats (via the heart-beat header)
* Hooks for authentication (via login and passcode headers)
* Pure Netty configuration for endless network I/O configuration options
* STOMP client implementation support async and sync message receiption
* Frame builder for fluent frame construction
* Header encoding/decoding of special characters

## Not Implemented Yet
* Most everything should be there

## Yeti, Really?
The name Yeti came about because:

1. All the cool variations of STOMP are already taken by other [implementations](http://stomp.github.io/implementations.html).
2. It's fun to say "Netty Yeti STOMP".
3. My daughter was watching Backyardigans and [this song](http://www.nickjr.com/kids-videos/backyardigans-the-yeti-stomp.html) gets stuck in your head (don't say I didn't warn you).

## Examples

TODO