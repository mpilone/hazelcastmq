# Yeti

A [STOMP](http://stomp.github.com) server and client framework built 
on [Netty](http://netty.io/) to make it simple to build STOMP implementations 
for existing brokers. Yeti borrows the best ideas from 
[Stampy](https://github.com/mrstampy/Stampy) and 
[Stilts](http://stilts.projectodd.org/stilts-stomp/) to provide fast, reusable 
STOMP frame codecs and channel handlers while abstracting away the underlying 
network IO with a Stomplet API similar to the familiar Servlet API. To build
a custom server, simply implement a Stomplet or extend one of the existing 
Stomplets to provide custom handling for STOMP frames.

Yeti is the core STOMP server implementation used by HazelcastMQ STOMP however 
it has no direct dependency on Hazelcast/HazelcastMQ and may be split out into 
a separate project in the future.

## Features
* Protocol version negotiation (Stomplet implementations can define supported protocols)
* Simple Stomplet framework for easily handling frame commands
* Heartbeats (via the heart-beat header)
* Hooks for authentication (via login and passcode headers)
* Pure Netty configuration for endless network I/O configuration options
* STOMP client implementation support async and sync message receiption
* Frame builder for fluent frame construction
* Header encoding/decoding of special characters
* Maximum frame size enforcement

## Not Implemented Yet
* Most everything should be there

## Yeti, Really?
The name Yeti came about because:

1. All the cool variations of STOMP are already taken by other [implementations](http://stomp.github.io/implementations.html).
2. It's fun to say "Netty Yeti STOMP".
3. My daughter was watching Backyardigans and [this song](https://www.youtube.com/watch?v=s_2L7O1UB5Q) gets stuck in your head (don't say I didn't warn you).

## Examples

Yeti comes with a STOMP server implementation that configures a Netty NIO 
server with reasonable defaults and initializes a channel pipeline to host a 
configured stomplet. An in-memory message broker stomplet is
also included and can be used to get a demonstration server up and running 
quickly.

    StompServer server = new StompServer(false, port,
        new StompServer.ClassStompletFactory(InMemoryBrokerStomplet.class));
    server.start();

The server can also be used with a custom stomplet implementation that handles 
all incoming STOMP frames. Normally such an implementaiton would map the STOMP 
frames to a full featured messaging system or broker.

The included server is just a convenience wrapper on Yeti's frame codecs and 
channel handlers, including the StompletFrameHandler which adapts Netty's 
read/write handling to the Stomplet implementation. A custom Netty 
configuration can be implemented and full STOMP functionality can be realized 
by building a channel pipeline with Yeti's codecs and handlers. This approach 
requires slightly more configuraiton but ultimate flexibility as all the Netty 
options are available.

    ch.pipeline().addLast(StompFrameDecoder.class.getName(),
        new StompFrameDecoder());
    ch.pipeline().addLast(StompFrameEncoder.class.getName(),
        new StompFrameEncoder());
    ch.pipeline().addLast(FrameDebugHandler.class.getName(),
        new FrameDebugHandler(true, true));
    ch.pipeline().addLast(StompletFrameHandler.class.getName(),
        new StompletFrameHandler(new MyStompletImpl()));

Yeti also comes with a STOMP client implementation that configures a Netty NIO 
client with reasonable defaults. The client can connect to any STOMP 1.2 server 
and subscribe, send messages, and recieve messages. This example shows how to 
use the in-memory broker stomplet and the STOMP client to subscribe, send, 
and receive messages.

View the [example](../yeti/src/test/java/org/mpilone/yeti/ServerClientApp.java).
