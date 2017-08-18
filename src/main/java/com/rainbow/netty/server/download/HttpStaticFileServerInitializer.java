package com.rainbow.netty.server.download;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;

public class HttpStaticFileServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslContext;

    public HttpStaticFileServerInitializer(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline channelPipeline = ch.pipeline();
        if (sslContext != null) {
            channelPipeline.addLast(sslContext.newHandler(ch.alloc()));
        }
        channelPipeline.addLast(new HttpServerCodec());
        channelPipeline.addLast(new HttpObjectAggregator(65536));
        channelPipeline.addLast(new ChunkedWriteHandler());
        channelPipeline.addLast(new HttpStaticFileServerHandler());
    }
}
