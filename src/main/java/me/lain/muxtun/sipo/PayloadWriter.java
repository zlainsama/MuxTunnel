package me.lain.muxtun.sipo;

import io.netty.buffer.ByteBuf;

@FunctionalInterface
interface PayloadWriter
{

    boolean write(ByteBuf payload) throws Exception;

}
