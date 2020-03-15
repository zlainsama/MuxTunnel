package me.lain.muxtun.sipo;

import io.netty.util.AttributeKey;

class Vars
{

    static final AttributeKey<Throwable> ERROR_KEY = AttributeKey.newInstance("me.lain.muxtun.sipo.Vars#Error");
    static final AttributeKey<LinkSession> SESSION_KEY = AttributeKey.newInstance("me.lain.muxtun.sipo.Vars#Session");
    static final AttributeKey<PayloadWriter> WRITER_KEY = AttributeKey.newInstance("me.lain.muxtun.sipo.Vars#Writer");

}
