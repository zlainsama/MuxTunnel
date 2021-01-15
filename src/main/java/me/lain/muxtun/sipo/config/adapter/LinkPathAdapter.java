package me.lain.muxtun.sipo.config.adapter;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import me.lain.muxtun.sipo.config.LinkPath;
import me.lain.muxtun.sipo.config.Socks5ProxyHandlerSupplier;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class LinkPathAdapter extends TypeAdapter<LinkPath> {

    @Override
    public void write(JsonWriter out, LinkPath value) throws IOException {
        if (value == null)
            out.nullValue();
        else if (value.getProxySupplier().getProxyAddress() instanceof InetSocketAddress)
            writeInetSocketAddress(out, value, (InetSocketAddress) value.getProxySupplier().getProxyAddress());
        else
            throw new UnsupportedOperationException();
    }

    private void writeInetSocketAddress(JsonWriter out, LinkPath value, InetSocketAddress proxyAddress) throws IOException {
        out.value(value.getPriority() + ":" + value.getWriteLimit() + "@" + proxyAddress.getHostString() + ":" + proxyAddress.getPort());
    }

    @Override
    public LinkPath read(JsonReader in) throws IOException {
        String value = in.nextString();
        int indexToFirstAt = value.indexOf("@");
        String propertiesString = value.substring(0, indexToFirstAt);
        String addressString = value.substring(indexToFirstAt + 1);
        int indexToPropertiesColon = propertiesString.indexOf(":");
        short priority = Short.parseShort(propertiesString.substring(0, indexToPropertiesColon));
        long writeLimit = Long.parseLong(propertiesString.substring(indexToPropertiesColon + 1));
        int indexToAddressColon = addressString.lastIndexOf(":");
        String host = addressString.substring(0, indexToAddressColon);
        int port = Integer.parseInt(addressString.substring(indexToAddressColon + 1));
        SocketAddress proxyAddress = new InetSocketAddress(host, port);
        Socks5ProxyHandlerSupplier proxySupplier = new Socks5ProxyHandlerSupplier().setProxyAddress(proxyAddress);
        return new LinkPath().setPriority(priority).setWriteLimit(writeLimit).setProxySupplier(proxySupplier);
    }

}
