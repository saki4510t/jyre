/*  =========================================================================
    ZreUdp - UDP management class

    -------------------------------------------------------------------------
    Copyright (c) 1991-2012 iMatix Corporation <www.imatix.com>
    Copyright other contributors as noted in the AUTHORS file.

    This file is part of ZyRE, the ZeroMQ Realtime Experience framework:
    http://zyre.org.

    This is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or (at
    your option) any later version.

    This software is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this program. If not, see
    <http://www.gnu.org/licenses/>.
    =========================================================================
    XXX t_saki@serenegiant.com
    remove unnecessary objects, add JavaDoc, fix wrong broadcast address
*/

package org.zyre;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.Inet4Address;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collections;
import java.util.Enumeration;

public class ZreUdp {
	private DatagramChannel handle;     //  Socket for send/recv
	private final InetSocketAddress broadcast;//  Broadcast address & port
	private SocketAddress sender;       //  Where last recv came from
	private String host;                //  Our own address as string
	private String from;                //  Sender address of last message

//-----------------------------------------------------------------
	/**
	 * Constructor
	 * @param port_nbr
	 */
	public ZreUdp(final int port_nbr) {
		try {
			InetAddress address = null;
			//  Create UDP socket
			handle = DatagramChannel.open();
			handle.configureBlocking(false);
			DatagramSocket sock = handle.socket();

			//  Ask operating system to let us do broadcasts from socket
			sock.setBroadcast(true);
			//  Allow multiple processes to bind to socket; incoming
			//  messages will come to each process
			sock.setReuseAddress(true);

			final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			for (final NetworkInterface netint : Collections.list(interfaces)) {

				if (netint.isLoopback()) {
					continue;
				}

				final Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
				for (final InetAddress addr : Collections.list(inetAddresses)) {
					if (addr instanceof Inet4Address)
						address = addr;
				}
			}
			host = address.getHostAddress();
			sock.bind(new InetSocketAddress(InetAddress.getByAddress(new byte[]{0, 0, 0, 0}), port_nbr));
			final byte[] addr = address.getAddress();
			addr[3] = (byte) 255;
			final InetAddress broadcast_addr = InetAddress.getByAddress(addr);
			broadcast = new InetSocketAddress(broadcast_addr, port_nbr);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

//-----------------------------------------------------------------
	/**
	 * Destructor
	 */
	public void destroy() {
		if (handle != null) {
			try {
				handle.close();
			} catch (IOException e) {
			}
		}
		handle = null;
	}

//-----------------------------------------------------------------
	/**
	 * Returns UDP socket handle
	 * @return
	 */
	public DatagramChannel handle() {
		return handle;
	}

//-----------------------------------------------------------------
	/**
	 * Return our own IP address as printable string
	 * @return
	 */
	public String host() {
		return host;
	}

//-----------------------------------------------------------------
	/**
	 * Return IP address of peer that sent last message
	 * @return
	 */
	public String from() {
		return from;
	}

//-----------------------------------------------------------------
	/**
	 * Send message using UDP broadcast
	 * @param buffer
	 * @throws IOException
	 */
	public void send(final ByteBuffer buffer) throws IOException, IllegalStateException {
		if (handle == null) {
			throw new IllegalStateException("already destroyed");
		}
		handle.send(buffer, broadcast);
	}

//-----------------------------------------------------------------
	/**
	 * Receive message from UDP broadcast
	 * Returns size of received message, or -1
	 * @param buffer
	 * @return
	 * @throws IOException
	 */
	public int recv(final ByteBuffer buffer) throws IOException, IllegalStateException {
		if (handle == null) {
			throw new IllegalStateException("already destroyed");
		}
		final int read = buffer.remaining();
		sender = handle.receive(buffer);
		if (sender == null) {
			return -1;
		}
		from = ((InetSocketAddress) sender).getAddress().getHostAddress();
		return read - buffer.remaining();
	}

}
