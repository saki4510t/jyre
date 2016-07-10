/*  =========================================================================
    ZreLog - record log data
        
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
    Add JavaDoc
*/
package org.zyre;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ZreLog {

	private final ZContext ctx;			//  CZMQ context
	private final Socket publisher;		//  Socket to send to
	private int nodeid;					//  Own correlation ID

//---------------------------------------------------------------------
	/**
	 * Construct new log object
	 * @param endpoint
	 */
	public ZreLog(final String endpoint) {
		ctx = new ZContext();
		publisher = ctx.createSocket(ZMQ.PUB);
		//  Modified Bernstein hashing function
		nodeid = endpoint.hashCode();
	}

//---------------------------------------------------------------------
	/**
	 * Destroy log object
	 */
	public void destroy() {
		ctx.destroy();
	}

//---------------------------------------------------------------------
	/**
	 * Connect log to remote endpoint
	 * @param endpoint
	 */
	public void connect(final String endpoint) {
		publisher.connect(endpoint);
	}

//---------------------------------------------------------------------
	/**
	 * Record one log event
	 * @param peer
	 * @param format
	 * @param args
	 */
	public void debug(final String peer, final String format, final Object... args) {
		final int peerid = peer != null ? peer.hashCode() : 0;
		final String body = format != null ? String.format(format, args) : "";

		sendLog(publisher, ZreLogMsg.ZRE_LOG_MSG_LEVEL_DEBUG,
				ZreLogMsg.ZRE_LOG_MSG_EVENT_DEBUG, nodeid, peerid, System.currentTimeMillis(), body);
	}

	/**
	 * Record one log event
 	 * @param event
	 * @param peer
	 * @param format
	 * @param args
	 */
	public void info(final int event, final String peer, final String format, final Object... args) {
		final int peerid = peer != null ? peer.hashCode() : 0;
		final String body = format != null ? String.format(format, args) : "";

		sendLog(publisher, ZreLogMsg.ZRE_LOG_MSG_LEVEL_INFO,
				event, nodeid, peerid, System.currentTimeMillis(), body);
	}

	/**
	 * Record one log event
	 * @param event
	 * @param peer
	 * @param format
	 * @param args
	 */
	public void warn(final int event, final String peer, final String format, final Object... args) {
		final int peerid = peer != null ? peer.hashCode() : 0;
		final String body = format != null ? String.format(format, args) : "";

		sendLog(publisher, ZreLogMsg.ZRE_LOG_MSG_LEVEL_WARNING,
				event, nodeid, peerid, System.currentTimeMillis(), body);
	}

	/**
	 * Record one log event
	 * @param event
	 * @param peer
	 * @param format
	 * @param args
	 */
	public void error(final int event, final String peer, final String format, final Object... args) {
		final int peerid = peer != null ? peer.hashCode() : 0;
		final String body = format != null ? String.format(format, args) : "";

		sendLog(publisher, ZreLogMsg.ZRE_LOG_MSG_LEVEL_ERROR,
				event, nodeid, peerid, System.currentTimeMillis(), body);
	}

//--------------------------------------------------------------------------
	/**
	 * Send the LOG to the socket in one step
	 * @param output
	 * @param level
	 * @param event
	 * @param node
	 * @param peer
	 * @param time
	 * @param data
	 * @return
	 */
	public static boolean sendLog(final Socket output, final int level, final int event,
		final int node, final int peer, final long time, final String data) {

		final ZreLogMsg msg = new ZreLogMsg(ZreLogMsg.LOG);
		msg.setLevel(level);
		msg.setEvent(event);
		msg.setNode(node);
		msg.setPeer(peer);
		msg.setTime(time);
		msg.setData(data);

		return msg.send(output);
	}

}
