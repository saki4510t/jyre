/*  =========================================================================
    ZrePeer - one of our peers in a ZyRE network

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
    Use ZreIdentity and fit to 36/ZRE ZeroM RFC, add JavaDoc
*/
package org.zyre;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ZrePeer {
	private static final int USHORT_MAX = 0xffff;
	private static final int UBYTE_MAX = 0xff;

	private final WeakReference<ZContext> weakCtx;          //  CZMQ context // XXX saki
	private Socket mailbox;					//  Socket through to peer
	private ZreIdentity identity;			//  Identity
	private String endpoint;				//  Endpoint connected to
	private long evasive_at;				//  Peer is being evasive
	private long expired_at;				//  Peer has expired by now
	private boolean connected;				//  Peer will send messages
	private boolean ready;					//  Peer has said Hello to us
	private int status;						//  Our status counter
	private int sent_sequence;				//  Outgoing message sequence
	private int want_sequence;				//  Incoming message sequence
	private Map<String, String> headers;	//  Peer headers
	private String name;					//  name of this peer

	private ZrePeer(final ZContext ctx, final ZreIdentity identity, final String name) {
		this.weakCtx = new WeakReference<ZContext>(ctx);
		this.identity = identity;
		this.name = name;

		ready = false;
		connected = false;
		sent_sequence = 0;
		want_sequence = 0;
	}

//---------------------------------------------------------------------
	/**
	 * Construct new peer object
	 * @param identity
	 * @param container
	 * @param ctx
	 * @return
	 */
	public static ZrePeer newPeer(final ZreIdentity identity, final String name, final Map<ZreIdentity, ZrePeer> container, final ZContext ctx) {
		final ZrePeer peer = new ZrePeer(ctx, identity, name);
		container.put(identity, peer);

		return peer;
	}

//---------------------------------------------------------------------
	/**
	 * Destroy peer object
	 */
	public void destroy() {
		disconnect();
	}

//---------------------------------------------------------------------
	/**
	 * Connect peer mailbox
	 * Configures mailbox and connects to peer's router endpoint
	 * @param reply_to_identity
	 * @param peer_endpoint
	 */
	public void connect(final ZreIdentity reply_to_identity, final String peer_endpoint) {
		disconnect();    // XXX saki
		final ZContext ctx = weakCtx.get();
		if (ctx != null) {
			//  Create new outgoing socket (drop any messages in transit)
			mailbox = ctx.createSocket(ZMQ.DEALER);

			//  Null if shutting down
			if (mailbox != null) {
				//  Set our caller 'From' identity so that receiving node knows
				//  who each message came from.
				mailbox.setIdentity(reply_to_identity.getBytes());

				//  Set a high-water mark that allows for reasonable activity
				mailbox.setSndHWM(ZreInterface.PEER_EXPIRED * 100);

				//  Send messages immediately or return EAGAIN
				mailbox.setSendTimeOut(0);

				//  Connect through to peer node
				mailbox.connect(peer_endpoint);
				endpoint = peer_endpoint;
				connected = true;
				ready = false;
			}
		} else {
			throw new IllegalStateException("ZContext already destroyed");
		}
	}

//---------------------------------------------------------------------
	/**
	 * Disconnect peer mailbox
	 * No more messages will be sent to peer until connected again
	 */
	public void disconnect() {
		final ZContext ctx = weakCtx.get();
		if (ctx != null) {
			if (mailbox != null) {
				ctx.destroySocket(mailbox);
			}
		}
		mailbox = null;
		endpoint = null;
		connected = false;
	}

	/**
	 * Send message to peer
	 * @param msg will be destroyed in this method
	 * @return true if success to send, otherwise false
	 */
	public boolean send(final ZreMsg msg) {
		boolean result = false;
		if (connected) {
			if (++sent_sequence > USHORT_MAX)
				sent_sequence = 0;
			msg.setSequence(sent_sequence);
			result = msg.send(mailbox);
			if (!result) {
				disconnect();
			}
		} else {
			msg.destroy();
		}

		return result;
	}

//---------------------------------------------------------------------
	/**
	 * Return peer connection endpoint
	 * @return
	 */
	public String endpoint() {
		if (connected)
			return endpoint;
		else
			return "";

	}

//---------------------------------------------------------------------
	/**
	 * Register activity at peer
	 */
	public void refresh() {
		final long t = System.currentTimeMillis();
		evasive_at = t + ZreInterface.PEER_EVASIVE;
		expired_at = t + ZreInterface.PEER_EXPIRED;
	}

//---------------------------------------------------------------------
	/**
	 * Return peer future expired time
	 * @return
	 */
	public long expiredAt() {
		return expired_at;
	}

//---------------------------------------------------------------------
	/**
	 * Return peer future evasive time
	 * @return
	 */
	public long evasiveAt() {
		return evasive_at;
	}

	/**
	 * Set whether this peer is ready or not
	 * @param ready
	 */
	public void setReady(final boolean ready) {
		this.ready = ready;
	}

	/**
	 * Get whether this peer is ready or not
	 * @return
	 */
	public boolean ready() {
		return ready;
	}

	/**
	 * Set status
	 * @param status
	 */
	public void setStatus(final int status) {
		this.status = status;
	}

	/**
	 * Get status
	 * @return
	 */
	public int status() {
		return status;
	}

	/**
	 * Increment status, if the value exceeds UBYTE_MAX, set to zero
	 */
	public void incStatus() {
		if (++status > UBYTE_MAX)
			status = 0;
	}

	/**
	 * Get identity as string
	 * @return
	 */
	public String identityString() {
		return identity.toString();
	}

	/**
	 * Get identity
	 * @return
	 */
	public ZreIdentity identity() {
		return identity;
	}

	/**
	 * Get header as string, if value is not in headers, return default value
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public String header(final String key, final String defaultValue) {
		if (headers.containsKey(key))
			return headers.get(key);

		return defaultValue;
	}

	/**
	 * Set headers
	 * @param headers
	 */
	public void setHeaders(final Map<String, String> headers) {
		this.headers = new HashMap<String, String>(headers);
	}

	/**
	 * Check whether message is valid
	 * @param msg
	 * @return
	 */
	public boolean checkMessage(final ZreMsg msg) {
		int recd_sequence = msg.sequence();
		if (++want_sequence > USHORT_MAX)
			want_sequence = 0;

		boolean valid = want_sequence == recd_sequence;
		if (!valid) {
			if (--want_sequence < 0)    //  Rollback
				want_sequence = USHORT_MAX;
		}
		return valid;
	}

	/**
	 * Set name of this peer
	 * @param name
	 */
	public void setName(final String name) {
		this.name = name;
	}

	/**
	 * Get name of this peer
	 * @return if peer name is null or empty string, return identity as string.
	 */
	public String name() {
		return TextUtils.isEmpty(name) ? identity.toString() : name;
	}
}
