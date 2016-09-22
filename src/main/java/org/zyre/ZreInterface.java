/*  =========================================================================
    ZreInterface - interface to a ZyRE network

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

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZThread;

public class ZreInterface {
//	private static final boolean DEBUG = false;	// FIXME set false on production
	private static final String TAG = ZreInterface.class.getSimpleName();

	public static final int UBYTE_MAX = 0xff;
	// Defined port numbers, pending IANA submission
	public static final int ZRE_DISC_PORT_NUMBER = 5670;
	public static final int LOG_PORT_NUMBER = 9992;

	//  Constants, to be configured/reviewed
	public static final int PING_INTERVAL = 1000;	//  Once per second
	public static final int PEER_EVASIVE = 5000;	//  Five seconds' silence is evasive
	public static final int PEER_EXPIRED = 10000;	//  Ten seconds' silence is expired

	private ZContext ctx;				//  Our context wrapper
	private Socket pipe;				//  Pipe through to agent
	private String mNodeName = "";		// XXX saki take care not to set null to this field, set "" instead. otherwise Socket#sendMore/#send crash
	private String mUuid = "";			// XXX saki
	private final Object mPipeSync = new Object();    // XXX saki
	private boolean initialized;		// XXX saki
	private final int mListenPort;		// XXX saki

// ---------------------------------------------------------------------
	/**
	 * Constructor
	 */
	public ZreInterface() {
		this(null, ZRE_DISC_PORT_NUMBER);
	}

	/**
	 * Constructor
	 * @param name
	 */
	public ZreInterface(final String name) {
		this(name, ZRE_DISC_PORT_NUMBER);
	}

	/**
	 * Constructor
	 * @param name
	 * @param beacon_port
	 */
	public ZreInterface(final String name, final int beacon_port) {
		mListenPort = beacon_port > 0 ? beacon_port : ZRE_DISC_PORT_NUMBER;
		mNodeName = !TextUtils.isEmpty(name) ? name : "";
		ctx = new ZContext();
		ctx.setLinger(100);    // XXX saki
		synchronized (mPipeSync) {    // XXX saki
			if (!initialized) {
				pipe = ZThread.fork(ctx, new ZreInterfaceAgent(this));
				try {
					mPipeSync.wait(3000);    // wait for initializing Agent
				} catch (final InterruptedException e) {
					initialized = false;
				}
			}
		}
		if (!initialized) {
			throw new RuntimeException("failed to initialize socket");
		}
		pipe.setLinger(100);
	}

//---------------------------------------------------------------------
	/**
	 * Destructor
	 */
	public void destroy() {
		pipe.close();    // XXX saki
		ctx.destroy();
	}

	/**
	 * get uuid as hex string
	 * @return
	 */
	public String uuid() {
		return mUuid != null ? mUuid : "";
	}

	/**
	 * Set node name
	 * @param name
	 */
	public void setName(final String name) {
		mNodeName = !TextUtils.isEmpty(name) ? name : "";
	}

	/**
	 * Get node name
	 * @return
	 */
	public String name() {    // XXX saki
		return mNodeName;
	}

//---------------------------------------------------------------------
	/**
	 * Receive next message from interface
	 * @return ZMsg object, or NULL if interrupted
	 */
	public ZMsg recv() {
		return ZMsg.recvMsg(pipe);
	}

// ---------------------------------------------------------------------
	/**
	 * Join a group
	 * @param group_name
	 */
	public void join(final String group_name) {
		pipe.sendMore("JOIN");
		pipe.send(group_name);
	}

//---------------------------------------------------------------------
	/**
	 * Leave a group
 	 * @param group_name
	 */
	public void leave(final String group_name) {
		pipe.sendMore("LEAVE");
		pipe.send(group_name);
	}

//---------------------------------------------------------------------
	/**
	 * Send message to single peer; peer ID is first frame in message
	 * Destroys message after sending
	 * @param msg
	 */
	public void whisper(final ZMsg msg) {
		pipe.sendMore("WHISPER");
		msg.send(pipe);
	}

//---------------------------------------------------------------------
	/**
	 * Send message to a group of peers
	 * @param msg
	 */
	public void shout(final ZMsg msg) {
		pipe.sendMore("SHOUT");
		msg.send(pipe);
	}

//---------------------------------------------------------------------
	/**
	 * Return interface handle, for polling
	 * @return
	 */
	public Socket handle() {
		return pipe;
	}

// ---------------------------------------------------------------------
	/**
	 * Set node header value
	 * @param key_name
	 * @param value_format
	 * @param args
	 */
	public void setHeader(final String key_name, final String value_format, final Object... args) {
		pipe.sendMore("SET");
		pipe.sendMore(key_name);
		pipe.send(String.format(value_format, args));
	}

// ---------------------------------------------------------------------
	/**
	 * Publish file into virtual space
	 * @param pathname
	 * @param virtual
	 */
	public void publish(final String pathname, final String virtual) {
		pipe.sendMore("PUBLISH");
		pipe.sendMore(pathname);
		pipe.send(virtual);
	}

//=====================================================================
	/**
	 * Asynchronous part, works in the background
	 * Beacon frame has this format:
	 * Z R E       3 bytes
	 * version     1 byte, %x01
	 * UUID        16 bytes
	 * port        2 bytes in network order = big endian = same as usual byte order of Java
	 */
	protected static class Beacon {
		public static final int BEACON_SIZE = 22;

		public static final String BEACON_PROTOCOL = "ZRE";
		public static final byte BEACON_VERSION = 0x01;

		private final UUID uuid;
		private final int listenPort;

		public Beacon(final ByteBuffer buffer) {
			uuid = new UUID(buffer.getLong(), buffer.getLong());
			final int port = buffer.getShort();
			listenPort = port < 0 ? (0xffff) & port : port;
		}

		public Beacon(final UUID uuid, final int port) {
			this.uuid = uuid;
			listenPort = port;
		}

		public ByteBuffer getBuffer() {
			final ByteBuffer buffer = ByteBuffer.allocate(BEACON_SIZE);
			buffer.put(BEACON_PROTOCOL.getBytes());
			buffer.put(BEACON_VERSION);
			buffer.putLong(uuid.getMostSignificantBits());
			buffer.putLong(uuid.getLeastSignificantBits());
			buffer.putShort((short) listenPort);
			buffer.flip();
			return buffer;
		}

	}

	public static String uuidStr(final UUID uuid) {
		return uuid.toString().replace("-", "").toUpperCase();
	}

	private static final String OUTBOX = ".outbox";
	private static final String INBOX = ".inbox";

	protected static class Agent {
		private final WeakReference<ZreInterface> mWeakParent;	// XXX saki
		private final WeakReference<ZContext> weakCtx;    //  CZMQ context, XXX saki change to WeakReference
		private final Socket appPipe;           //  Pipe back to application
		private final ZreUdp udp;               //  UDP object
		private final ZreLog log;               //  Log object
		private final UUID uuid;                //  Our UUID as binary blob
		private final String uuidString;        //  Our UUID as hex string
		private final ZreIdentity identity;     //  Identity
		private final Socket inbox;             //  Our inbox socket (ROUTER)
		private final String endpoint;          //  "tcp://ipaddress:port"
		private final Map<ZreIdentity, ZrePeer> peers;			// Hash of known peers, fast lookup
		private final Map<String, ZreGroup> peer_groups;		// Groups that our peers are in
		private final Map<String, ZreGroup> own_groups;			// Groups that we are in
		private final Map<String, String> headers;				// Our header values
		private final Beacon beacon;            // XXX saki
		private int status;                     //  Our own change counter

		private Agent(final ZContext ctx, final Socket app_pipe, final Socket inbox,
					  final ZreUdp udp, final int port, final ZreInterface parent) {

			mWeakParent = new WeakReference<ZreInterface>(parent);
			weakCtx = new WeakReference<ZContext>(ctx);
			this.appPipe = app_pipe;
			this.inbox = inbox;
			this.udp = udp;

			uuid = UUID.randomUUID();
			uuidString = uuidStr(uuid);
			identity = new ZreIdentity(uuid);
			endpoint = String.format("tcp://%s:%d", udp.host(), port);
			peers = new HashMap<ZreIdentity, ZrePeer>();
			peer_groups = new HashMap<String, ZreGroup>();
			own_groups = new HashMap<String, ZreGroup>();
			headers = new HashMap<String, String>();
			beacon = new Beacon(uuid, port);
			log = new ZreLog(endpoint);
			log.debug(null, "Agent: assign router port=" + port);
		}

		protected static Agent newAgent(final ZContext ctx, final Socket app_pipe, final ZreInterface parent) {
			final Socket inbox = ctx.createSocket(ZMQ.ROUTER);
			if (inbox == null)      //  Interrupted
				return null;

			// create ZreUdp to listen
			final ZreUdp udp = new ZreUdp(parent.mListenPort);    // XXX saki, ZRE_DISC_PORT_NUMBER
			// bind random port to router(inbox)
			final int port = inbox.bindToRandomPort("tcp://*", 0xc000, 0xffff);
			if (port < 0) {          //  Interrupted
				System.err.println("Failed to bind a random port");
				udp.destroy();
				return null;
			}
			return new Agent(ctx, app_pipe, inbox, udp, port, parent);
		}

		protected void destroy() {
			for (final ZrePeer peer : peers.values())
				peer.destroy();
			peers.clear();
			for (final ZreGroup group : peer_groups.values()) {
				group.destroy();
			}
			peer_groups.clear();	// saki, fixed own_groups => peer_groups
			for (final ZreGroup group : own_groups.values()) {
				group.destroy();
			}
			own_groups.clear();

			udp.destroy();
			log.destroy();

		}

		private int incStatus() {
			if (++status > UBYTE_MAX)
				status = 0;
			return status;
		}

		/**
		 * Delete peer for a given endpoint
		 */
		private void purgePeer() {
			for (final Map.Entry<ZreIdentity, ZrePeer> entry : peers.entrySet()) {
				final ZrePeer peer = entry.getValue();
				if (peer.endpoint().equals(endpoint)) {
					peer.disconnect();
				}
			}
		}

		private String name() {
			final ZreInterface parent = mWeakParent.get();
			return parent != null ? parent.name() : "";
		}

		/**
		 * Find or create peer via its UUID string
		 * @param peer_identity
		 * @param peer_endpoint
		 * @return
		 */
		private ZrePeer requirePeer(final ZreIdentity peer_identity, final String peer_name, final String peer_endpoint) {
			ZrePeer peer = peers.get(peer_identity);
			if (peer == null) {
				//  Purge any previous peer on same endpoint
				purgePeer();

				log.debug(peer_identity.toString(), "requirePeer:peer not found, create new:peer_endpoint=%s", peer_endpoint);
				final ZContext ctx = weakCtx.get();    // XXX saki
				if (ctx != null) {    // XXX saki
					peer = ZrePeer.newPeer(peer_identity, peer_name, peers, ctx);
					peer.connect(identity, peer_endpoint);

					//  Handshake discovery by sending HELLO as first message
					final ZreMsg msg = new ZreMsg(ZreMsg.HELLO);
					msg.setEndpoint(endpoint);
					msg.setName(name());
					msg.setGroups(own_groups.keySet());
					msg.setStatus(status);
					msg.setHeaders(headers);   // XXX saki remove unnecessary duplication
					peer.send(msg);

					log.info(ZreLogMsg.ZRE_LOG_MSG_EVENT_ENTER, peer.identityString(), peer.endpoint(), endpoint);
// saki move to #recvFromPeer when receive ZreMsg.HELLO
//					//  Now tell the caller about the peer
//					appPipe.sendMore("ENTER");
//					appPipe.sendMore(peer.identityString());
//					appPipe.sendMore(peer.name());
//					appPipe.send(peer_endpoint);
				}
			}
			return peer;
		}

		/**
		 * Find or create group via its name
		 * @param group_name
		 * @return
		 */
		private ZreGroup requirePeerGroup(final String group_name) {
			ZreGroup group = peer_groups.get(group_name);
			if (group == null) {
				group = ZreGroup.newGroup(group_name, peer_groups);
			}
			return group;

		}

		/**
		 * add peer to group
		 * @param peer
		 * @param group_name
		 * @return
		 */
		private ZreGroup joinPeerGroup(final ZrePeer peer, final String group_name) {
			// find or create group with specific group name
			final ZreGroup group = requirePeerGroup(group_name);
			group.join(peer);

			//  Now tell the caller about the peer joined a group
			appPipe.sendMore("JOIN");
			appPipe.sendMore(peer.identityString());
			appPipe.sendMore(peer.name());
			appPipe.send(group_name);

			return group;
		}

		/**
		 * remove peer from group
		 * @param peer
		 * @param group_name
		 * @return
		 */
		private ZreGroup leavePeerGroup(final ZrePeer peer, final String group_name) {
			final ZreGroup group = requirePeerGroup(group_name);
			group.leave(peer);

			//  Now tell the caller about the peer joined a group
			appPipe.sendMore("LEAVE");
			appPipe.sendMore(peer.identityString());
			appPipe.sendMore(peer.name());
			appPipe.send(group_name);

			return group;
		}

		/**
		 * Here we handle the different control messages from the front-end
		 */
		protected boolean recvFromApi() {
			//  Get the whole message off the pipe in one go
			final ZMsg request = ZMsg.recvMsg(appPipe);
			final String command = request.popString();
			if (command == null)
				return false;                  //  Interrupted

			if (command.equals("WHISPER")) {
				//  Get peer to send message to
				final ZreIdentity identity = ZreIdentity.fromUUID(request.popString());
				final ZrePeer peer = peers.get(identity);

				//  Send frame on out to peer's mailbox, drop message
				//  if peer doesn't exist (may have been destroyed)
				if (peer != null) {
					final ZreMsg msg = new ZreMsg(ZreMsg.WHISPER);
					msg.setContent(request.pop());
					peer.send(msg);
				}
			} else if (command.equals("SHOUT")) {
				//  Get group to send message to
				final String group_name = request.popString();
				final ZreGroup group = peer_groups.get(group_name);
				if (group != null) {
					final ZreMsg msg = new ZreMsg(ZreMsg.SHOUT);
					msg.setGroup(group_name);
					msg.setContent(request.pop());
					group.send(msg);
				}
			} else if (command.equals("JOIN")) {
				final String group_name = request.popString();
				ZreGroup group = own_groups.get(group_name);
				if (group == null) {
					//  Only send if we're not already in group
					group = ZreGroup.newGroup(group_name, own_groups);
					final ZreMsg msg = new ZreMsg(ZreMsg.JOIN);
					msg.setGroup(group_name);
					//  Update status before sending command
					msg.setStatus(incStatus());
					try {    // XXX saki
						sendPeers(peers, msg);
					} catch (final ZMQException e) {
						e.printStackTrace();
					}
					msg.destroy();
					log.info(ZreLogMsg.ZRE_LOG_MSG_EVENT_JOIN, null, group_name);
				}
			} else if (command.equals("LEAVE")) {
				final String group_name = request.popString();
				final ZreGroup group = own_groups.get(group_name);
				if (group != null) {
					//  Only send if we are actually in group
					final ZreMsg msg = new ZreMsg(ZreMsg.LEAVE);
					msg.setGroup(group_name);
					//  Update status before sending command
					msg.setStatus(incStatus());
					try {    // XXX saki
						sendPeers(peers, msg);
					} catch (final ZMQException e) {
						e.printStackTrace();
					}
					own_groups.remove(group_name);
					log.info(ZreLogMsg.ZRE_LOG_MSG_EVENT_LEAVE, null, group_name);
				}
			} else if (command.equals("SET")) {
				final String node_name = request.popString();
				final String name = request.popString();
				final String value = request.popString();
				headers.put(name, value);
			} else {
				System.err.println("Unknown command: " + command);
			}


			request.destroy();
			return true;
		}

		/**
		 * Here we handle messages coming from other peers
		 * @return
		 */
		protected boolean recvFromPeer() {
			//  Router socket tells us the identity of this peer
			final ZreMsg msg = ZreMsg.recv(inbox);
			if (msg == null)
				return false;               //  Interrupted

			final ZreIdentity peer_identity = msg.identity();

			//  On HELLO we may create the peer if it's unknown
			//  On other commands the peer must already exist
			ZrePeer peer = peers.get(peer_identity);
			if (msg.id() == ZreMsg.HELLO) {
				peer = requirePeer(peer_identity, msg.name(), msg.endpoint());
				assert (peer != null);
				peer.setReady(true);
			}
			//  Ignore command if peer isn't ready
			if (peer == null || !peer.ready()) {
				msg.destroy();
				return true;
			}

			if (!peer.checkMessage(msg)) {
				System.err.printf("W: [%s] lost messages from %s\n", this.identity, peer_identity);
				assert (false);
			}

			//  Now process each command
			if (msg.id() == ZreMsg.HELLO) {
				peer.setName(msg.name());
				//  Join peer to listed groups
				for (final String group_name : msg.groups()) {
					joinPeerGroup(peer, group_name);    // XXX saki
				}
				//  Hello command holds latest status of peer
				peer.setStatus(msg.status());

				//  Store peer headers for future reference
				peer.setHeaders(msg.headers());
				//  Pass up to caller API as ENTER event
				appPipe.sendMore("ENTER");
				appPipe.sendMore(peer.identityString());
				appPipe.sendMore(peer.name());
				appPipe.send(peer_endpoint);
			} else if (msg.id() == ZreMsg.WHISPER) {
				//  Pass up to caller API as WHISPER event
				final ZFrame cookie = msg.content();
				appPipe.sendMore("WHISPER");
				appPipe.sendMore(peer.identityString());
				appPipe.sendMore(peer.name());
				cookie.send(appPipe, 0); // let msg free the frame
			} else if (msg.id() == ZreMsg.SHOUT) {
				//  Pass up to caller as SHOUT event
				final ZFrame cookie = msg.content();
				appPipe.sendMore("SHOUT");
				appPipe.sendMore(peer.identityString());
				appPipe.sendMore(peer.name());
				appPipe.sendMore(msg.group());
				cookie.send(appPipe, 0); // let msg free the frame
			} else if (msg.id() == ZreMsg.PING) {
				final ZreMsg pingOK = new ZreMsg(ZreMsg.PING_OK);
				peer.send(pingOK);
			} else if (msg.id() == ZreMsg.JOIN) {
				joinPeerGroup(peer, msg.group());
				assert (msg.status() == peer.status());
			} else if (msg.id() == ZreMsg.LEAVE) {
				leavePeerGroup(peer, msg.group());
				assert (msg.status() == peer.status());
			}
			msg.destroy();

			//  Activity from peer resets peer timers
			peer.refresh();
			return true;
		}

		/**
		 * Handle beacon
		 * @return
		 */
		protected boolean recvUdpBeacon() {
			final ByteBuffer buffer = ByteBuffer.allocate(Beacon.BEACON_SIZE);

			//  Get beacon frame from network
			int size = 0;
			try {
				size = udp.recv(buffer);
			} catch (IOException e) {
				e.printStackTrace();
			}
			buffer.rewind();

			//  Basic validation on the frame
			if (size != Beacon.BEACON_SIZE
					|| buffer.get() != 'Z'
					|| buffer.get() != 'R'
					|| buffer.get() != 'E'
					|| buffer.get() != Beacon.BEACON_VERSION)
				return true;       //  Ignore invalid beacons

			//  If we got a UUID and it's not our own beacon, we have a peer
			final Beacon beacon = new Beacon(buffer);
			if (!beacon.uuid.equals(uuid) && (beacon.listenPort != 0)) {
				final ZreIdentity peer_identity = new ZreIdentity(beacon.uuid);
				final String peer_endpoint = String.format("tcp://%s:%d", udp.from(), beacon.listenPort);
				final ZrePeer peer = requirePeer(peer_identity, null, peer_endpoint);
				peer.refresh();
			}

			return true;
		}

		/**
		 * Send more beacon
		 */
		public void sendBeacon() {
			// XXX saki use field value to avoid unnecessary object allocation in loop
			try {
				udp.send(beacon.getBuffer());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 *  We do this once a second:
		 *  - if peer has gone quiet, send TCP ping
		 *  - if peer has disappeared, expire it
		 */
		public void pingAllPeers() {
			final Iterator<Map.Entry<ZreIdentity, ZrePeer>> it = peers.entrySet().iterator();
			while (it.hasNext()) {
				try {    // XXX saki
					final Map.Entry<ZreIdentity, ZrePeer> entry = it.next();
					final ZreIdentity identity = entry.getKey();
					final ZrePeer peer = entry.getValue();
					if (System.currentTimeMillis() >= peer.expiredAt()) {
						log.info(ZreLogMsg.ZRE_LOG_MSG_EVENT_EXIT, peer.identityString(), peer.endpoint());
						//  If peer has really vanished, expire it
						appPipe.sendMore("EXIT");
						appPipe.sendMore(identity.toString());
						appPipe.send(peer.name());
						deletePeerFromGroups(peer_groups, peer);
						it.remove();
						peer.destroy();
					} else if (System.currentTimeMillis() >= peer.evasiveAt()) {
						//  If peer is being evasive, force a TCP ping.
						//  TODO: do this only once for a peer in this state;
						//  it would be nicer to use a proper state machine
						//  for peer management.
						final ZreMsg msg = new ZreMsg(ZreMsg.PING);
						peer.send(msg);
					}
				} catch (final ZMQException e) {    // XXX saki
					break;
				}
			}
		}

		/**
		 * get uuid as string
		 * @return
		 */
		public String uuid() {
			return uuidString;
		}
	}

	/**
	 * Send message to all peers
	 * @param peers
	 * @param msg
	 */
	private static void sendPeers(final Map<ZreIdentity, ZrePeer> peers, final ZreMsg msg) {
		for (final ZrePeer peer : peers.values())
			peer.send(msg);
	}

	/**
	 * Remove peer from group, if it's a member
	 * @param groups
	 * @param peer
	 */
	private static void deletePeerFromGroups(final Map<String, ZreGroup> groups, final ZrePeer peer) {
		for (final ZreGroup group : groups.values())
			group.leave(peer);
	}

	private static class ZreInterfaceAgent implements ZThread.IAttachedRunnable {

		private final WeakReference<ZreInterface> mWeakParent; // XXX saki

		public ZreInterfaceAgent(final ZreInterface parent) {
			mWeakParent = new WeakReference<ZreInterface>(parent); // XXX saki
		}

		@Override
		public void run(final Object[] args, final ZContext ctx, final Socket app_pipe) {
			Agent agent = null;
			final ZreInterface parent = mWeakParent.get();    // XXX saki
			if (parent != null) {
				synchronized (parent.mPipeSync) {
					try {
						agent = Agent.newAgent(ctx, app_pipe, parent);
						if (agent != null) {
							// success
							parent.mUuid = agent.uuid();
							parent.initialized = true;
						}
					} catch (final Exception e) {
						e.printStackTrace();
					}
					parent.mPipeSync.notifyAll();
				}
			}
			if (agent == null) {   //  Interrupted etc.
				return;
			}

			long pingAt = System.currentTimeMillis();
			final Poller items = ctx.getContext().poller();

			items.register(agent.appPipe, Poller.POLLIN);
			items.register(agent.inbox, Poller.POLLIN);
			items.register(agent.udp.handle(), Poller.POLLIN);

			while (!Thread.currentThread().isInterrupted()) {
				long timeout = pingAt - System.currentTimeMillis();
				assert (timeout <= PING_INTERVAL);

				if (timeout < 0)
					timeout = 0;

				if (items.poll(timeout) < 0)
					break;      // Interrupted

				if (items.pollin(0))
					agent.recvFromApi();

				if (items.pollin(1))
					agent.recvFromPeer();

				if (items.pollin(2))
					agent.recvUdpBeacon();

				if (System.currentTimeMillis() >= pingAt) {
					agent.sendBeacon();
					pingAt = System.currentTimeMillis() + PING_INTERVAL;
					//  Ping all peers and reap any expired ones
					agent.pingAllPeers();
				}
			}
			agent.destroy();
		}
	}
}
