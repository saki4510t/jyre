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

public class ZreInterface
{
    public static final int UBYTE_MAX = 0xff;
    //  Defined port numbers, pending IANA submission
    public static final int PING_PORT_NUMBER = 9991;
    public static final int LOG_PORT_NUMBER = 9992;

    //  Constants, to be configured/reviewed
    public static final int PING_INTERVAL   = 1000;   //  Once per second
    public static final int PEER_EVASIVE    = 5000;   //  Five seconds' silence is evasive
    public static final int PEER_EXPIRED   = 10000;   //  Ten seconds' silence is expired
    
    private ZContext ctx;       //  Our context wrapper
    private Socket pipe;        //  Pipe through to agent
	private String mNodeName = "";	// XXX saki take care not to set null to this field, set "" instead. otherwise Socket#sendMore/#send crash
	private String mUuid = "";		// XXX saki
    //  ---------------------------------------------------------------------
    //  Constructor
    
    public ZreInterface () 
    {
    	this(null);
    }
    
    public ZreInterface (final String name) // XXX saki
    {
		mNodeName = !TextUtils.isEmpty(name) ? name : "";
        ctx = new ZContext ();
        ctx.setLinger(100);	// XXX saki
        pipe = ZThread.fork (ctx, new ZreInterfaceAgent (this));
        pipe.setLinger(100);	// XXX saki
    }
    
    //  ---------------------------------------------------------------------
    //  Destructor
    public void destroy ()
    {
		pipe.close();	// XXX saki
        ctx.destroy ();
    }
    
   	public String uuid()	// XXX saki
   	{
		return mUuid != null ? mUuid : "";
	}

	public void setName(final String name)	// XXX saki
	{
		mNodeName = !TextUtils.isEmpty(name) ? name : "";
	}

	public String name()	// XXX saki
	{
		return mNodeName;
	}

    //  ---------------------------------------------------------------------
    //  Receive next message from interface
    //  Returns ZMsg object, or NULL if interrupted
    public ZMsg recv ()
    {
        return ZMsg.recvMsg (pipe);
    }
    
    //  ---------------------------------------------------------------------
    //  Join a group
    public void join (final String group_name) 
    {
        pipe.sendMore ("JOIN");
		pipe.sendMore(mNodeName);	// XXX saki
        pipe.send (group_name);
    }
    
    //  ---------------------------------------------------------------------
    //  Leave a group
    public void leave (final String group_name)
    {
        pipe.sendMore ("LEAVE");
		pipe.sendMore(mNodeName);	// XXX saki
        pipe.send (group_name);
    }
    
    //  ---------------------------------------------------------------------
    //  Send message to single peer; peer ID is first frame in message
    //  Destroys message after sending
    public void whisper (final ZMsg msg) 
    {
        pipe.sendMore ("WHISPER");
		pipe.sendMore(mNodeName);	// XXX saki
        msg.send (pipe);
    }

    //  ---------------------------------------------------------------------
    //  Send message to a group of peers
    public void shout (final ZMsg msg) 
    {
        pipe.sendMore ("SHOUT");
		pipe.sendMore(mNodeName);	// XXX saki
        msg.send (pipe);
    }
    
    //  ---------------------------------------------------------------------
    //  Return interface handle, for polling
    public Socket handle ()
    {
        return pipe;
    }
    
    //  ---------------------------------------------------------------------
    //  Set node header value
    public void setHeader (final String key_name, final String value_format, final Object ... args)
    {
        pipe.sendMore ("SET");
		pipe.sendMore(mNodeName);	// XXX saki
        pipe.sendMore (key_name);
        pipe.send (String.format (value_format, args));
    }
    
    //  ---------------------------------------------------------------------
    //  Publish file into virtual space
    public void publish (final String pathname, final String virtual)
    {
        pipe.sendMore ("PUBLISH");
		pipe.sendMore(mNodeName);	// XXX saki
        pipe.sendMore (pathname);
        pipe.send (virtual);
    }
    
    //  =====================================================================
    //  Asynchronous part, works in the background
    
    //  Beacon frame has this format:
    //
    //  Z R E       3 bytes
    //  version     1 byte, %x01
    //  UUID        16 bytes
    //  port        2 bytes in network order

    protected static class Beacon 
    {
        public static final int BEACON_SIZE = 22;

        public static final String BEACON_PROTOCOL = "ZRE";
        public static final byte BEACON_VERSION = 0x01;
        
        private final byte [] protocol = BEACON_PROTOCOL.getBytes ();
        private final byte version = BEACON_VERSION;
        private UUID uuid;
        private int port;
        
        public Beacon (final ByteBuffer buffer)
        {
            final long msb = buffer.getLong ();
            final long lsb = buffer.getLong ();
            uuid = new UUID (msb, lsb);
            port = buffer.getShort ();
            if (port < 0)
                port = (0xffff) & port;
        }
        
        public Beacon (final UUID uuid, final int port)
        {
            this.uuid = uuid;
            this.port = port;
        }
        
        public ByteBuffer getBuffer ()
        {
            final ByteBuffer buffer = ByteBuffer.allocate (BEACON_SIZE);
            buffer.put (protocol);
            buffer.put (version);
            buffer.putLong (uuid.getMostSignificantBits ());
            buffer.putLong (uuid.getLeastSignificantBits ());
            buffer.putShort ((short) port);
            buffer.flip ();
            return buffer;
        }

    }
    
    private static String uuidStr (final UUID uuid)
    {
        return uuid.toString ().replace ("-","").toUpperCase ();
    }
    
    private static final String OUTBOX = ".outbox";
    private static final String INBOX = ".inbox";
    
    protected static class Agent 
    {
		private final WeakReference<ZContext> weakCtx;	//  CZMQ context, XXX saki change to WeakReference
        private final Socket pipe;              //  Pipe back to application
        private final ZreUdp udp;               //  UDP object
        private final ZreLog log;               //  Log object
        private final UUID uuid;                //  Our UUID as binary blob
        private final String identity;          //  Our UUID as hex string
        private final Socket inbox;             //  Our inbox socket (ROUTER)
        private final String host;              //  Our host IP address
        private final int port;                 //  Our inbox port number
        private final String endpoint;          //  ipaddress:port endpoint
        private int status;                     //  Our own change counter
        private final Map <String, ZrePeer> peers;            //  Hash of known peers, fast lookup
        private final Map <String, ZreGroup> peer_groups;     //  Groups that our peers are in
        private final Map <String, ZreGroup> own_groups;      //  Groups that we are in
        private final Map <String, String> headers;           //  Our header values
		private final WeakReference<ZreInterface> mWeakParent;	// XXX saki
        
        private Agent (final ZContext ctx, final Socket pipe, final Socket inbox, 
						final ZreUdp udp, final int port, final ZreInterface parent)
        {
			this.weakCtx = new WeakReference<ZContext>(ctx); // XXX saki
            this.pipe = pipe;
            this.inbox = inbox;
            this.udp = udp;
            this.port = port;
			mWeakParent = new WeakReference<ZreInterface>(parent);
            
            host = udp.host ();
            uuid = UUID.randomUUID ();
            identity = uuidStr (uuid);
            endpoint = String.format ("%s:%d", host, port);
            peers = new HashMap <String, ZrePeer> ();
            peer_groups = new HashMap <String, ZreGroup> ();
            own_groups = new HashMap <String, ZreGroup> ();
            headers = new HashMap <String, String> ();
            
            log = new ZreLog (endpoint);
        }
        
        protected static Agent newAgent (final ZContext ctx, final Socket pipe, final ZreInterface parent) 
        {
            final Socket inbox = ctx.createSocket (ZMQ.ROUTER);
            if (inbox == null)      //  Interrupted
                return null;

            final ZreUdp udp = new ZreUdp (PING_PORT_NUMBER);
            final int port = inbox.bindToRandomPort ("tcp://*", 0xc000, 0xffff);
            if (port < 0) {          //  Interrupted
                System.err.println ("Failed to bind a random port");
                udp.destroy ();
                return null;
            }
            
            return new Agent (ctx, pipe, inbox, udp, port, parent);
        }
        
        protected void destroy () 
        {
            for (final ZrePeer peer : peers.values ())
                peer.destroy ();
			peers.clear();
            for (final ZreGroup group : peer_groups.values ())
                group.destroy ();
			own_groups.clear();
            for (final ZreGroup group : own_groups.values ())
                group.destroy ();
			own_groups.clear();
            
            udp.destroy ();
            log.destroy ();
            
        }
        
        private int incStatus ()
        {
            if (++status > UBYTE_MAX)
                status = 0;
            return status;
        }
        
        //  Delete peer for a given endpoint
        private void purgePeer ()
        {
            for (final Map.Entry <String, ZrePeer> entry : peers.entrySet ()) {
                final ZrePeer peer = entry.getValue ();
                if (peer.endpoint ().equals (endpoint))
                    peer.disconnect ();
            }
        }
        
        //  Find or create peer via its UUID string
        private ZrePeer requirePeer (final String identity, final String name, final String address, final int port)	// XXX saki
        {
            ZrePeer peer = peers.get (identity);
            if (peer == null) {
                //  Purge any previous peer on same endpoint
                final String endpoint = String.format ("%s:%d", address, port);
                
                purgePeer ();

				final ZContext ctx = weakCtx.get();	// XXX saki
				if (ctx != null) {	// XXX saki
	                peer = ZrePeer.newPeer (identity, peers, ctx);
    	            peer.connect (this.identity, endpoint);

        	        //  Handshake discovery by sending HELLO as first message
            	    final ZreMsg msg = new ZreMsg (ZreMsg.HELLO);
                	msg.setIpaddress (this.udp.host ()); 
    	            msg.setMailbox (this.port);
	                msg.setGroups (own_groups.keySet ());
        	        msg.setStatus (status);
					msg.setName(name);    // XXX saki
            	    msg.setHeaders (new HashMap <String, String> (headers));
                	peer.send (msg);

	                log.info (ZreLogMsg.ZRE_LOG_MSG_EVENT_ENTER,
                              peer.endpoint (), endpoint);

    	            //  Now tell the caller about the peer
        	        pipe.sendMore ("ENTER");
					pipe.sendMore(identity);    // XXX saki
					pipe.send(name);    // XXX saki
            	}
            }
            return peer;
        }
        
        //  Find or create group via its name
        private ZreGroup requirePeerGroup (final String group_name)
        {
            ZreGroup group = peer_groups.get (group_name);
            if (group == null)
                group = ZreGroup.newGroup (group_name, peer_groups);
            return group;

        }
        
        private ZreGroup joinPeerGroup (final ZrePeer peer, final String my_name, final String group_name)	// XXX saki
        {
            final ZreGroup group = requirePeerGroup (group_name);
            group.join (peer);
            
            //  Now tell the caller about the peer joined a group
            pipe.sendMore ("JOIN");
            pipe.sendMore (peer.identity ());
			pipe.sendMore(my_name);	// XXX saki
            pipe.send (group_name);
            
            return group;
        }
        
        private ZreGroup leavePeerGroup (final ZrePeer peer, final String my_name, final String group_name)	// XXX saki
        {
            final ZreGroup group = requirePeerGroup (group_name);
            group.leave (peer);
            
            //  Now tell the caller about the peer joined a group
            pipe.sendMore ("LEAVE");
            pipe.sendMore (peer.identity ());
			pipe.sendMore(my_name);	// XXX saki
            pipe.send (group_name);
            
            return group;
        }

        //  Here we handle the different control messages from the front-end
        protected boolean recvFromApi ()
        {
            //  Get the whole message off the pipe in one go
            final ZMsg request = ZMsg.recvMsg (pipe);
            final String command = request.popString ();
            if (command == null)
                return false;                  //  Interrupted

            if (command.equals ("WHISPER")) {
				final String name = request.popString();	// XXX saki
                //  Get peer to send message to
                final String identity = request.popString ();
                final ZrePeer peer = peers.get (identity);

                //  Send frame on out to peer's mailbox, drop message
                //  if peer doesn't exist (may have been destroyed)
                if (peer != null) {
                    final ZreMsg msg = new ZreMsg (ZreMsg.WHISPER);
                    msg.setContent (request.pop ());
					msg.setName(name);	// XXX saki
                    peer.send (msg);
                }
            } else if (command.equals ("SHOUT")) {
				final String name = request.popString();	// XXX saki
                //  Get group to send message to
                final String group_name = request.popString ();
                final ZreGroup group = peer_groups.get (group_name);
                if (group != null) {
                    final ZreMsg msg = new ZreMsg (ZreMsg.SHOUT);
                    msg.setGroup (group_name);
					msg.setName(name);	// XXX saki
                    msg.setContent (request.pop ());
                    group.send (msg);
                }
            } else if (command.equals ("JOIN")) {
				final String name = request.popString();	// XXX saki
                final String group_name = request.popString ();
                ZreGroup group = own_groups.get (group_name);
                if (group == null) {
                    //  Only send if we're not already in group
                    group = ZreGroup.newGroup (group_name, own_groups);
                    final ZreMsg msg = new ZreMsg (ZreMsg.JOIN);
                    msg.setGroup (group_name);
					msg.setName(name);	// XXX saki
                    //  Update status before sending command
                    msg.setStatus (incStatus ());
					try {	// XXX saki
	                    sendPeers (peers, msg);
					} catch (final ZMQException e) {
						e.printStackTrace();
					}
                    msg.destroy ();
                    log.info (ZreLogMsg.ZRE_LOG_MSG_EVENT_JOIN, null, group_name);
                }
            } else if (command.equals ("LEAVE")) {
				final String name = request.popString();	// XXX saki
                final String group_name = request.popString ();
                final ZreGroup group = own_groups.get (group_name);
                if (group != null) {
                    //  Only send if we are actually in group
                    final ZreMsg msg = new ZreMsg (ZreMsg.LEAVE);
                    msg.setGroup (group_name);
					msg.setName(name);	// XXX saki
                    //  Update status before sending command
                    msg.setStatus (incStatus ());
					try {	// XXX saki
	                    sendPeers (peers, msg);
					} catch (final ZMQException e) {
						e.printStackTrace();
					}
                    own_groups.remove (group_name);
                    log.info (ZreLogMsg.ZRE_LOG_MSG_EVENT_LEAVE, null, group_name);
                }
            } else if (command.equals ("SET")) {
                final String node_name = request.popString ();
                final String name = request.popString ();
                final String value = request.popString ();
                headers.put (name, value);
            } else {
            	System.err.println("Unknown command: " + command);
            }

            
            request.destroy ();
            return true;
        }
        
        //  Here we handle messages coming from other peers
        protected boolean recvFromPeer ()
        {
            //  Router socket tells us the identity of this peer
            final ZreMsg msg = ZreMsg.recv (inbox);
            if (msg == null)
                return false;               //  Interrupted

            final String identity = new String (msg.address ().getData ());
            
            //  On HELLO we may create the peer if it's unknown
            //  On other commands the peer must already exist
            ZrePeer peer = peers.get (identity);
            if (msg.id () == ZreMsg.HELLO) {
                peer = requirePeer (
                    identity, msg.name(), msg.ipaddress (), msg.mailbox ());
                assert (peer != null);
                peer.setReady (true);
            }
            //  Ignore command if peer isn't ready
            if (peer == null || !peer.ready ()) {
                msg.destroy ();
                return true;
            }

            if (!peer.checkMessage (msg)) {
                System.err.printf ("W: [%s] lost messages from %s\n", this.identity, identity);
                assert (false);
            }

            //  Now process each command
            if (msg.id () == ZreMsg.HELLO) {
                //  Join peer to listed groups
				final String peer_name = msg.name();	// XXX saki
                for (final String group_name : msg.groups ()) {
                    joinPeerGroup (peer, peer_name, group_name);	// XXX saki
                }
                //  Hello command holds latest status of peer
                peer.setStatus (msg.status ());

                //  Store peer headers for future reference
                peer.setHeaders (msg.headers ());
            }
            else
            if (msg.id () == ZreMsg.WHISPER) {
                //  Pass up to caller API as WHISPER event
                final ZFrame cookie = msg.content ();
                pipe.sendMore ("WHISPER");
                pipe.sendMore (identity);
				pipe.sendMore(msg.name());	// XXX saki
                cookie.send (pipe, 0); // let msg free the frame
            }
            else
            if (msg.id () == ZreMsg.SHOUT) {
                //  Pass up to caller as SHOUT event
                final ZFrame cookie = msg.content ();
                pipe.sendMore ("SHOUT");
                pipe.sendMore (identity);
				pipe.sendMore(msg.name());	// XXX saki
                pipe.sendMore (msg.group ());
                cookie.send (pipe, 0); // let msg free the frame
            }
            else
            if (msg.id () == ZreMsg.PING) {
                final ZreMsg pingOK = new ZreMsg (ZreMsg.PING_OK);
                peer.send (pingOK);
            }
            else
            if (msg.id () == ZreMsg.JOIN) {
                joinPeerGroup (peer, msg.name(), msg.group ()); // XXX saki
                assert (msg.status () == peer.status ());
            }
            else
            if (msg.id () == ZreMsg.LEAVE) {
                leavePeerGroup (peer, msg.name(), msg.group ()); // XXX saki
                assert (msg.status () == peer.status ());
            }
            msg.destroy ();

            //  Activity from peer resets peer timers
            peer.refresh ();
            return true;
        }

        //  Handle beacon
        protected boolean recvUdpBeacon ()
        {
            ByteBuffer buffer = ByteBuffer.allocate (Beacon.BEACON_SIZE);
            
            //  Get beacon frame from network
            int size = 0;
            try {
                size = udp.recv (buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            buffer.rewind ();
            
            //  Basic validation on the frame
            if (size != Beacon.BEACON_SIZE
                    || buffer.get () != 'Z'
                    || buffer.get () != 'R'
                    || buffer.get () != 'E'
                    || buffer.get () != Beacon.BEACON_VERSION)
                return true;       //  Ignore invalid beacons
            
            //  If we got a UUID and it's not our own beacon, we have a peer
            final Beacon beacon = new Beacon (buffer);
            if (!beacon.uuid.equals (uuid)) {
                final String identity = uuidStr (beacon.uuid);
				final ZreInterface parent = mWeakParent.get();	// XXX saki
				final String name = parent != null ? parent.name() : ""; // XXX saki
                final ZrePeer peer = requirePeer (identity, name, udp.from (), beacon.port);	// XXX saki
                peer.refresh ();
            }
            
            return true;
        }

        //  Send moar beacon
        public void sendBeacon ()
        {
            final Beacon beacon = new Beacon (uuid, port);
            try {
                udp.send (beacon.getBuffer ());
            } catch (IOException e) {
                e.printStackTrace ();
            }
        }
        
        //  We do this once a second:
        //  - if peer has gone quiet, send TCP ping
        //  - if peer has disappeared, expire it
        public void pingAllPeers ()
        {
            final Iterator <Map.Entry <String, ZrePeer>> it = peers.entrySet ().iterator ();
            while (it.hasNext ()) {
				try {	// XXX saki
	                final Map.Entry<String, ZrePeer> entry = it.next ();
    	            final String identity = entry.getKey ();
        	        final ZrePeer peer = entry.getValue ();
            	    if (System.currentTimeMillis () >= peer.expiredAt ()) {
                	    log.info (ZreLogMsg.ZRE_LOG_MSG_EVENT_EXIT,
                    	        peer.endpoint (),
                        	    peer.endpoint ());
	                    //  If peer has really vanished, expire it
    	                pipe.sendMore ("EXIT");
						pipe.sendMore (identity);	// XXX saki
						pipe.send (identity);	// XXX saki
                	    deletePeerFromGroups (peer_groups, peer);
                    	it.remove ();
	                    peer.destroy ();
    	            } 
        	        else 
            	    if (System.currentTimeMillis () >= peer.evasiveAt ()){
                	    //  If peer is being evasive, force a TCP ping.
                    	//  TODO: do this only once for a peer in this state;
	                    //  it would be nicer to use a proper state machine
    	                //  for peer management.
        	            final ZreMsg msg = new ZreMsg (ZreMsg.PING);
            	        peer.send (msg);
                	}
				} catch (final ZMQException e) {	// XXX saki
					break;
				}
            }
        }

		public String uuid() {
			return identity;
		}
    }
    
    //  Send message to all peers
    private static void sendPeers (final Map <String, ZrePeer> peers, final ZreMsg msg)
    {
        for (final ZrePeer peer : peers.values ())
            peer.send (msg);
    }
    
    //  Remove peer from group, if it's a member
    private static void deletePeerFromGroups (final Map <String, ZreGroup> groups, final ZrePeer peer)
    {
        for (final ZreGroup group : groups.values ())
            group.leave (peer);
    }
    
    private static class ZreInterfaceAgent 
                            implements ZThread.IAttachedRunnable 
    {

		private final WeakReference<ZreInterface> mWeakParent; // XXX saki
		private final Object mSync = new Object(); // XXX saki
		public ZreInterfaceAgent(final ZreInterface parent)
		{
			mWeakParent = new WeakReference<ZreInterface>(parent); // XXX saki
		}

        @Override
        public void run (final Object[] args, final ZContext ctx, final Socket pipe)
        {
			final Agent agent;
			synchronized (mSync) { // XXX saki
				final ZreInterface parent = mWeakParent.get();
				if (parent != null) {
					agent = Agent.newAgent(ctx, pipe, parent);
					parent.mUuid = agent != null ? agent.uuid() : "";
					mSync.notifyAll();
					if (agent == null) {   //  Interrupted
						return;
					}
				} else {
					return;
				}
			}
            
            long pingAt = System.currentTimeMillis ();
            final Poller items = ctx.getContext ().poller ();
            
            items.register (agent.pipe, Poller.POLLIN);
            items.register (agent.inbox, Poller.POLLIN);
            items.register (agent.udp.handle (), Poller.POLLIN);
            
            while (!Thread.currentThread ().isInterrupted ()) {
                long timeout = pingAt - System.currentTimeMillis ();
                assert (timeout <= PING_INTERVAL);
                
                if (timeout < 0)
                    timeout = 0;
                
                if (items.poll (timeout) < 0)
                    break;      // Interrupted
                
                if (items.pollin (0))
                    agent.recvFromApi ();
                
                if (items.pollin (1))
                    agent.recvFromPeer ();
                
                if (items.pollin (2))
                    agent.recvUdpBeacon ();
                
                if (System.currentTimeMillis () >= pingAt) {
                    agent.sendBeacon ();
                    pingAt = System.currentTimeMillis () + PING_INTERVAL;
                    //  Ping all peers and reap any expired ones
                    agent.pingAllPeers ();
                }
            }
            agent.destroy ();
        }
    }
}
