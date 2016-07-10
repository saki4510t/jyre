/*  =========================================================================
    ZreMsg.java
    
    Generated codec class for ZreMsg
    -------------------------------------------------------------------------
    Copyright (c) 1991-2012 iMatix Corporation -- http://www.imatix.com     
    Copyright other contributors as noted in the AUTHORS file.              
                                                                            
    This file is part of Zyre, an open-source framework for proximity-based 
    peer-to-peer applications -- See http://zyre.org.                       
                                                                            
    This is free software; you can redistribute it and/or modify it under   
    the terms of the GNU Lesser General Public License as published by the  
    Free Software Foundation; either version 3 of the License, or (at your  
    option) any later version.                                              
                                                                            
    This software is distributed in the hope that it will be useful, but    
    WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTA-   
    BILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General  
    Public License for more details.                                        
                                                                            
    You should have received a copy of the GNU Lesser General Public License
    along with this program. If not, see http://www.gnu.org/licenses/.      
    =========================================================================
    XXX t_saki@serenegiant.com
    Use ZreIdentity and fit to 36/ZRE ZeroM RFC, add JavaDoc
*/

/*  These are the zre_msg messages
	HELLO - Greet a peer so it can connect back to us
		version		number 1
		sequence	number 2
        endpoint	string
        groups		strings
        status		number 1
		name		string XXX saki
		headers		dictionary
    WHISPER - Send a message to a peer
		version		number 1
        sequence	number 2
        content	frame
    SHOUT - Send a message to a group
		version		number 1
        sequence	number 2
        group		string
        content		frame
    JOIN - Join a group
		version		number 1
        sequence	number 2
        group		string
        status		number 1
    LEAVE - Leave a group
		version		number 1
        sequence	number 2
        group		string
        status		number 1
    PING - Ping a peer that has gone silent
		version		number 1
        sequence	number 2
    PING_OK - Reply to a peer's ping
		version		number 1
        sequence	number 2
*/

package org.zyre;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.nio.ByteBuffer;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

/**
 * Opaque class structure
 */
public class ZreMsg {
	public static final int ZRE_MSG_VERSION = 2;

	public static final int HELLO = 1;
	public static final int WHISPER = 2;
	public static final int SHOUT = 3;
	public static final int JOIN = 4;
	public static final int LEAVE = 5;
	public static final int PING = 6;
	public static final int PING_OK = 7;

	//  Structure of our class
	private ZFrame routing_id;          //  Routing_id from ROUTER, if any
	private int id;                     //  ZreMsg message ID
	private ByteBuffer needle;          //  Read/write pointer for serialization
	private int version = ZRE_MSG_VERSION;
	private int sequence;
	private String endpoint;
	private List<String> groups;
	private int status;
	private Map<String, String> headers;
	private int headersBytes;
	private ZFrame content;
	private String group;
	private String name;

//--------------------------------------------------------------------------
	/**
	 * constructor
	 * @param id
	 */
	public ZreMsg(final int id) {
		this.id = id;
	}

//--------------------------------------------------------------------------
	/**
	 * Destroy the zre_msg
 	 */
	public void destroy() {
		//  Free class properties
		if (routing_id != null)
			routing_id.destroy();
		routing_id = null;

		//  Destroy frame fields
		if (content != null)
			content.destroy();
		content = null;
	}

//--------------------------------------------------------------------------
//  Network data encoding helper methods

	/**
	 * Put a 1-byte number to the frame
	 * @param value
	 */
	private final void putNumber1(final int value) {
		needle.put((byte) value);
	}

	/**
	 * Get a 1-byte number to the frame then make it unsigned
	 * @return
	 */
	private int getNumber1() {
		int value = needle.get();
		if (value < 0)
			value = (0xff) & value;
		return value;
	}

	/**
	 * Put a 2-byte number to the frame
	 * @param value
	 */
	private final void putNumber2(final int value) {
		needle.putShort((short) value);
	}

	/**
	 * Get a 2-byte number to the frame
	 * @return
	 */
	private int getNumber2() {
		int value = needle.getShort();
		if (value < 0)
			value = (0xffff) & value;
		return value;
	}

	/**
	 * Put a 4-byte number to the frame
	 */
	private final void putNumber4(final long value) {
		needle.putInt((int) value);
	}

	/**
	 * Get a 4-byte number to the frame then make it unsigned
 	 */
	private long getNumber4() {
		long value = needle.getInt();
		if (value < 0)
			value = (0xffffffff) & value;
		return value;
	}

	/**
	 * Put a 8-byte number to the frame
	 * @param value
	 */
	public void putNumber8(final long value) {
		needle.putLong(value);
	}

	/**
	 * Get a 8-byte number to the frame
	 * @return
	 */
	public long getNumber8() {
		return needle.getLong();
	}


	/**
	 * Put a block to the frame
	 */
	private void putBlock(final byte[] value, final int size) {
		needle.put(value, 0, size);
	}

	private byte[] getBlock(final int size) {
		byte[] value = new byte[size];
		needle.get(value);

		return value;
	}

	/**
	 * Put a string to the frame
	 */
	public void putString(final String value) {
		needle.put((byte) value.length());
		needle.put(value.getBytes());
	}

	/**
	 * Get a string from the frame
	 */
	public String getString() {
		final int size = getNumber1();
		final byte[] value = new byte[size];
		needle.get(value);

		return new String(value);
	}

	/**
	 * Put a long string to the frame
	 */
	public void putLongString(final String value) {
		needle.putInt(value.length());
		needle.put(value.getBytes());
	}

	/**
	 * Get a long string from the frame
	 */
	public String getLongString() {
		final long size = getNumber4();
		final byte[] value = new byte[(int)size];
		needle.get(value);

		return new String(value);
	}

//  --------------------------------------------------------------------------
	/**
	 * Receive and parse a ZreMsg from the socket. Returns new object or
	 * null if error. Will block if there's no message waiting.
	 * @param input
	 * @return
	 */
	public static ZreMsg recv(final Socket input) {
		assert (input != null);
		final ZreMsg self = new ZreMsg(0);
		ZFrame frame = null;

		try {
			//  Read valid message frame from socket; we loop over any
			//  garbage data we might receive from badly-connected peers
			while (true) {
				//  If we're reading from a ROUTER socket, get routing_id
				if (input.getType() == ZMQ.ROUTER) {
					self.routing_id = ZFrame.recvFrame(input);
					if (self.routing_id == null)
						return null;         //  Interrupted
					if (!input.hasReceiveMore())
						throw new IllegalArgumentException("unexpectedly has no more message");
				}
				//  Read and parse command in frame
				frame = ZFrame.recvFrame(input);
				if (frame == null)
					return null;             //  Interrupted

				//  Get and check protocol signature
				self.needle = ByteBuffer.wrap(frame.getData());
				final int signature = self.getNumber2();
				if (signature == (0xAAA0 | 1))
					break;                  //  Valid signature

				//  Protocol assertion, drop message
				while (input.hasReceiveMore()) {
					frame.destroy();
					frame = ZFrame.recvFrame(input);
				}
				frame.destroy();
			}

			//  Get message id, which is first byte in frame
			self.id = self.getNumber1();
			long listSize;
			long hashSize;

			switch(self.id) {
			case HELLO:
				self.version = self.getNumber1();
				if (self.version != ZRE_MSG_VERSION) {
					throw new RuntimeException("invalid version:" + self.version);
				}
				self.sequence = self.getNumber2();
				self.endpoint = self.getString();
				{
					listSize = self.getNumber4();
					self.groups = new ArrayList<String>();
					while (listSize-- > 0) {
						final String string = self.getLongString();
						self.groups.add(string);
					}
				}
				self.status = self.getNumber1();
				self.name = self.getString();
				{
					hashSize = self.getNumber4();
					self.headers = new HashMap<String, String>();
					while (hashSize-- > 0) {
						final String key = self.getString();
						final String value = self.getLongString();
						self.headers.put(key, value);
					}
				}
				break;

			case WHISPER:
				self.version = self.getNumber1();
				if (self.version != ZRE_MSG_VERSION) {
					throw new RuntimeException("invalid version:" + self.version);
				}
				self.sequence = self.getNumber2();
				//  Get next frame, leave current untouched
				if (!input.hasReceiveMore())
					throw new IllegalArgumentException();
				self.content = ZFrame.recvFrame(input);
				break;

			case SHOUT:
				self.version = self.getNumber1();
				if (self.version != ZRE_MSG_VERSION) {
					throw new RuntimeException("invalid version:" + self.version);
				}
				self.sequence = self.getNumber2();
				self.group = self.getString();
				//  Get next frame, leave current untouched
				if (!input.hasReceiveMore())
					throw new IllegalArgumentException();
				self.content = ZFrame.recvFrame(input);
				break;

			case JOIN:
				self.version = self.getNumber1();
				if (self.version != ZRE_MSG_VERSION) {
					throw new RuntimeException("invalid version:" + self.version);
				}
				self.sequence = self.getNumber2();
				self.group = self.getString();
				self.status = self.getNumber1();
				break;

			case LEAVE:
				self.version = self.getNumber1();
				if (self.version != ZRE_MSG_VERSION) {
					throw new RuntimeException("invalid version:" + self.version);
				}
				self.sequence = self.getNumber2();
				self.group = self.getString();
				self.status = self.getNumber1();
				break;

			case PING:
				self.version = self.getNumber1();
				if (self.version != ZRE_MSG_VERSION) {
					throw new RuntimeException("invalid version:" + self.version);
				}
				self.sequence = self.getNumber2();
				break;

			case PING_OK:
				self.version = self.getNumber1();
				if (self.version != ZRE_MSG_VERSION) {
					throw new RuntimeException("invalid version:" + self.version);
				}
				self.sequence = self.getNumber2();
				break;

			default:
				throw new IllegalArgumentException("unknown id:" + self.id);
			}

			return self;

		} catch (final Exception e) {
			//  Error returns
			System.out.printf("E: malformed message id=`%d`,%s\n", self.id, e.getMessage());
			self.destroy();
			return null;
		} finally {
			if (frame != null)
				frame.destroy();
		}
	}

	/**
	 * Count size of key=value pair
	 * @param entry
	 * @param self
	 */
	private static void headersCount(final Map.Entry<String, String> entry, final ZreMsg self) {
		self.headersBytes += entry.getKey().length() + 1 + entry.getValue().length() + 4;
	}

	//  Serialize headers key=value pair
	private static void
	headersWrite(final Map.Entry<String, String> entry, final ZreMsg self) {
		self.putString(entry.getKey());
		self.putLongString(entry.getValue());
	}


//--------------------------------------------------------------------------
	/**
	 * Send the ZreMsg to the socket, and destroy it
	 * @param socket
	 * @return
	 */
	public boolean send(Socket socket) {
		assert (socket != null);

		//  Calculate size of serialized data
		int frameSize = 2 + 1;          //  Signature and message ID
		switch(id) {
		case HELLO:
			//  version is a 1-byte integer
			frameSize += 1;
			//  sequence is a 2-byte integer
			frameSize += 2;
			//  endpoint is a string with 1-byte length
			frameSize++;       //  Size is one octet
			if (endpoint != null)
				frameSize += endpoint.length();
			//  groups is an array of strings
			frameSize += 4;       //  Size is 4 octets
			if (groups != null) {
				for (final String value : groups)
					frameSize += 4 + value.length();
			}
			//  status is a 1-byte integer
			frameSize += 1;
			//  name is a string with 1-byte length
			frameSize++;       //  Size is one octet
			if (!TextUtils.isEmpty(name)) {
				frameSize += name.length();
			}
			//  headers is an array of key=value strings
			frameSize += 4;       //  Size is 4 octets
			if (headers != null) {
				headersBytes = 0;
				for (Map.Entry<String, String> entry : headers.entrySet()) {
					headersCount(entry, this);
				}
				frameSize += headersBytes;
			}
			break;

		case WHISPER:
			//  version is a 1-byte integer
			frameSize += 1;
			//  sequence is a 2-byte integer
			frameSize += 2;
			break;

		case SHOUT:
			//  version is a 1-byte integer
			frameSize += 1;
			//  sequence is a 2-byte integer
			frameSize += 2;
			//  group is a string with 1-byte length
			frameSize++;       //  Size is one octet
			if (group != null)
				frameSize += group.length();
			break;

		case JOIN:
			//  version is a 1-byte integer
			frameSize += 1;
			//  sequence is a 2-byte integer
			frameSize += 2;
			//  group is a string with 1-byte length
			frameSize++;       //  Size is one octet
			if (group != null)
				frameSize += group.length();
			//  status is a 1-byte integer
			frameSize += 1;
			break;

		case LEAVE:
			//  version is a 1-byte integer
			frameSize += 1;
			//  sequence is a 2-byte integer
			frameSize += 2;
			//  group is a string with 1-byte length
			frameSize++;       //  Size is one octet
			if (group != null)
				frameSize += group.length();
			//  status is a 1-byte integer
			frameSize += 1;
			break;

		case PING:
			//  version is a 1-byte integer
			frameSize += 1;
			//  sequence is a 2-byte integer
			frameSize += 2;
			break;

		case PING_OK:
			//  version is a 1-byte integer
			frameSize += 1;
			//  sequence is a 2-byte integer
			frameSize += 2;
			break;

		default:
			System.out.printf("E: bad message type '%d', not sent\n", id);
			assert (false);
		}
		//  Now serialize message into the frame
		final ZFrame frame = new ZFrame(new byte[frameSize]);
		needle = ByteBuffer.wrap(frame.getData());
		int frameFlags = 0;
		putNumber2(0xAAA0 | 1);
		putNumber1((byte) id);

		switch (id) {
		case HELLO:
			putNumber1(ZRE_MSG_VERSION);

			putNumber2(sequence);

			if (endpoint != null) {
				putString(endpoint);
			} else {
				putNumber1((byte) 0);      //  Empty string
			}

			if (groups != null) {
				putNumber4(groups.size());
				for (final String value : groups) {
					putLongString(value);
				}
			} else {
				putNumber4(0);      //  Empty string array
			}

			putNumber1(status);

			if (!TextUtils.isEmpty(name)) {
				putString(name);
			} else {
				putNumber1((byte) 0);
			}

			if (headers != null) {
				putNumber4(headers.size());
				for (final Map.Entry<String, String> entry : headers.entrySet()) {
					headersWrite(entry, this);
				}
			} else {
				putNumber4(0);      //  Empty dictionary
			}
			break;

		case WHISPER:
			putNumber1(ZRE_MSG_VERSION);
			putNumber2(sequence);
			frameFlags = ZMQ.SNDMORE;
			break;

		case SHOUT:
			putNumber1(ZRE_MSG_VERSION);
			putNumber2(sequence);
			if (group != null) {
				putString(group);
			} else {
				putNumber1((byte) 0);      //  Empty string
			}
			frameFlags = ZMQ.SNDMORE;
			break;

		case JOIN:
			putNumber1(ZRE_MSG_VERSION);
			putNumber2(sequence);
			if (group != null) {
				putString(group);
			} else {
				putNumber1((byte) 0);      //  Empty string
			}
			putNumber1(status);
			break;

		case LEAVE:
			putNumber1(ZRE_MSG_VERSION);
			putNumber2(sequence);
			if (group != null) {
				putString(group);
			} else {
				putNumber1((byte) 0);      //  Empty string
			}
			putNumber1(status);
			break;

		case PING:
			putNumber1(ZRE_MSG_VERSION);
			putNumber2(sequence);
			break;

		case PING_OK:
			putNumber1(ZRE_MSG_VERSION);
			putNumber2(sequence);
			break;

		}
		//  If we're sending to a ROUTER, we send the routing_id first
		if (socket.getType() == ZMQ.ROUTER) {
			assert (routing_id != null);
			if (!routing_id.send(socket, ZMQ.SNDMORE)) {
				destroy();
				return false;
			}
		}
		//  Now send the data frame
		if (!frame.send(socket, frameFlags)) {
			frame.destroy();
			destroy();
			return false;
		}

		//  Now send any frame fields, in order
		switch(id) {
		case WHISPER:
			//  If content isn't set, send an empty frame
			if (content == null)
				content = new ZFrame("".getBytes());
			if (!content.send(socket, 0)) {
				frame.destroy();
				destroy();
				return false;
			}
			break;
		case SHOUT:
			//  If content isn't set, send an empty frame
			if (content == null)
				content = new ZFrame("".getBytes());
			if (!content.send(socket, 0)) {
				frame.destroy();
				destroy();
				return false;
			}
			break;
		}
		//  Destroy ZreMsg object
		destroy();
		return true;
	}


//--------------------------------------------------------------------------
	/**
	 * Send the HELLO to the socket in one step
	 * @param output
	 * @param sequence
	 * @param name
	 * @param endpoint "tcp://ipaddress:mailbox"
	 * @param mailbox
	 * @param groups
	 * @param status
	 * @param headers
	 */
	public static void sendHello(
			final Socket output,
			final int sequence,
			final String name,
			final String endpoint,
			int mailbox,
			final Collection<String> groups,
			final int status,
			final Map<String, String> headers) {

		final ZreMsg self = new ZreMsg(ZreMsg.HELLO);
		self.setSequence(sequence);
		self.setEndpoint(endpoint);
		self.setGroups(groups);
		self.setStatus(status);
		self.setName(name);
		self.setHeaders(headers);
		self.send(output);
	}

//--------------------------------------------------------------------------
	/**
	 * Send the WHISPER to the socket in one step
	 * @param output
	 * @param sequence
	 * @param content
	 */
	public static void sendWhisper(
			final Socket output,
			final int sequence,
			final ZFrame content) {

		final ZreMsg self = new ZreMsg(ZreMsg.WHISPER);
		self.setSequence(sequence);
		self.setContent(content.duplicate());
		self.send(output);
	}

//--------------------------------------------------------------------------
	/**
	 * Send the SHOUT to the socket in one step
	 * @param output
	 * @param sequence
	 * @param group
	 * @param content
	 */
	public static void sendShout(
			final Socket output,
			final int sequence,
			final String group,
			final ZFrame content) {

		final ZreMsg self = new ZreMsg(ZreMsg.SHOUT);
		self.setSequence(sequence);
		self.setGroup(group);
		self.setContent(content.duplicate());
		self.send(output);
	}

//--------------------------------------------------------------------------
	/**
	 * Send the JOIN to the socket in one step
	 * @param output
	 * @param sequence
	 * @param group
	 * @param status
	 */
	public static void sendJoin(
			final Socket output,
			final int sequence,
			final String group,
			final int status) {

		final ZreMsg self = new ZreMsg(ZreMsg.JOIN);
		self.setSequence(sequence);
		self.setGroup(group);
		self.setStatus(status);
		self.send(output);
	}

//--------------------------------------------------------------------------
	/**
	 * Send the LEAVE to the socket in one step
	 * @param output
	 * @param sequence
	 * @param group
	 * @param status
	 */
	public static void sendLeave(
			final Socket output,
			final int sequence,
			final String group,
			final int status) {

		final ZreMsg self = new ZreMsg(ZreMsg.LEAVE);
		self.setSequence(sequence);
		self.setGroup(group);
		self.setStatus(status);
		self.send(output);
	}

//--------------------------------------------------------------------------
	/**
	 * Send the PING to the socket in one step
	 * @param output
	 * @param sequence
	 */
	public static void sendPing(
			final Socket output,
			final int sequence) {

		final ZreMsg self = new ZreMsg(ZreMsg.PING);
		self.setSequence(sequence);
		self.send(output);
	}

//--------------------------------------------------------------------------
	/**
	 * Send the PING_OK to the socket in one step
	 * @param output
	 * @param sequence
	 */
	public static void sendPing_Ok(
			final Socket output,
			final int sequence) {

		final ZreMsg self = new ZreMsg(ZreMsg.PING_OK);
		self.setSequence(sequence);
		self.send(output);
	}


//--------------------------------------------------------------------------
	/**
	 * Duplicate the ZreMsg message FIXME should #clone not a #dup!!
	 * @return
	 */
	public ZreMsg dup() {
		final ZreMsg copy = new ZreMsg(this.id);
		if (this.routing_id != null)
			copy.routing_id = this.routing_id.duplicate();
		switch(this.id) {
		case HELLO:
			copy.version = version;
			copy.sequence = sequence;
			copy.endpoint = endpoint;
			copy.groups = new ArrayList<String>(groups);
			copy.status = status;
			copy.name = name;
			copy.headers = new HashMap<String, String>(headers);
			break;
		case WHISPER:
			copy.version = version;
			copy.sequence = sequence;
			copy.content = content.duplicate();
			break;
		case SHOUT:
			copy.version = version;
			copy.sequence = sequence;
			copy.group = group;
			copy.content = content.duplicate();
			break;
		case JOIN:
			copy.version = version;
			copy.sequence = sequence;
			copy.group = group;
			copy.status = status;
			break;
		case LEAVE:
			copy.version = version;
			copy.sequence = sequence;
			copy.group = group;
			copy.status = status;
			break;
		case PING:
			copy.version = version;
			copy.sequence = sequence;
			break;
		case PING_OK:
			copy.version = version;
			copy.sequence = sequence;
			break;
		}
		return copy;
	}

	/**
	 * Dump headers key=value pair to stdout
	 * @param entry
	 * @param self
	 */
	public static void headersDump(final Map.Entry<String, String> entry, final ZreMsg self) {
		System.out.printf("        %s=%s\n", entry.getKey(), entry.getValue());
	}


//--------------------------------------------------------------------------
	/**
	 * Print contents of message to stdout
	 */
	public void dump() {
		switch(id) {
		case HELLO:
			System.out.println("HELLO:");
			System.out.printf("    version=%d\n", (long) version);
			System.out.printf("    sequence=%d\n", (long) sequence);
			if (endpoint != null) {
				System.out.printf("    endpoint='%s'\n", endpoint);
			} else {
				System.out.printf("    endpoint=\n");
			}
			System.out.printf("    groups={");
			if (groups != null) {
				for (String value : groups) {
					System.out.printf(" '%s'", value);
				}
			}
			System.out.printf(" }\n");
			System.out.printf("    status=%d\n", (long) status);
			if (name != null) {
				System.out.printf("    name='%s'\n", name);
			} else {
				System.out.printf("    name=\n");
			}
			System.out.printf("    headers={\n");
			if (headers != null) {
				for (Map.Entry<String, String> entry : headers.entrySet())
					headersDump(entry, this);
			}
			System.out.printf("    }\n");
			break;

		case WHISPER:
			System.out.println("WHISPER:");
			System.out.printf("    version=%d\n", (long) version);
			System.out.printf("    sequence=%d\n", (long) sequence);
			System.out.printf("    content={\n");
			if (content != null) {
				int size = content.size();
				byte[] data = content.getData();
				System.out.printf("        size=%d\n", content.size());
				if (size > 32)
					size = 32;
				int contentIndex;
				for (contentIndex = 0; contentIndex < size; contentIndex++) {
					if (contentIndex != 0 && (contentIndex % 4 == 0))
						System.out.printf("-");
					System.out.printf("%02X", data[contentIndex]);
				}
			}
			System.out.printf("    }\n");
			break;

		case SHOUT:
			System.out.println("SHOUT:");
			System.out.printf("    version=%d\n", (long) version);
			System.out.printf("    sequence=%d\n", (long) sequence);
			if (group != null) {
				System.out.printf("    group='%s'\n", group);
			} else {
				System.out.printf("    group=\n");
			}
			System.out.printf("    content={\n");
			if (content != null) {
				int size = content.size();
				byte[] data = content.getData();
				System.out.printf("        size=%d\n", content.size());
				if (size > 32)
					size = 32;
				int contentIndex;
				for (contentIndex = 0; contentIndex < size; contentIndex++) {
					if (contentIndex != 0 && (contentIndex % 4 == 0))
						System.out.printf("-");
					System.out.printf("%02X", data[contentIndex]);
				}
			}
			System.out.printf("    }\n");
			break;

		case JOIN:
			System.out.println("JOIN:");
			System.out.printf("    version=%d\n", (long) version);
			System.out.printf("    sequence=%d\n", (long) sequence);
			if (group != null)
				System.out.printf("    group='%s'\n", group);
			else
				System.out.printf("    group=\n");
			System.out.printf("    status=%d\n", (long) status);
			break;

		case LEAVE:
			System.out.println("LEAVE:");
			System.out.printf("    version=%d\n", (long) version);
			System.out.printf("    sequence=%d\n", (long) sequence);
			if (group != null)
				System.out.printf("    group='%s'\n", group);
			else
				System.out.printf("    group=\n");
			System.out.printf("    status=%d\n", (long) status);
			break;

		case PING:
			System.out.println("PING:");
			System.out.printf("    version=%d\n", (long) version);
			System.out.printf("    sequence=%d\n", (long) sequence);
			break;

		case PING_OK:
			System.out.println("PING_OK:");
			System.out.printf("    version=%d\n", (long) version);
			System.out.printf("    sequence=%d\n", (long) sequence);
			break;

		}
	}

//--------------------------------------------------------------------------
	/**
	 * Get the message routing_id
	 * @return
	 */
	public ZFrame routingId() {
		return routing_id;
	}

	/**
	 * Set the message routing_id
	 * @return
	 */
	public void setRoutingId(final ZFrame routing_id) {
		if (this.routing_id != null)
			this.routing_id.destroy();
		this.routing_id = routing_id.duplicate();
	}

	/**
	 * Get routing_id as ZreIdentity
	 * @return
	 */
	public ZreIdentity identity() {
		return new ZreIdentity(routing_id.getData());
	}
//--------------------------------------------------------------------------
	/**
	 * Get the zre_msg id
	 * @return
	 */
	public int id() {
		return id;
	}

	/**
	 * Set the zre_msg id
	 * @return
	 */
	public void setId(final int id) {
		this.id = id;
	}

//--------------------------------------------------------------------------
	/**
	 * Get the sequence field
	 * @return
	 */
	public int sequence() {
		return sequence;
	}

	/**
	 * Set the sequence field
	 * @return
	 */
	public void setSequence(final int sequence) {
		this.sequence = sequence;
	}

//--------------------------------------------------------------------------
	/**
	 * Get the endpoint field
	 * @return
	 */
	public String endpoint() {
		return endpoint;
	}

	/**
	 * Set the endpoint field
	 * @return
	 */
	public void setEndpoint(final String format, final Object... args) {
		//  Format into newly allocated string
		endpoint = String.format(format, args);
	}

//--------------------------------------------------------------------------
//  Iterate through the groups field, and append a groups value

	public List<String> groups() {
		return groups;
	}

	public void appendGroups(final String format, final Object... args) {
		//  Format into newly allocated string

		final String string = String.format(format, args);
		//  Attach string to list
		if (groups == null)
			groups = new ArrayList<String>();
		groups.add(string);
	}

	public void setGroups(final Collection<String> value) {
		groups = new ArrayList<String>(value);
	}

//--------------------------------------------------------------------------

	/**
	 * Get the status field
	 * @return
	 */
	public int status() {
		return status;
	}

	/**
	 * Set the status field
	 * @param status
	 */
	public void setStatus(final int status) {
		this.status = status;
	}


//--------------------------------------------------------------------------

	/**
	 * Get a value in the headers dictionary
	 * @return
	 */
	public Map<String, String> headers() {
		return headers;
	}

	/**
	 * Sset a value in the headers dictionary
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public String headersString(final String key, final String defaultValue) {
		String value = null;
		if (headers != null)
			value = headers.get(key);
		if (value == null)
			value = defaultValue;

		return value;
	}

	public long headersNumber(final String key, final long defaultValue) {
		long value = defaultValue;
		String string = null;
		if (headers != null)
			string = headers.get(key);
		if (string != null)
			value = Long.valueOf(string);

		return value;
	}

	public void insertHeaders(final String key, final String format, final Object... args) {
		//  Format string into buffer
		final String string = String.format(format, args);

		//  Store string in hash table
		if (headers == null)
			headers = new HashMap<String, String>();
		headers.put(key, string);
		headersBytes += key.length() + 1 + string.length();
	}

	public void setHeaders(final Map<String, String> value) {
		if (value != null)
			headers = new HashMap<String, String>(value);
		else
			headers = value;
	}


//--------------------------------------------------------------------------
	/**
	 * Get the content field
	 * @return
	 */
	public ZFrame content() {
		return content;
	}

	/**
	 * Set the content field
	 * Takes ownership of supplied frame
	 * @param frame
	 */
	public void setContent(final ZFrame frame) {
		if (content != null)
			content.destroy();
		content = frame;
	}

//--------------------------------------------------------------------------
	/**
	 * Get the group field
	 * @return
	 */
	public String group() {
		return group;
	}

	/**
	 * Set the group field
	 * @param format
	 * @param args
	 */
	public void setGroup(final String format, final Object... args) {
		//  Format into newly allocated string
		group = String.format(format, args);
	}

	/**
	 * Get the name field
	 * @return
	 */
	public String name() {    // XXX saki
		return name;
	}

	/**
	 * Set the name field
	 * @param format
	 * @param args
	 */
	public void setName(final String format, final Object... args) {    // XXX saki
		//  Format into newly allocated string
		name = String.format(format, args);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		switch(id) {
		case HELLO:
			sb.append("HELLO:\n");
			sb.append(String.format("    version=%d\n", (long) version));
			sb.append(String.format("    sequence=%d\n", (long) sequence));
			if (endpoint != null) {
				sb.append(String.format("    endpoint='%s'\n", endpoint));
			} else {
				sb.append("    endpoint=\n");
			}
			sb.append("    groups={");
			if (groups != null) {
				for (String value : groups) {
					sb.append(String.format(" '%s'", value));
				}
			}
			sb.append(" }\n");
			sb.append(String.format("    status=%d\n", (long) status));
			if (name != null) {
				sb.append(String.format("    name='%s'\n", name));
			} else {
				sb.append("    name=\n");
			}
			sb.append("    headers={\n");
			if (headers != null) {
				for (Map.Entry<String, String> entry : headers.entrySet())
					headersDump(entry, this);
			}
			sb.append("    }\n");
			break;

		case WHISPER:
			sb.append("WHISPER:\n");
			sb.append(String.format("    version=%d\n", (long) version));
			sb.append(String.format("    sequence=%d\n", (long) sequence));
			sb.append("    content={\n");
			if (content != null) {
				int size = content.size();
				byte[] data = content.getData();
				sb.append(String.format("        size=%d\n", content.size()));
				if (size > 32)
					size = 32;
				int contentIndex;
				for (contentIndex = 0; contentIndex < size; contentIndex++) {
					if (contentIndex != 0 && (contentIndex % 4 == 0))
						sb.append("-");
					sb.append(String.format("%02X", data[contentIndex]));
				}
			}
			sb.append("    }\n");
			break;

		case SHOUT:
			sb.append("SHOUT:\n");
			sb.append(String.format("    version=%d\n", (long) version));
			sb.append(String.format("    sequence=%d\n", (long) sequence));
			if (group != null) {
				sb.append(String.format("    group='%s'\n", group));
			} else {
				sb.append("    group=\n");
			}
			sb.append("    content={\n");
			if (content != null) {
				int size = content.size();
				byte[] data = content.getData();
				sb.append(String.format("        size=%d\n", content.size()));
				if (size > 32)
					size = 32;
				int contentIndex;
				for (contentIndex = 0; contentIndex < size; contentIndex++) {
					if (contentIndex != 0 && (contentIndex % 4 == 0))
						sb.append("-");
					sb.append(String.format("%02X", data[contentIndex]));
				}
			}
			sb.append("    }\n");
			break;

		case JOIN:
			sb.append("JOIN:\n");
			sb.append(String.format("    version=%d\n", (long) version));
			sb.append(String.format("    sequence=%d\n", (long) sequence));
			if (group != null)
				sb.append(String.format("    group='%s'\n", group));
			else
				sb.append("    group=\n");
			sb.append(String.format("    status=%d\n", (long) status));
			break;

		case LEAVE:
			sb.append("LEAVE:\n");
			sb.append(String.format("    version=%d\n", (long) version));
			sb.append(String.format("    sequence=%d\n", (long) sequence));
			if (group != null)
				sb.append(String.format("    group='%s'\n", group));
			else
				sb.append("    group=\n");
			sb.append(String.format("    status=%d\n", (long) status));
			break;

		case PING:
			sb.append("PING:\n");
			sb.append(String.format("    version=%d\n", (long) version));
			sb.append(String.format("    sequence=%d\n", (long) sequence));
			break;

		case PING_OK:
			sb.append("PING_OK:\n");
			sb.append(String.format("    version=%d\n", (long) version));
			sb.append(String.format("    sequence=%d\n", (long) sequence));
			break;

		}
		return sb.toString();
	}

}

