/*  =========================================================================
    ZreGroup - group known to this node
            
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

import java.util.HashMap;
import java.util.Map;

public class ZreGroup {

	private final String name;
	private final Map<ZreIdentity, ZrePeer> peers;

	private ZreGroup(final String name) {
		this.name = name;
		peers = new HashMap<ZreIdentity, ZrePeer>();
	}

//---------------------------------------------------------------------
	/**
	 * Destroy group object
	 */
	public void destroy() {
	}

//---------------------------------------------------------------------
	/**
	 * Construct new group object
	 * @param name
	 * @param container
	 * @return
	 */
	public static ZreGroup newGroup(final String name, final Map<String, ZreGroup> container) {
		final ZreGroup group = new ZreGroup(name);
		container.put(name, group);

		return group;
	}

//---------------------------------------------------------------------
	/**
	 * Add peer to group
	 * Ignore duplicate joins
	 * @param peer
	 */
	public void join(final ZrePeer peer) {
		peers.put(peer.identity(), peer);
		peer.incStatus();
	}

//---------------------------------------------------------------------
	/**
	 * Remove peer from group
	 * @param peer
	 */
	public void leave(final ZrePeer peer) {
		peers.remove(peer.identity());
		peer.incStatus();
	}

//---------------------------------------------------------------------
	/**
	 * Send message to all peers in group
	 * @param msg
	 */
	public void send(ZreMsg msg) {
		for (final ZrePeer peer : peers.values())
			peer.send(msg.dup());

		msg.destroy();
	}

	/**
	 * Get name of this group
	 * @return
	 */
	public String name() {
		return name;
	}

	@Override
	public String toString() {
		return String.format("ZreGroup(%s, peers=%d)", name, peers.size());
	}
}
