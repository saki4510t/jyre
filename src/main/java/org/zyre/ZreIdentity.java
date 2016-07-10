/*  =========================================================================
    ZreIdentity - holds identity of peer in a ZyRE network

    -------------------------------------------------------------------------
    Copyright (c) 2016 t_saki@serenegiant.com
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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.CRC32;

public class ZreIdentity implements Serializable, Comparable<ZreIdentity> {

	private static final long serialVersionUID = -7315239823644160093L;

	private final byte[] mIdentity;
	private final long mHash;

	/**
	 * Helper method to create ZreIdentity from UUID string
	 * @param uuid UUID string that match to format of UUID.toString or HEX string
	 * @return
	 */
	public static ZreIdentity fromUUID(final String uuid) throws IllegalArgumentException {
		try {
			return new ZreIdentity(UUID.fromString(uuid));
		} catch (final IllegalArgumentException e) {
			// try to hex string
			if ((uuid != null) && !uuid.contains("-")) {
				final StringBuilder sb = new StringBuilder(uuid);
				sb.insert(23, '-');
				sb.insert(18, '-');
				sb.insert(13, '-');
				sb.insert(8, '-');
				return new ZreIdentity(UUID.fromString(sb.toString()));
			} else {
				throw e;
			}
		}
	}

	/**
	 * Constructor
	 * @param id wrapped raw byte array with String
	 */
	public ZreIdentity(final String id) {
		this(id.getBytes(), 0);
	}

	/**
	 * Constructor
	 * @param uuid
	 */
	public ZreIdentity(final UUID uuid) {
		this(uuidBytes(uuid, true), 0);
	}

	/**
	 * Constructor
	 * @param identity
	 */
	public ZreIdentity(final byte[] identity) {
		this(identity, 0);
	}

	/**
	 * Constructor
	 * @param identity
	 * @param offset
	 */
	public ZreIdentity(final byte[] identity, final int offset) {
		if (identity == null) throw new IllegalArgumentException();
		mIdentity = new byte[17];
		final int n = identity.length + offset;
		if (n == 16) {
			System.arraycopy(identity, offset, mIdentity, 1, 16);
			mIdentity[0] = 0x01;
		} else if (n == 17) {
			System.arraycopy(identity, offset, mIdentity, 0, 17);
			if (mIdentity[0] != 0x01) {
				System.err.printf("W: wrong identity, fixed %d=>1\n", mIdentity[0]);
				mIdentity[0] = 0x01;
			}
		} else if (n > 17) {
			System.arraycopy(identity, offset, mIdentity, 0, 17);
			if (mIdentity[0] != 0x01) {
				System.err.printf("W: invalid identity, fixed %d=>1\n", mIdentity[0]);
				mIdentity[0] = 0x01;
			}
		} else {
			throw new IllegalArgumentException("invalid identity");
		}
		// calculate hash using CRC32
		final CRC32 crc32 = new CRC32();
		crc32.update(mIdentity);
		mHash = crc32.getValue();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ZreIdentity) {
			return ((ZreIdentity)o).mHash == mHash;
		} else if (o instanceof UUID) {
			final byte[] identity = uuidBytes((UUID)o, true);
			final CRC32 crc32 = new CRC32();
			crc32.update(identity);
			return mHash == crc32.getValue();
		} else if (o instanceof byte[]) {
			final CRC32 crc32 = new CRC32();
			crc32.update((byte[])o);
			return mHash == crc32.getValue();
		}
		return super.equals(o);
	}

	@Override
	public int hashCode() {
		return (int)mHash;
	}

	@Override
	public String toString() {
		final ByteBuffer buf = ByteBuffer.wrap(mIdentity, 1, 16);
		final UUID uuid = new UUID(buf.getLong(), buf.getLong());
		return uuid.toString().replace("-", "").toUpperCase();
	}

	@Override
	public int compareTo(final ZreIdentity another) {
		return (int)(mHash - another.mHash);
	}

	/**
	 * Get raw identity as byte[], should not modify resulted byte[]
	 * @return
	 */
	public byte[] getBytes() {
		return mIdentity;
	}

	/**
	 * Get length of identity
	 * @return
	 */
	public int length() {
		return mIdentity != null ? mIdentity.length : 0;
	}

	/**
	 * Helper method to convert UUID to raw byte[]
	 * @param uuid
	 * @param setLeadingOne
	 * @return
	 */
	public static byte[] uuidBytes(final UUID uuid, final boolean setLeadingOne) {
		final ByteBuffer buf = ByteBuffer.wrap(new byte[16 + (setLeadingOne ? 1 : 0)]);
		if (setLeadingOne) {
			buf.put((byte)1);
		}
		buf.putLong(uuid.getMostSignificantBits());
		buf.putLong(uuid.getLeastSignificantBits());
		return buf.array();
	}

}
