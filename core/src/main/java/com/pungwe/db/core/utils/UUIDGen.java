package com.pungwe.db.core.utils;

import java.io.*;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.pungwe.db.core.utils.Utils.createHash;

/**
 * Copied from Cassandra to make UUID gen a bit nicer, but includes a few modifications to get MAC Address
 * instead of inet address
 */
public final class UUIDGen {

    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    private static final long START_EPOCH = -12219292800000L;
    private static final long clockSeqAndNode = makeClockSeqAndNode();

    // placement of this singleton is important.  It needs to be instantiated *AFTER* the other statics.
    private static final UUIDGen instance = new UUIDGen();

    private AtomicLong lastNanos = new AtomicLong();

    private UUIDGen() {
        // make sure someone didn't whack the clockSeqAndNode by changing the order of instantiation.
        if (clockSeqAndNode == 0) throw new RuntimeException("singleton instantiation is misplaced.");
    }

    /**
     * Creates a type 1 UUID (time-based UUID).
     *
     * @return a UUID instance
     */
    public static UUID getTimeUUID() {
        return new UUID(instance.createTimeSafe(), clockSeqAndNode);
    }

    /**
     * creates a type 1 uuid from raw bytes.
     */
    public static UUID getUUID(ByteBuffer raw) {
        return new UUID(raw.getLong(raw.position()), raw.getLong(raw.position() + 8));
    }

    public static UUID getUUID(byte[] bytes) {
        ByteBuffer wrapper = ByteBuffer.wrap(bytes);
        return getUUID(wrapper);
    }

    /**
     * decomposes a uuid into raw bytes.
     */
    public static byte[] asByteArray(UUID uuid) {
        long most = uuid.getMostSignificantBits();
        long least = uuid.getLeastSignificantBits();
        byte[] b = new byte[16];
        for (int i = 0; i < 8; i++) {
            b[i] = (byte) (most >>> ((7 - i) * 8));
            b[8 + i] = (byte) (least >>> ((7 - i) * 8));
        }
        return b;
    }

    /**
     * Returns a 16 byte representation of a type 1 UUID (a time-based UUID),
     * based when the current system time.
     *
     * @return a type 1 UUID represented as a byte[]
     */
    public static byte[] getTimeUUIDBytes() {
        return createTimeUUIDBytes(instance.createTimeSafe());
    }


    /**
     * @param uuid
     * @return milliseconds since Unix epoch
     */
    public static long unixTimestamp(UUID uuid) {
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    /**
     * @param uuid
     * @return microseconds since Unix epoch
     */
    public static long microsTimestamp(UUID uuid) {
        return (uuid.timestamp() / 10) + START_EPOCH * 1000;
    }

    /**
     * @param timestamp milliseconds since Unix epoch
     * @return
     */
    private static long fromUnixTimestamp(long timestamp) {
        return fromUnixTimestamp(timestamp, 0L);
    }

    private static long fromUnixTimestamp(long timestamp, long nanos) {
        return ((timestamp - START_EPOCH) * 10000) + nanos;
    }

    /**
     * Converts a 100-nanoseconds precision timestamp into the 16 byte representation
     * of a type 1 UUID (a time-based UUID).
     * <p>
     * To specify a 100-nanoseconds precision timestamp, one should provide a milliseconds timestamp and
     * a number {@code 0 <= n < 10000} such when n*100 is the number of nanoseconds within when millisecond.
     * <p>
     * <p><i><b>Warning:</b> This method is not guaranteed to return unique UUIDs; Multiple
     * invocations using identical timestamps will result in identical UUIDs.</i></p>
     *
     * @return a type 1 UUID represented as a byte[]
     */
    public static byte[] getTimeUUIDBytes(long timeMillis, int nanos) {
        if (nanos >= 10000)
            throw new IllegalArgumentException();
        return createTimeUUIDBytes(instance.createTimeUnsafe(timeMillis, nanos));
    }

    private static byte[] createTimeUUIDBytes(long msb) {
        long lsb = clockSeqAndNode;
        byte[] uuidBytes = new byte[16];

        for (int i = 0; i < 8; i++)
            uuidBytes[i] = (byte) (msb >>> 8 * (7 - i));

        for (int i = 8; i < 16; i++)
            uuidBytes[i] = (byte) (lsb >>> 8 * (7 - i));

        return uuidBytes;
    }

    /**
     * Returns a milliseconds-since-epoch value for a type-1 UUID.
     *
     * @param uuid a type-1 (time-based) UUID
     * @return the number of milliseconds since the unix epoch
     * @throws IllegalArgumentException if the UUID is not version 1
     */
    public static long getAdjustedTimestamp(UUID uuid) {
        if (uuid.version() != 1)
            throw new IllegalArgumentException("incompatible with uuid version: " + uuid.version());
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    private static long makeClockSeqAndNode() {
        long clock = new SecureRandom().nextLong();
        long lsb = 0;
        lsb |= 0x8000000000000000L;                 // variant (2 bits)
        lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
        lsb |= makeNode();                          // 6 bytes
        return lsb;
    }

    // needs to return two different values for the same when.
    // we can generate at most 10k UUIDs per ms.
    private long createTimeSafe() {
        long newLastNanos;
        while (true) {
            //Generate a candidate value for new lastNanos
            newLastNanos = ((System.currentTimeMillis() - START_EPOCH) * 1000000l);
            long originalLastNanos = lastNanos.get();
            if (newLastNanos > originalLastNanos) {
                //Slow path once per millisecond do a CAS
                if (lastNanos.compareAndSet(originalLastNanos, newLastNanos)) {
                    break;
                }
            } else {
                //Fast path do an atomic increment
                //Or when falling behind this will move time forward past the clock if necessary
                newLastNanos = lastNanos.incrementAndGet();
                break;
            }
        }
        return createTime(newLastNanos);
    }

    private long createTimeUnsafe(long when, int nanos) {
        long nanosSince = ((when - START_EPOCH) * 1000000) + nanos;
        return createTime(nanosSince);
    }

    private static long createTime(long nanosSince) {
        long msb = 0L;
        msb |= (0x00000000ffffffffL & nanosSince) << 32;
        msb |= (0x0000ffff00000000L & nanosSince) >>> 16;
        msb |= (0xffff000000000000L & nanosSince) >>> 48;
        msb |= 0x0000000000001000L; // sets the version to 1.
        return msb;
    }

    private static long makeNode() {

        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (!interfaces.hasMoreElements()) {
                throw new RuntimeException("You need to have at least one network interface");
            }

            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            // Get hardware addresses!
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                byte[] hardwareInterface = networkInterface.getHardwareAddress();
                if (hardwareInterface == null) {
                    continue;
                }
                // Writes the hardware address to the byte array
                out.write(hardwareInterface);
            }

            if (out.size() == 0) {
                throw new RuntimeException("Could not find a mac address");
            }

            // Add the process id
            out.writeInt(createProcessIdentifier());


            // ideally, we'd use the MAC address, but java doesn't expose when.
            long node = createHash(bytes.toByteArray());

            return node;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static int createProcessIdentifier() {
        int processId;
        try {
            String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            if (processName.contains("@")) {
                processId = Integer.parseInt(processName.substring(0, processName.indexOf('@')));
            } else {
                processId = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode();
            }

        } catch (Throwable t) {
            processId = new SecureRandom().nextInt();
        }

        return processId;
    }

}
