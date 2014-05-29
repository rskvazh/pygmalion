package org.pygmalion.udf.uuid;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Generates a time UUID. Either give it a time to use or
 * don't specify one and get a time UUID based on the current
 * time.
 */
public class GenerateDateTimeFromTimeUUID extends EvalFunc<Long> {

    @Override
    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        try {
            ByteBuffer bb = (ByteBuffer)input.get(0);
            UUID u = TimeUUIDUtils.uuid(bb);
            return u.timestamp();
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row ", e);
        }
    }
}
