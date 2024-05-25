package org.s3iter;

public record ByteChunk(
        long startRange,
        long endRange,
        long rangeSize,
        byte[] bytes
) {
}
