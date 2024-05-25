package org.s3iter;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.Iterator;

public class ObjectStream implements Iterator<ByteChunk> {

    private final S3Client s3Client;
    private final long chunkSize;
    private final long objectSize;
    private final long numberChunks;
    private long currentChunk = 0;
    private long currentIndexInFile = 0;
    private final String bucket;
    private final String objectKey;

    public static class Builder {
        private S3Client s3Client;
        private long chunkSize;
        private String bucket;
        private String objectKey;

        public Builder withS3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder withChunkSize(long chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder withBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder withObjectKey(String objectKey) {
            this.objectKey = objectKey;
            return this;
        }

        public ObjectStream build() {
            HeadObjectResponse headBucketResponse = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectKey)
                    .build());

            long numberChunks;
            long objectSize = headBucketResponse.contentLength();
            if (objectSize <= chunkSize) {
                numberChunks = 1;
            } else {
                numberChunks = objectSize / chunkSize + 1;
            }
            return new ObjectStream(chunkSize, s3Client, objectSize, numberChunks, bucket, objectKey);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private ObjectStream(long chunkSize, S3Client s3Client, long objectSize, long numberChunks,
                         String bucket, String objectKey) {
        this.s3Client = s3Client;
        this.chunkSize = chunkSize;
        this.objectSize = objectSize;
        this.numberChunks = numberChunks;
        this.bucket = bucket;
        this.objectKey = objectKey;
    }

    @Override
    public boolean hasNext() {
        return currentChunk < numberChunks;
    }

    @Override
    public ByteChunk next() {
        long rangeEnd = nextRangeEnd();
        byte[] chunkBytes = s3Client.getObjectAsBytes(GetObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .range("bytes=" + currentIndexInFile + "-" + rangeEnd)
                .build()).asByteArray();
        ByteChunk rangeRequest = new ByteChunk(currentIndexInFile, rangeEnd, chunkSize, chunkBytes);

        currentChunk++;
        currentIndexInFile += chunkSize;

        return rangeRequest;
    }

    private long nextRangeEnd() {
        if (currentChunk < numberChunks) {
            return Math.min(currentIndexInFile + chunkSize, objectSize);
        }  else {
            throw new RuntimeException("This shouldn't have happened current=" + currentChunk + " number chunks = " + numberChunks);
        }
    }


    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
