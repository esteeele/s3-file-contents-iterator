package org.s3iter;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ObjectStreamTest {

    private final S3Client s3Client = mock(S3Client.class);

    @Test
    void canStreamContent() {
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder()
                        .contentLength(1000L)
                        .build());

        when(s3Client.getObjectAsBytes(any(GetObjectRequest.class)))
                .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                        new byte[] { 0x1F, 0x11 })); //content is irrelevant

        ObjectStream objectStream = ObjectStream.builder()
                .withS3Client(s3Client)
                .withBucket("test")
                .withObjectKey("testKey")
                .withChunkSize(300)
                .build();

        boolean firstBatch = objectStream.hasNext();
        Assertions.assertTrue(firstBatch);
        ByteChunk byteChunk = objectStream.next();
        assertEquals(0L, byteChunk.startRange());
        assertEquals(300L, byteChunk.endRange());

        boolean secondBatch = objectStream.hasNext();
        Assertions.assertTrue(secondBatch);
        byteChunk = objectStream.next();
        assertEquals(300L, byteChunk.startRange());
        assertEquals(600L, byteChunk.endRange());

        boolean thirdBatch = objectStream.hasNext();
        Assertions.assertTrue(thirdBatch);
        byteChunk = objectStream.next();
        assertEquals(600L, byteChunk.startRange());
        assertEquals(900L, byteChunk.endRange());

        boolean fourthBatch = objectStream.hasNext();
        Assertions.assertTrue(fourthBatch);
        byteChunk = objectStream.next();
        assertEquals(900L, byteChunk.startRange());
        assertEquals(1000L, byteChunk.endRange());

        Assertions.assertFalse(objectStream.hasNext());
    }
}