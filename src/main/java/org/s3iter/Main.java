package org.s3iter;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) {
        final String BUCKET = "{}";
        final String KEY = "{}";
        StringBuilder stringBuilder = new StringBuilder(1000);

        try (S3Client s3Client = S3Client.builder()
                     .credentialsProvider(() -> AwsBasicCredentials.builder()
                             .secretAccessKey("{}")
                             .accessKeyId("{}")
                             .build())
                     .region(Region.EU_WEST_2)
                     .build()) {

            ObjectStream objectStream = ObjectStream.builder()
                    .withBucket(BUCKET)
                    .withObjectKey(KEY)
                    .withChunkSize(250)
                    .withS3Client(s3Client)
                    .build();

            while (objectStream.hasNext()) {
                String contentChunk = new String(objectStream.next().bytes(), StandardCharsets.UTF_8);
                // do something that might go wrong
            }
        }

        Path demoFile = Paths.get("src/main/resources/recreated.txt");
        if (!Files.exists(demoFile)) {
            try {
                Files.createFile(demoFile);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        try {
            Files.writeString(demoFile, stringBuilder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}