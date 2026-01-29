package com.nontrauma.migration.migrationutil.repository;

import java.io.File;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.awspring.cloud.autoconfigure.s3.S3TransferManagerAutoConfiguration;
import io.awspring.cloud.autoconfigure.s3.properties.S3TransferManagerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;


//import com.amazonaws.services.s3.transfer.TransferManager;
//import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import jakarta.annotation.PostConstruct;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

@Repository
public class S3Repository {

    private static final Logger log = LoggerFactory.getLogger(S3Repository.class);

    @Value("${region}")
    private String region;

    @Value("${ntclaims.bucket}")
    private String bucketName;

    private S3Client s3Client;

    // private TransferManager xfer_mgr;
    private S3TransferManager xfer_mgr;

    @Value("${ntclaims.source.bucket}")
    private String sourceBucketName;
    private S3AsyncClient asyncS3Client;

    @Value("${ntclaims.target.bucket}")
    private String targetBucketName;

    @PostConstruct
    private void init() {
        this.s3Client = S3Client.builder().build();
        this.asyncS3Client = S3AsyncClient.builder().region(Region.of(region)).build();
        this.xfer_mgr = S3TransferManager.builder().s3Client(asyncS3Client).build();
        if (this.s3Client != null) {
            log.info("S3Client is initialized successfully");
        }
    }

    public void uploadLargeFile(File fileToUpload) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(this.bucketName)
                .key("migration/" + fileToUpload.getName())
                .build();
        this.s3Client.putObject(putObjectRequest, RequestBody.fromFile(fileToUpload));

    }

    public void uploadFile(BigInteger payerKey, File fileToUpload) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(this.bucketName)
                .key("migration-keyspaces/" + payerKey + "/" + fileToUpload.getName())
                .build();
        this.s3Client.putObject(putObjectRequest, RequestBody.fromFile(fileToUpload));
    }

    public List<String> listFilesFromDirectoryFromSourceBucket(final String directory) {
        return this.listFilesFromDirectory(this.sourceBucketName, directory);
    }

    private List<String> listFilesFromDirectory(final String bucket, final String directory) {
        ListObjectsRequest listObjectRequest = ListObjectsRequest
                .builder()
                .bucket(bucket)
                .prefix(directory)
                .build();
        return this.s3Client.listObjects(listObjectRequest).contents().stream().map(s3Object -> s3Object.key()).collect(Collectors.toList());
    }

    public void processAndMoveFileFromRawBucket(final String key, Consumer<byte[]> processor, final String moveDirectory) {
        this.process(this.sourceBucketName, key, processor);
        this.moveFile(this.sourceBucketName, key, moveDirectory);
    }

    private void process(final String bucket, final String key, Consumer<byte[]> processor) {
        //log.info("Processing file {}", key);
        GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .key(key)
                .bucket(bucket)
                .build();
        ResponseBytes<GetObjectResponse> objectBytes = this.s3Client.getObjectAsBytes(objectRequest);
        log.info("Before calling consumer callback...");
        processor.accept(objectBytes.asByteArray());
        log.info("After calling consumer callback...");
    }

    private void moveFile(final String bucket, final String key, final String moveDirectory) {
        //  log.info("Moving file {} to directory {}", key, moveDirectory);
        CompletableFuture<CopyObjectResponse> cor = this.asyncS3Client.copyObject(builder -> builder.sourceBucket(bucket).sourceKey(key).
                destinationBucket(bucket).destinationKey(moveDirectory + "/" + key));
        cor.whenComplete((response, exception) -> {
            if (response != null) {
                log.debug("File moved successfully");
                this.asyncS3Client.deleteObject(builder -> builder.bucket(bucket).key(key));
            } else {
                log.error("Error moving file", exception);
            }
        });
    }

    public byte[] getContents(final String fileName) {
        GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .key(fileName)
                .bucket(targetBucketName)
                .build();

        ResponseBytes<GetObjectResponse> objectBytes = this.s3Client.getObjectAsBytes(objectRequest);
        return objectBytes.asByteArray();
    }

    public List<byte[]> getContentsBatch(final List<String> fileNames) {
        return fileNames.parallelStream().map(fileName -> {
            try {
                GetObjectRequest objectRequest = GetObjectRequest
                        .builder()
                        .key(fileName)
                        .bucket(targetBucketName)
                        .build();

                ResponseBytes<GetObjectResponse> objectBytes = this.s3Client.getObjectAsBytes(objectRequest);
                return objectBytes.asByteArray();
            } catch (NoSuchKeyException e) {
                log.error("File not found on S3: {}", fileName, e);
                return null; // Handle missing file gracefully
            } catch (Exception e) {
                log.error("Error retrieving content for file: {}", fileName, e);
                return null; // Handle other errors gracefully
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public CompletableFuture<ResponseBytes<GetObjectResponse>> getContentsAsync(final String fileName) {
        log.info("loading file {} from bucket {}", fileName, this.targetBucketName);
        GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .key(fileName)
                .bucket(targetBucketName)
                .build();
        return this.asyncS3Client.getObject(objectRequest, AsyncResponseTransformer.toBytes());//.whenComplete(consumer);

    }

    public void uploadSync(final String fileName, final String content) {

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(this.targetBucketName)
                .key(fileName)
                .build();
        this.s3Client.putObject(putObjectRequest, RequestBody.fromString(content));
    }

    public void uploadMemberFile(Long payerKey, File fileToUpload) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(this.bucketName)
                .key("migrated-members/" + payerKey + "/" + fileToUpload.getName())
                .build();
        this.s3Client.putObject(putObjectRequest, RequestBody.fromFile(fileToUpload));
    }
}
