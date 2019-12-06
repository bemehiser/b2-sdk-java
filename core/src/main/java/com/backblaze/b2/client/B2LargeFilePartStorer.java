/*
 * Copyright 2019, Backblaze Inc. All Rights Reserved.
 * License https://www.backblaze.com/using_b2_code.html
 */
package com.backblaze.b2.client;

import com.backblaze.b2.client.contentSources.B2ContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.exceptions.B2LocalException;
import com.backblaze.b2.client.structures.*;
import com.backblaze.b2.util.B2ByteProgressListener;
import com.backblaze.b2.util.B2ByteRange;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * A class for handling the creation of large files.
 *
 * @see B2StorageClient#storeLargeFile(B2FileVersion, List, B2UploadListener, ExecutorService)
 * for a description of the various ways large files can be created
 * and the uses cases supported.
 */
public class B2LargeFilePartStorer implements B2FileStorer {

    /**
     * The B2FileVersion for the large file that is being created.
     */
    private final B2FileVersion fileVersion;

    /**
     * The parts that need to be stored before finishing the large file.
     */
    private final B2PartStorer partStorer;

    /**
     * Stores where each part will begin in the finished large file.
     * Will be set to B2UploadProgress.UNKNOWN_PART_START_BYTE for
     * parts that come after a copied part.
     */
    private final Long startingBytePositions;

    private final B2AccountAuthorizationCache accountAuthCache;
    private final B2UploadPartUrlCache uploadPartUrlCache;
    private final B2StorageClientWebifier webifier;
    private final B2Retryer retryer;
    private final Supplier<B2RetryPolicy> retryPolicySupplier;

    B2LargeFilePartStorer(
            B2FileVersion fileVersion,
            B2PartStorer partStorer,
            B2AccountAuthorizationCache accountAuthCache,
            B2StorageClientWebifier webifier,
            B2Retryer retryer,
            Supplier<B2RetryPolicy> retryPolicySupplier) {

        this.fileVersion = fileVersion;
        this.partStorer = validatePartStorers(partStorer);
        this.startingBytePositions = (Long) 0L;

        this.accountAuthCache = accountAuthCache;
        this.uploadPartUrlCache = new B2UploadPartUrlCache(webifier, accountAuthCache, fileVersion.getFileId());
        this.webifier = webifier;
        this.retryer = retryer;
        this.retryPolicySupplier = retryPolicySupplier;
    }

    private B2PartStorer validatePartStorers(B2PartStorer partStorer) {
        // Go through the parts - throw if there are duplicates or gaps.
        final int partNumber = partStorer.getPartNumber();

        if (partNumber < 1) {
            throw new IllegalArgumentException("invalid part number: " + partNumber);
        }
        return partStorer;
    }

    B2PartStorer getPartStorer() {
        return partStorer;
    }

    /**
     * @return The start byte for the part, or UNKNOWN_PART_START_BYTE if not known.
     */
    long getStartByteOrUnknown() {
        return startingBytePositions;
    }

    public static B2LargeFilePartStorer forLocalContent(
            B2FileVersion largeFileVersion,
            B2ContentSource contentSource,
            int partNumber,
            B2AccountAuthorizationCache accountAuthCache,
            B2StorageClientWebifier webifier,
            B2Retryer retryer,
            Supplier<B2RetryPolicy> retryPolicySupplier) throws B2Exception {

        final B2PartStorer partContentSource;
        try {
            B2PartSpec partSpec = new B2PartSpec(partNumber, 0, contentSource.getContentLength());
            partContentSource = new B2UploadingPartStorer(
                    partSpec.getPartNumber(),
                    createRangedContentSource(contentSource, partSpec.getStart(), partSpec.getLength()));
        } catch (IOException e) {
            throw new B2LocalException("trouble", "exception working with content source" + e, e);
        }
        // Instantiate and return the manager.
        return new B2LargeFilePartStorer(
                largeFileVersion,
                partContentSource,
                accountAuthCache,
                webifier,
                retryer,
                retryPolicySupplier);
    }

    String storeFilePart(B2UploadListener uploadListenerOrNull) throws B2Exception {
        B2Part b2Part = storeFileParts(uploadListenerOrNull);
        return b2Part.getContentSha1();
    }

    private B2Part storeFileParts(B2UploadListener uploadListenerOrNull) throws B2Exception {
        final B2UploadListener uploadListener;
        if (uploadListenerOrNull == null) {
            uploadListener = B2UploadListener.noopListener();
        } else {
            uploadListener = uploadListenerOrNull;
        }

        try {
            return partStorer.storePart(this, uploadListener);
        } catch (IOException e) {
            throw new B2LocalException("trouble", "exception working with content source" + e, e);
        }
    }

    private String finishLargeFilePartFuture(Future<B2Part> partFuture) throws B2Exception {

        final String partSha1;
        try {
            final B2Part part = partFuture.get();
            partSha1 = part.getContentSha1();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new B2LocalException("interrupted", "interrupted while trying to copy parts: " + e, e);

        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof B2Exception) {
                throw (B2Exception) e.getCause();
            } else {
                throw new B2LocalException("trouble", "exception while trying to upload parts: " + cause, cause);
            }
        } finally {
            // we've hit an exception and we aren't going to wait.
            partFuture.cancel(true);
        }
        return partSha1;
    }

    public void updateProgress(
            B2UploadListener uploadListener,
            int partNumber,
            long partLength,
            long bytesSoFar,
            B2UploadState uploadState) {

        uploadListener.progress(
                new B2UploadProgress(
                        partNumber - 1,
                        partStorer.getPartNumber(),
                        getStartByteOrUnknown(),
                        partLength,
                        bytesSoFar,
                        uploadState));
    }

    /**
     * Stores a part by uploading the bytes from a content source.
     */
    public B2Part uploadPart(
            int partNumber,
            B2ContentSource contentSource,
            B2UploadListener uploadListener) throws IOException, B2Exception {

        updateProgress(
                uploadListener,
                partNumber,
                contentSource.getContentLength(),
                0,
                B2UploadState.WAITING_TO_START);

        // Set up the listener for the part upload.
        final B2ByteProgressListener progressAdapter = new B2UploadProgressAdapter(
                uploadListener,
                partNumber - 1,
                partStorer.getPartNumber(),
                getStartByteOrUnknown(),
                contentSource.getContentLength());
        final B2ByteProgressFilteringListener progressListener = new B2ByteProgressFilteringListener(progressAdapter);

        try {
            return retryer.doRetry(
                    "b2_upload_part",
                    accountAuthCache,
                    (isRetry) -> {
                        final B2UploadPartUrlResponse uploadPartUrlResponse = uploadPartUrlCache.get(isRetry);

                        final B2ContentSource contentSourceThatReportsProgress =
                                new B2ContentSourceWithByteProgressListener(contentSource, progressListener);
                        final B2UploadPartRequest uploadPartRequest = B2UploadPartRequest
                                .builder(partNumber, contentSourceThatReportsProgress)
                                .build();

                        updateProgress(
                                uploadListener,
                                partNumber,
                                contentSource.getContentLength(),
                                0,
                                B2UploadState.STARTING);

                        final B2Part part = webifier.uploadPart(uploadPartUrlResponse, uploadPartRequest);

                        // Return the upload part URL, because it works and can be reused.
                        uploadPartUrlCache.unget(uploadPartUrlResponse);

                        updateProgress(
                                uploadListener,
                                partNumber,
                                part.getContentLength(),
                                part.getContentLength(),
                                B2UploadState.SUCCEEDED);

                        return part;
                    },
                    retryPolicySupplier.get()
            );
        } catch (B2Exception e) {
            updateProgress(
                    uploadListener,
                    partNumber,
                    contentSource.getContentLength(),
                    0,
                    B2UploadState.FAILED);

            throw e;
        }
    }

    /**
     * Stores a part by copying from a file that is already stored in a bucket.
     * <p>
     * We do not know the true size of the part until it is finally stored. Some
     * copy operations do not provide a byte range being copied, and byte ranges
     * can be clamped down if they exceed the bounds of the file. Therefore, we
     * use a placeholder value until the operation succeeds. Once the API returns
     * a B2Part object, we supply the true size in the SUCCEEDED event.
     */
    public B2Part copyPart(
            int partNumber,
            String sourceFileId,
            B2ByteRange byteRangeOrNull,
            B2UploadListener uploadListener) throws B2Exception {

        updateProgress(
                uploadListener,
                partNumber,
                B2UploadProgress.UNKNOWN_PART_SIZE_PLACEHOLDER,
                0,
                B2UploadState.WAITING_TO_START);

        final B2CopyPartRequest copyPartRequest = B2CopyPartRequest
                .builder(partNumber, sourceFileId, fileVersion.getFileId())
                .setRange(byteRangeOrNull)
                .build();

        try {
            return retryer.doRetry(
                    "b2_copy_part",
                    accountAuthCache,
                    () -> {
                        updateProgress(
                                uploadListener,
                                partNumber,
                                B2UploadProgress.UNKNOWN_PART_SIZE_PLACEHOLDER,
                                0,
                                B2UploadState.STARTING);

                        final B2Part part = webifier.copyPart(accountAuthCache.get(), copyPartRequest);

                        updateProgress(
                                uploadListener,
                                partNumber,
                                part.getContentLength(),
                                part.getContentLength(),
                                B2UploadState.SUCCEEDED);

                        return part;
                    },
                    retryPolicySupplier.get());
        } catch (B2Exception e) {
            updateProgress(
                    uploadListener,
                    partNumber,
                    B2UploadProgress.UNKNOWN_PART_SIZE_PLACEHOLDER,
                    0,
                    B2UploadState.FAILED);

            throw e;
        }
    }

    static B2ContentSource createRangedContentSource(
            B2ContentSource contentSource, long start, long length) throws IOException {

        final B2ContentSource contentSourceWithRangeOrNull = contentSource.createContentSourceWithRangeOrNull(
                start, length);
        if (contentSourceWithRangeOrNull != null) {
            return contentSourceWithRangeOrNull;
        }

        return new B2PartOfContentSource(contentSource, start, length);
    }

}

