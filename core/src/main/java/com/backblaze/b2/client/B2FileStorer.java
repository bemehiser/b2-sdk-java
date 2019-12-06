/*
 * Copyright 2017, Backblaze Inc. All Rights Reserved.
 * License https://www.backblaze.com/using_b2_code.html
 */
package com.backblaze.b2.client;

import com.backblaze.b2.client.contentSources.B2ContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2Part;
import com.backblaze.b2.client.structures.B2UploadListener;
import com.backblaze.b2.client.structures.B2UploadState;
import com.backblaze.b2.util.B2ByteRange;

import java.io.IOException;

public interface B2FileStorer {

    /**
     * Stores a part by uploading the bytes from a content source.
     */
    B2Part uploadPart(
            int partNumber,
            B2ContentSource contentSource,
            B2UploadListener uploadListener) throws IOException, B2Exception;

    /**
     * Stores a part by copying from a file that is already stored in a bucket.
     *
     * We do not know the true size of the part until it is finally stored. Some
     * copy operations do not provide a byte range being copied, and byte ranges
     * can be clamped down if they exceed the bounds of the file. Therefore, we
     * use a placeholder value until the operation succeeds. Once the API returns
     * a B2Part object, we supply the true size in the SUCCEEDED event.
     */
    B2Part copyPart(
            int partNumber,
            String sourceFileId,
            B2ByteRange byteRangeOrNull,
            B2UploadListener uploadListener) throws B2Exception;

    void updateProgress(
            B2UploadListener uploadListener,
            int partNumber,
            long partLength,
            long bytesSoFar,
            B2UploadState uploadState);
}
