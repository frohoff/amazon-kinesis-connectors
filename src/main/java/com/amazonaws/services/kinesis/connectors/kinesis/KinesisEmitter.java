/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.connectors.kinesis;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.Record;

/**
 * This implementaion of IEmitter inserts records into Amazon S3 and emits filenames into a separate
 * Amazon Kinesis stream. The separate Amazon Kinesis stream is to be used by another Amazon Kinesis enabled application
 * that utilizes RedshiftManifestEmitters to insert the records into Amazon Redshift via a manifest copy.
 * This class requires the configuration of an Amazon S3 bucket and endpoint, as well as Amazon Kinesis endpoint
 * and output stream.
 * <p>
 * When the buffer is full, this Emitter:
 * <ol>
 * <li>Puts all records into a single file in S3</li>
 * <li>Puts the single file name into the manifest stream</li>
 * </ol>
 * <p>
 * NOTE: the Amazon S3 bucket and Amazon Redshift cluster must be in the same region.
 */
public class KinesisEmitter implements IEmitter<Record> {
    private static final Log LOG = LogFactory.getLog(KinesisEmitter.class);
    private final AmazonKinesisClient kinesisClient;
    private final String stream;

    public KinesisEmitter(KinesisConnectorConfiguration configuration) {
        stream = configuration.KINESIS_OUTPUT_STREAM;
        kinesisClient = new AmazonKinesisClient(configuration.AWS_CREDENTIALS_PROVIDER);
        if (configuration.REGION_NAME != null) {
        	kinesisClient.setRegion(Region.getRegion(Regions.fromName(configuration.REGION_NAME)));
        }
        if (configuration.KINESIS_ENDPOINT != null) {
            kinesisClient.setEndpoint(configuration.KINESIS_ENDPOINT);
        }
    }

    @Override
    public List<Record> emit(final UnmodifiableBuffer<Record> buffer) throws IOException {
    	List<Record> records = buffer.getRecords();
    	List<Record> failedRecords = new LinkedList<Record>();
    	for (Record record : records) {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setData(record.getData());
            putRecordRequest.setStreamName(stream);
            putRecordRequest.setPartitionKey(record.getPartitionKey());
            try {
                kinesisClient.putRecord(putRecordRequest);
                LOG.info("KinesisEmitter emitted " + record.getData().capacity() + " byte record downstream");
            } catch (Exception e) {
            	// TODO: retry?
                LOG.error(e);
                failedRecords.add(record);
            }
    	}
    	return failedRecords;
    }

    @Override
    public void fail(List<Record> records) {
        for (Record record : records) {
            LOG.error("Record failed: " + Arrays.toString(record.getData().array()));
        }
    }

    @Override
    public void shutdown() {
        kinesisClient.shutdown();
    }
}
