/*
 *
 *   Copyright (C) 219  InterviewParrot SIBQ
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package sibq.publisher;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.util.List;

/**
 * An stateless publisher which can publish to multiple topics
 * under the same project. This would avoid create one publisher
 * for each topic.  Remember this class has an overhead because the publisher
 * is created on the fly.
 * @Author interviewparrot
 */
@Log4j2
@Getter
public class CloudMessagePublisher {

    private String projectId;
    private String topicName;
    Publisher publisher = null;


    public CloudMessagePublisher(String topic) throws IOException {
        projectId = ServiceOptions.getDefaultProjectId();
        this.topicName = topic;
        ProjectTopicName topicName = ProjectTopicName.of(projectId, topic);
        publisher = Publisher.newBuilder(topicName).build();

    }


    public List<String> publishMessage(@NonNull List<String> messages) {
        log.info("Publishing to topic: {}", topicName);
        sendMessage(messages);
        return Lists.newArrayList();
    }

    private void sendMessage(List<String> messages) {
        for (final String message : messages) {
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

            // Once published, returns a server-assigned message id (unique within the topic)
            ApiFuture<String> future = publisher.publish(pubsubMessage);

            // Add an asynchronous callback to handle success / failure
            ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

                @Override
                public void onFailure(Throwable throwable) {
                    if (throwable instanceof ApiException) {
                        ApiException apiException = ((ApiException) throwable);
                        // details on the API exception
                        log.info(apiException.getStatusCode().getCode());
                        log.info(apiException.isRetryable());
                    }
                    throwable.printStackTrace();
                    log.error("Error publishing message : " , throwable);
                }

                @Override
                public void onSuccess(String messageId) {
                    log.info("Successfull published the message with id: {}", messageId);
                }
            });
        }

    }
}
