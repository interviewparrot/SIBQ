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

import com.google.api.client.util.Base64;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.gson.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import sibq.config.DWDatasetConfig;
import sibq.message.BaseMessage;
import sibq.message.DWMessage;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @Author interviewparrot
 */
@Service
public class DWMessagePublisher {

    @Autowired
    private CloudMessagePublisher dwPublisher;


    @Autowired
    private DWDatasetConfig datasetConfig;

    public void publishDWMessage(BaseMessage message) {

        Gson gson = getGson();
        message.setCreationTime(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        message.setCreationDate(LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE));
        String payload = Base64.encodeBase64String(gson.toJson(message).getBytes(Charsets.UTF_8));
        DWMessage dwMessage = DWMessage.builder()
                .payload(payload)
                .datasetInfo(datasetConfig.getDatasetInfo(message.getClass()))
                .build();
        dwPublisher.publishMessage(Lists.newArrayList(gson.toJson(dwMessage)));
    }

    private Gson getGson() {
        JsonSerializer<LocalDateTime> jsonSerializer = new JsonSerializer<LocalDateTime>() {
            @Override
            public JsonElement serialize(LocalDateTime src, Type typeOfSrc, JsonSerializationContext context) {
                long sec = src.toInstant(ZoneOffset.UTC).getEpochSecond();
                return new JsonPrimitive(sec);

            }
        };
        return new GsonBuilder().registerTypeAdapter(LocalDateTime.class, jsonSerializer).create();
    }
}
