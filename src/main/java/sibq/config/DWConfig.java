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

package sibq.config;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import sibq.buffer.PersistentBuffer;
import sibq.message.DWDatasetInfo;
import sibq.publisher.CloudMessagePublisher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * All the SIBQ configuration should go here.
 * @Author interviewparrot
 */
@Configuration
public class DWConfig {


    @Value("${sibq.dw.topicName}")
    private String dwTopicName;
    @Value("${sibq.dw.bufferName}")
    private String dwBufferName;

    @Value("${phoenix.dw.datasetName}")
    private String datasetName;


    @Bean
    public CloudMessagePublisher getCloudMessagePublisher() throws IOException {
        return new CloudMessagePublisher(dwTopicName);
    }

    @Bean
    public PersistentBuffer getPersistenceCache() throws Exception {
        PersistentBuffer buffer = new PersistentBuffer(dwBufferName);
        buffer.init();
        return buffer;
    }


    @Bean
    public String getDWConfigJson() {
        return "";
    }

    @Bean
    public DWDatasetConfig datasetConfig() {
        JsonParser parser = new JsonParser();
        Map<String, DWDatasetInfo> map = new HashMap<>();

        try {
            JsonElement root = parser.parse(getDWConfigJson());

            JsonObject jsonObject = root.getAsJsonObject();
            JsonElement datasetName = jsonObject.get("DatasetName");
            JsonArray tableNames = jsonObject.getAsJsonArray("TableNames");
            Iterator<JsonElement> iterator = tableNames.iterator();
            while(iterator.hasNext()) {
                JsonElement element = iterator.next();
                JsonObject tableNameObj = element.getAsJsonObject();
                Set<Map.Entry<String, JsonElement>> entries = tableNameObj.entrySet();
                for(Map.Entry<String, JsonElement> entry: entries) {
                    map.put(entry.getKey(), DWDatasetInfo.builder()
                            .datasetName(datasetName.getAsString())
                            .tableName(entry.getValue().getAsString())
                            .build());
                }
            }

            System.out.println(map);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return DWDatasetConfig.builder()
                .datasetInfoMap(map)
                .build();
    }



}
