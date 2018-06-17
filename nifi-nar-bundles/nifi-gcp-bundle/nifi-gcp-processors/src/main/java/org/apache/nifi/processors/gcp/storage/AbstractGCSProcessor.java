/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.gcp.storage;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.util.StringUtils;

import java.net.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class for creating processors which connect to Google Cloud Storage.
 *
 * Every GCS processor operation requires a bucket, whether it's reading or writing from said bucket.
 */
public abstract class AbstractGCSProcessor extends AbstractGCPProcessor<Storage, StorageOptions> {

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder().name("success")
                    .description("FlowFiles are routed to this relationship after a successful Google Cloud Storage operation.")
                    .build();
    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("FlowFiles are routed to this relationship if the Google Cloud Storage operation fails.")
                    .build();

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .build();
    }


    @Override
    protected StorageOptions getServiceOptions(ProcessContext context, GoogleCredentials credentials) {
        final String projectId = context.getProperty(PROJECT_ID).getValue();
        final Integer retryCount = context.getProperty(RETRY_COUNT).asInteger();

        final String proxyHost = context.getProperty(PROXY_HOST).getValue();
        final Integer proxyPort = context.getProperty(PROXY_PORT).asInteger();

        StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(projectId)
                .setRetrySettings(RetrySettings.newBuilder()
                        .setMaxAttempts(retryCount)
                        .build());

        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context, () -> {
            if (proxyHost != null && proxyPort != null) {
                final ProxyConfiguration componentConfiguration = new ProxyConfiguration();

                componentConfiguration.setProxyType(Proxy.Type.HTTP);
                componentConfiguration.setProxyServerHost(proxyHost);
                componentConfiguration.setProxyServerPort(proxyPort);

                return componentConfiguration;
            }
            return ProxyConfiguration.DIRECT_CONFIGURATION;
        });

        if (!StringUtils.isBlank(proxyHost) && proxyPort > 0) {
            storageOptionsBuilder.setTransportOptions(HttpTransportOptions.newBuilder().setHttpTransportFactory(() -> {
                NetHttpTransport.Builder httpTransportBuilder = new NetHttpTransport.Builder();
                        if (Proxy.Type.HTTP.equals(proxyConfiguration.getProxyType())) {
                            httpTransportBuilder.setProxy(proxyConfiguration.createProxy());
                        }

                        return httpTransportBuilder.build();
            }).build());
        }

        return storageOptionsBuilder.build();

    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();
        validateProxySpec(validationContext, results);
        return results;
    }

}
